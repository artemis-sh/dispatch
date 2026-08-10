import { serve } from "@hono/node-server";
import { loadConfig } from "./config.js";
import { logger } from "./logger.js";
import { mountControlApi } from "./control/api.js";
import { mountGitHubWebhookApi } from "./connectors/github/api.js";
import { mountGitHubEffectsApi } from "./connectors/github/effects-api.js";
import { runExecutionMaintenanceLoop } from "./dispatch/maintenance.js";
import { DispatcherWorker, OpenCodeExecutionAttemptRunner } from "./dispatch/worker.js";
import { createOpenApiApp, mountHealthRoute, mountMetricsRoute, mountOpenApiDocs } from "./openapi.js";
import { createRuntimeStore } from "./runtime/store.js";
import { createKubeConfig } from "./sandbox/client.js";
import { SandboxClaimExecutionAttemptProvisioner } from "./sandbox/provisioner.js";
import { GitHubAppRevisionResolver } from "./revision/github.js";
import { RevisionResolutionWorker } from "./revision/worker.js";
import { GitHubIssueAcknowledgmentTransport, GITHUB_ISSUE_REACTION_TOPIC } from "./connectors/github/issue-acknowledgment.js";
import { disabledGitHubIssueAcknowledgmentTransport } from "./connectors/github/issue-acknowledgment-release.js";
import { OutboxPublisher } from "./outbox/publisher.js";
import { runOutboxPublisherLoop } from "./outbox/worker.js";
import { ScheduleWorker } from "./schedule/worker.js";
import { metricsRegistry, registerDatabaseMetrics, workerLoopFailures } from "./observability/metrics.js";

const config = loadConfig();
const runtimeStore = await createRuntimeStore();
registerDatabaseMetrics(runtimeStore);
const provisioner = config.executionMaintenanceEnabled || config.dispatcherEnabled
  ? new SandboxClaimExecutionAttemptProvisioner(createKubeConfig(), config)
  : undefined;
const dispatcherController = new AbortController();
const maintenanceController = new AbortController();
const revisionController = new AbortController();
const acknowledgmentController = new AbortController();
const scheduleController = new AbortController();
const scheduleTask = config.scheduleWorkerEnabled
  ? new ScheduleWorker({
      store: runtimeStore,
      workerId: config.scheduleWorkerId,
      leaseDurationMs: config.scheduleWorkerLeaseDurationMs,
      idlePollMs: config.scheduleWorkerIdlePollMs,
      retryDelayMs: config.scheduleWorkerRetryDelayMs,
      maxAttempts: config.scheduleWorkerMaxAttempts,
      materializeBatchSize: config.scheduleWorkerMaterializeBatchSize,
    }).run(scheduleController.signal)
  : Promise.resolve();
const scheduler = scheduleTask.catch((error: unknown) => {
  workerLoopFailures.inc({ component: "schedule" });
  logger.error("schedule worker stopped unexpectedly", { error });
});
const acknowledgmentTask = config.githubIssueAcknowledgmentEnabled
  ? runOutboxPublisherLoop({
      publisher: new OutboxPublisher({
        store: runtimeStore,
        transport: new GitHubIssueAcknowledgmentTransport({
          appIdFile: config.githubAppIdFile!,
          privateKeyFile: config.githubAppPrivateKeyFile!,
        }),
        batchSize: 1,
        leaseDurationMs: config.githubIssueAcknowledgmentLeaseDurationMs,
        transportTimeoutMs: config.githubIssueAcknowledgmentRequestTimeoutMs,
        baseRetryDelayMs: config.githubIssueAcknowledgmentRetryDelayMs,
        maxRetryDelayMs: 5 * 60_000,
        topics: [GITHUB_ISSUE_REACTION_TOPIC],
      }),
      idlePollMs: config.githubIssueAcknowledgmentIdlePollMs,
      signal: acknowledgmentController.signal,
    })
  : runOutboxPublisherLoop({
      publisher: new OutboxPublisher({
        store: runtimeStore,
        transport: disabledGitHubIssueAcknowledgmentTransport,
        batchSize: 1,
        leaseDurationMs: config.githubIssueAcknowledgmentLeaseDurationMs,
        transportTimeoutMs: config.githubIssueAcknowledgmentRequestTimeoutMs,
        baseRetryDelayMs: config.githubIssueAcknowledgmentRetryDelayMs,
        maxRetryDelayMs: 5 * 60_000,
        topics: [GITHUB_ISSUE_REACTION_TOPIC],
      }),
      idlePollMs: config.githubIssueAcknowledgmentIdlePollMs,
      signal: acknowledgmentController.signal,
    });
const acknowledgment = acknowledgmentTask.catch((error: unknown) => {
  workerLoopFailures.inc({ component: "github-acknowledgment" });
  logger.error("GitHub issue acknowledgment worker stopped unexpectedly", { error });
});
const revisionTask = config.revisionResolverEnabled
  ? new RevisionResolutionWorker({
      store: runtimeStore,
      resolver: new GitHubAppRevisionResolver({
        appIdFile: config.githubAppIdFile!,
        privateKeyFile: config.githubAppPrivateKeyFile!,
      }),
      workerId: config.revisionResolverWorkerId,
      leaseDurationMs: config.revisionResolverLeaseDurationMs,
      idlePollMs: config.revisionResolverIdlePollMs,
      retryDelayMs: config.revisionResolverRetryDelayMs,
      maxAttempts: config.revisionResolverMaxAttempts,
      requestTimeoutMs: config.revisionResolverRequestTimeoutMs,
    }).run(revisionController.signal)
  : Promise.resolve();
const revisionResolver = revisionTask.catch((error: unknown) => {
  workerLoopFailures.inc({ component: "revision-resolution" });
  logger.error("revision resolution worker stopped unexpectedly", { error });
});
const maintenanceTask = config.executionMaintenanceEnabled
  ? runExecutionMaintenanceLoop({
      batchSize: config.executionMaintenanceBatchSize,
      intervalMs: config.executionMaintenanceIntervalMs,
      maxAttempts: config.executionMaxAttempts,
      retryDelayMs: config.executionRetryDelayMs,
      signal: maintenanceController.signal,
      cancellationCleaner: provisioner!,
      store: runtimeStore,
    })
  : Promise.resolve();
const maintenance = maintenanceTask.catch((error: unknown) => {
  workerLoopFailures.inc({ component: "execution-maintenance" });
  logger.error("execution maintenance loop stopped unexpectedly", { error });
});
const dispatcherTask = config.dispatcherEnabled
  ? new DispatcherWorker({
      controlPlaneUrl: config.controlPlaneUrl,
      idlePollMs: config.dispatcherIdlePollMs,
      leaseDurationMs: config.dispatcherLeaseDurationMs,
      maxAttempts: config.executionMaxAttempts,
      provisioner: provisioner!,
      renewIntervalMs: config.dispatcherRenewIntervalMs,
      retryDelayMs: config.executionRetryDelayMs,
      runner: new OpenCodeExecutionAttemptRunner({
        directory: config.opencodeDirectory,
        port: config.opencodePort,
        readyTimeoutMs: config.claimReadyTimeoutMs,
      }),
      store: runtimeStore,
      workerId: config.dispatcherWorkerId,
    }).run(dispatcherController.signal)
  : Promise.resolve();
const dispatcher = dispatcherTask.catch((error: unknown) => {
  workerLoopFailures.inc({ component: "dispatcher" });
  logger.error("dispatcher worker stopped unexpectedly", { error });
});
const app = createOpenApiApp();
const metricsApp = createOpenApiApp();

mountHealthRoute(app);
mountMetricsRoute(metricsApp, metricsRegistry);
mountControlApi(app, config, runtimeStore);
mountGitHubWebhookApi(app, runtimeStore, undefined, undefined, config.githubIssueAcknowledgmentEnabled);
mountGitHubEffectsApi(app, runtimeStore);
mountOpenApiDocs(app);

const server = serve({ fetch: (request) => app.fetch(request), port: config.port }, (info) => {
  logger.info("dispatch listening", { port: info.port });
});
const metricsServer = serve({ fetch: (request) => metricsApp.fetch(request), port: config.metricsPort ?? 9090 }, (info) => {
  logger.info("dispatch metrics listening", { port: info.port });
});

let shutdownPromise: Promise<void> | undefined;

function shutdown(signal: string): Promise<void> {
  shutdownPromise ??= performShutdown(signal);
  return shutdownPromise;
}

async function performShutdown(signal: string): Promise<void> {
  logger.info("shutdown received", { signal });
  maintenanceController.abort();
  dispatcherController.abort();
  revisionController.abort();
  acknowledgmentController.abort();
  scheduleController.abort();
  const serverClosed = new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  const metricsServerClosed = new Promise<void>((resolve, reject) => metricsServer.close((error) => error ? reject(error) : resolve()));
  await Promise.all([serverClosed, metricsServerClosed]);
  await Promise.all([maintenance, dispatcher, revisionResolver, acknowledgment, scheduler]);
  await runtimeStore.close();
}

function handleSignal(signal: string): void {
  void shutdown(signal).catch((error: unknown) => {
    logger.error("shutdown failed", { error, signal });
    process.exitCode = 1;
  });
}

process.once("SIGINT", () => handleSignal("SIGINT"));
process.once("SIGTERM", () => handleSignal("SIGTERM"));
