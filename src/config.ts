import type { SandboxClaimAPIVersion } from "./sandbox/types.js";

export type Config = {
  adminToken?: string;
  dispatcherEnabled: boolean;
  dispatcherIdlePollMs: number;
  dispatcherLeaseDurationMs: number;
  dispatcherRenewIntervalMs: number;
  dispatcherWorkerId: string;
  claimReadyTimeoutMs: number;
  controlPlaneUrl?: string;
  kubeNamespace: string;
  opencodeDirectory: string;
  opencodePort: number;
  port: number;
  metricsPort?: number;
  sandboxClaimApiVersion: SandboxClaimAPIVersion;
  executionMaintenanceBatchSize: number;
  executionMaintenanceEnabled: boolean;
  executionMaintenanceIntervalMs: number;
  executionMaxAttempts: number;
  executionRetryDelayMs: number;
  revisionResolverEnabled: boolean;
  revisionResolverIdlePollMs: number;
  revisionResolverLeaseDurationMs: number;
  revisionResolverMaxAttempts: number;
  revisionResolverRequestTimeoutMs: number;
  revisionResolverRetryDelayMs: number;
  revisionResolverWorkerId: string;
  githubAppIdFile?: string;
  githubAppPrivateKeyFile?: string;
  githubIssueAcknowledgmentEnabled: boolean;
  githubIssueAcknowledgmentIdlePollMs: number;
  githubIssueAcknowledgmentLeaseDurationMs: number;
  githubIssueAcknowledgmentRequestTimeoutMs: number;
  githubIssueAcknowledgmentRetryDelayMs: number;
  scheduleWorkerEnabled: boolean;
  scheduleWorkerIdlePollMs: number;
  scheduleWorkerLeaseDurationMs: number;
  scheduleWorkerRetryDelayMs: number;
  scheduleWorkerMaxAttempts: number;
  scheduleWorkerMaterializeBatchSize: number;
  scheduleWorkerId: string;
};

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const config = {
    adminToken: emptyToUndefined(env.DISPATCH_ADMIN_TOKEN),
    claimReadyTimeoutMs: readTimerDelay(env.DISPATCH_CLAIM_READY_TIMEOUT_MS, 180_000),
    controlPlaneUrl: readControlPlaneUrl(env.DISPATCH_CONTROL_PLANE_URL),
    dispatcherEnabled: readStrictBoolean(env.DISPATCH_DISPATCHER_ENABLED, true),
    dispatcherIdlePollMs: readTimerDelay(env.DISPATCH_DISPATCHER_IDLE_POLL_MS, 500),
    dispatcherLeaseDurationMs: readPositiveInteger(env.DISPATCH_DISPATCHER_LEASE_DURATION_MS, 60_000),
    dispatcherRenewIntervalMs: readTimerDelay(env.DISPATCH_DISPATCHER_RENEW_INTERVAL_MS, 20_000),
    dispatcherWorkerId: env.DISPATCH_DISPATCHER_WORKER_ID ?? env.HOSTNAME ?? `dispatch-${process.pid}`,
    executionMaintenanceBatchSize: readPositiveInteger(env.DISPATCH_EXECUTION_MAINTENANCE_BATCH_SIZE, 100),
    executionMaintenanceEnabled: readStrictBoolean(env.DISPATCH_EXECUTION_MAINTENANCE_ENABLED, true),
    executionMaintenanceIntervalMs: readTimerDelay(env.DISPATCH_EXECUTION_MAINTENANCE_INTERVAL_MS, 5_000),
    executionMaxAttempts: readPositiveInteger(env.DISPATCH_EXECUTION_MAX_ATTEMPTS, 3),
    executionRetryDelayMs: readNonnegativeInteger(env.DISPATCH_EXECUTION_RETRY_DELAY_MS, 30_000),
    revisionResolverEnabled: readStrictBoolean(env.DISPATCH_REVISION_RESOLVER_ENABLED, false),
    revisionResolverIdlePollMs: readTimerDelay(env.DISPATCH_REVISION_RESOLVER_IDLE_POLL_MS, 500),
    revisionResolverLeaseDurationMs: readPositiveInteger(env.DISPATCH_REVISION_RESOLVER_LEASE_DURATION_MS, 60_000),
    revisionResolverMaxAttempts: readPositiveInteger(env.DISPATCH_REVISION_RESOLVER_MAX_ATTEMPTS, 5),
    revisionResolverRequestTimeoutMs: readTimerDelay(env.DISPATCH_REVISION_RESOLVER_REQUEST_TIMEOUT_MS, 30_000),
    revisionResolverRetryDelayMs: readNonnegativeInteger(env.DISPATCH_REVISION_RESOLVER_RETRY_DELAY_MS, 30_000),
    revisionResolverWorkerId: env.DISPATCH_REVISION_RESOLVER_WORKER_ID ?? env.HOSTNAME ?? `dispatch-${process.pid}`,
    githubAppIdFile: emptyToUndefined(env.DISPATCH_GITHUB_APP_ID_FILE),
    githubAppPrivateKeyFile: emptyToUndefined(env.DISPATCH_GITHUB_PRIVATE_KEY_FILE),
    githubIssueAcknowledgmentEnabled: readStrictBoolean(env.DISPATCH_GITHUB_ISSUE_ACKNOWLEDGMENT_ENABLED, false),
    githubIssueAcknowledgmentIdlePollMs: readTimerDelay(env.DISPATCH_GITHUB_ISSUE_ACKNOWLEDGMENT_IDLE_POLL_MS, 250),
    githubIssueAcknowledgmentLeaseDurationMs: readPositiveInteger(env.DISPATCH_GITHUB_ISSUE_ACKNOWLEDGMENT_LEASE_DURATION_MS, 60_000),
    githubIssueAcknowledgmentRequestTimeoutMs: readTimerDelay(env.DISPATCH_GITHUB_ISSUE_ACKNOWLEDGMENT_REQUEST_TIMEOUT_MS, 30_000),
    githubIssueAcknowledgmentRetryDelayMs: readNonnegativeInteger(env.DISPATCH_GITHUB_ISSUE_ACKNOWLEDGMENT_RETRY_DELAY_MS, 5_000),
    scheduleWorkerEnabled: readStrictBoolean(env.DISPATCH_SCHEDULE_WORKER_ENABLED, false),
    scheduleWorkerIdlePollMs: readTimerDelay(env.DISPATCH_SCHEDULE_WORKER_IDLE_POLL_MS, 1_000),
    scheduleWorkerLeaseDurationMs: readPositiveInteger(env.DISPATCH_SCHEDULE_WORKER_LEASE_DURATION_MS, 60_000),
    scheduleWorkerRetryDelayMs: readNonnegativeInteger(env.DISPATCH_SCHEDULE_WORKER_RETRY_DELAY_MS, 30_000),
    scheduleWorkerMaxAttempts: readPositiveInteger(env.DISPATCH_SCHEDULE_WORKER_MAX_ATTEMPTS, 5),
    scheduleWorkerMaterializeBatchSize: readPositiveInteger(env.DISPATCH_SCHEDULE_WORKER_MATERIALIZE_BATCH_SIZE, 100),
    scheduleWorkerId: env.DISPATCH_SCHEDULE_WORKER_ID ?? env.HOSTNAME ?? `dispatch-${process.pid}`,
    kubeNamespace: env.DISPATCH_KUBE_NAMESPACE ?? env.POD_NAMESPACE ?? "agents",
    opencodeDirectory: env.DISPATCH_OPENCODE_DIRECTORY ?? "/workspace",
    opencodePort: readPort(env.DISPATCH_OPENCODE_PORT, 4096, "DISPATCH_OPENCODE_PORT", false),
    port: readPort(env.PORT, 3000, "PORT", true),
    metricsPort: readPort(env.DISPATCH_METRICS_PORT, 9090, "DISPATCH_METRICS_PORT", true),
    sandboxClaimApiVersion: readSandboxClaimApiVersion(env.DISPATCH_SANDBOX_CLAIM_API_VERSION),
  };
  if (config.port !== 0 && config.port === config.metricsPort) {
    throw new Error("PORT and DISPATCH_METRICS_PORT must differ when both listeners use concrete ports");
  }
  if (config.dispatcherRenewIntervalMs >= config.dispatcherLeaseDurationMs) {
    throw new Error("DISPATCH_DISPATCHER_RENEW_INTERVAL_MS must be less than DISPATCH_DISPATCHER_LEASE_DURATION_MS");
  }
  if (config.scheduleWorkerEnabled && !config.revisionResolverEnabled) {
    throw new Error("DISPATCH_SCHEDULE_WORKER_ENABLED requires DISPATCH_REVISION_RESOLVER_ENABLED");
  }
  if ((config.revisionResolverEnabled || config.githubIssueAcknowledgmentEnabled) && (!config.githubAppIdFile || !config.githubAppPrivateKeyFile)) {
    throw new Error("DISPATCH_GITHUB_APP_ID_FILE and DISPATCH_GITHUB_PRIVATE_KEY_FILE are required when GitHub control-plane workers are enabled");
  }
  if (config.revisionResolverRequestTimeoutMs >= config.revisionResolverLeaseDurationMs) {
    throw new Error("DISPATCH_REVISION_RESOLVER_REQUEST_TIMEOUT_MS must be less than DISPATCH_REVISION_RESOLVER_LEASE_DURATION_MS");
  }
  if (config.githubIssueAcknowledgmentRequestTimeoutMs >= config.githubIssueAcknowledgmentLeaseDurationMs) {
    throw new Error("DISPATCH_GITHUB_ISSUE_ACKNOWLEDGMENT_REQUEST_TIMEOUT_MS must be less than DISPATCH_GITHUB_ISSUE_ACKNOWLEDGMENT_LEASE_DURATION_MS");
  }
  return config;
}

function readSandboxClaimApiVersion(value: string | undefined): SandboxClaimAPIVersion {
  if (value === undefined || value === "") return "v1beta1";
  if (value === "v1alpha1" || value === "v1beta1") return value;
  throw new Error(`Expected DISPATCH_SANDBOX_CLAIM_API_VERSION to be v1alpha1 or v1beta1, got ${value}`);
}

function emptyToUndefined(value: string | undefined): string | undefined {
  return value && value.length > 0 ? value : undefined;
}

function readControlPlaneUrl(value: string | undefined): string | undefined {
  const configured = emptyToUndefined(value);
  if (!configured) return undefined;
  const url = new URL(configured);
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error("DISPATCH_CONTROL_PLANE_URL must be an HTTP(S) URL without credentials, query, or fragment");
  return url.toString();
}

function readPositiveInteger(value: string | undefined, fallback: number): number {
  const parsed = readInteger(value, fallback);
  if (parsed < 1) throw new Error(`Expected a positive integer, got ${value}`);
  return parsed;
}

function readNonnegativeInteger(value: string | undefined, fallback: number): number {
  const parsed = readInteger(value, fallback);
  if (parsed < 0) throw new Error(`Expected a nonnegative integer, got ${value}`);
  return parsed;
}

function readInteger(value: string | undefined, fallback: number): number {
  if (value === undefined || value === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error(`Expected a safe integer, got ${value}`);
  return parsed;
}

function readPort(value: string | undefined, fallback: number, name: string, allowZero: boolean): number {
  const parsed = readInteger(value, fallback);
  if (parsed > 65_535 || parsed < (allowZero ? 0 : 1)) {
    throw new Error(`${name} must be a ${allowZero ? "nonnegative" : "positive"} integer TCP port at most 65535, got ${value}`);
  }
  return parsed;
}

function readTimerDelay(value: string | undefined, fallback: number): number {
  const parsed = readPositiveInteger(value, fallback);
  if (parsed > 2_147_483_647) throw new Error(`Expected a timer delay at most 2147483647, got ${value}`);
  return parsed;
}

function readStrictBoolean(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined || value === "") return fallback;
  if (value === "true") return true;
  if (value === "false") return false;
  throw new Error(`Expected true or false, got ${value}`);
}
