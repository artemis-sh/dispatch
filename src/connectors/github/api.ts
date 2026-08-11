import { randomUUID } from "node:crypto";
import type { Hook, OpenAPIHono } from "@hono/zod-openapi";
import type { Context } from "hono";
import { bodyLimit } from "hono/body-limit";
import type { Trigger, TriggerStore } from "../../control/trigger.js";
import { TriggerNotFoundError } from "../../control/trigger.js";
import { sourceDeliveryIdempotencyKey } from "../../execution/idempotency.js";
import type { EventAdmissionStore } from "../../execution/store.js";
import { IdempotencyConflictError } from "../../execution/types.js";
import { hashCanonicalJson, type JsonValue } from "../../json.js";
import { logger } from "../../logger.js";
import { WorkspaceResolutionError } from "../../workspace/resolver.js";
import { githubWebhookRoute } from "./api-schema.js";
import { normalizeGitHubEvent, type GitHubEventName } from "./normalize.js";
import { sha256, verifyGitHubSignature } from "./signature.js";

const TENANT_ID = "default";
const MAX_BODY_BYTES = 128 * 1024;
const DUMMY_WEBHOOK_SECRET = "dispatch-github-webhook-dummy-secret-v1";
const supportedEvents = new Set<GitHubEventName>([
  "issues",
  "issue_comment",
  "pull_request",
  "pull_request_review",
  "pull_request_review_comment",
  "workflow_run",
]);

export type GitHubWebhookApiStore = TriggerStore & EventAdmissionStore;

export function mountGitHubWebhookApi(
  app: OpenAPIHono<any>,
  store: GitHubWebhookApiStore,
  readEnvironmentVariable: (name: string) => string | undefined = (name) => process.env[name],
  verifySignature: typeof verifyGitHubSignature = verifyGitHubSignature,
  issueAcknowledgmentEnabled = false,
): void {
  app.use("/hooks/github/:triggerID", bodyLimit({
    maxSize: MAX_BODY_BYTES,
    onError: (context) => context.json({ error: "Request body too large" }, 413),
  }));
  app.openapi(githubWebhookRoute, async (context) => handle(context, async () => {
    const { triggerID } = context.req.valid("param");

    const contentEncoding = context.req.header("content-encoding");
    if (contentEncoding && contentEncoding.toLowerCase() !== "identity") {
      return context.json({ error: "Unsupported media type" }, 415);
    }

    const headers = context.req.valid("header");
    const delivery = headers["X-GitHub-Delivery"];
    const eventName = headers["X-GitHub-Event"];
    const body = new Uint8Array(await context.req.arrayBuffer());
    let trigger: Trigger | undefined;
    try {
      trigger = await store.getTrigger(TENANT_ID, triggerID);
    } catch {}
    let configuredSecret: string | undefined;
    if (trigger?.type === "github.app.webhook") {
      try {
        configuredSecret = readEnvironmentVariable(trigger.config.webhookSecretEnv);
      } catch {}
    }
    const secretBytes = configuredSecret === undefined ? 0 : Buffer.byteLength(configuredSecret, "utf8");
    const isGitHubTrigger = trigger?.type === "github.app.webhook";
    const hasValidSecret = configuredSecret !== undefined
      && !configuredSecret.includes("\0")
      && secretBytes >= 32
      && secretBytes <= 1_024;
    const verificationSecret = isGitHubTrigger && hasValidSecret
      ? configuredSecret as string
      : DUMMY_WEBHOOK_SECRET;
    const signatureValid = verifySignature(
      headers["X-Hub-Signature-256"],
      verificationSecret,
      body,
    );
    if (trigger?.type !== "github.app.webhook"
      || configuredSecret === undefined
      || configuredSecret.includes("\0")
      || secretBytes < 32
      || secretBytes > 1_024
      || !signatureValid) return unauthorized(context);

    let payload: unknown;
    try {
      payload = JSON.parse(Buffer.from(body).toString("utf8"));
    } catch {
      return context.json({ error: "Invalid request" }, 400);
    }
    if (payload === null || typeof payload !== "object" || Array.isArray(payload)) return context.json({ error: "Invalid request" }, 400);
    if (eventName === "ping" || !supportedEvents.has(eventName as GitHubEventName)) {
      return trigger.enabled ? context.body(null, 204) : context.json({ error: "Trigger not found" }, 404);
    }

    let event;
    try {
      event = normalizeGitHubEvent({ event: eventName as GitHubEventName, deliveryId: delivery, payloadSha256: sha256(body), payload });
    } catch {
      return context.json({ error: "Invalid request" }, 400);
    }
    if (!event) return trigger.enabled ? context.body(null, 204) : context.json({ error: "Trigger not found" }, 404);

    const revisionResolution = githubRevisionResolution(event.data);
    const githubIssueAcknowledgment = issueAcknowledgmentEnabled
      && (event.type === "com.github.issues.opened" || event.type === "com.github.pull_request.opened")
      ? issueAcknowledgment(event.data, event.type === "com.github.issues.opened" ? "issue" : "pullRequest")
      : undefined;
    await store.admitEvent({
      tenantId: TENANT_ID,
      triggerId: triggerID,
      internalEventId: randomUUID(),
      event,
      sourceDeduplicationKey: sourceDeliveryIdempotencyKey(triggerID, delivery),
      admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId: triggerID, event } as JsonValue),
      admittedAt: new Date().toISOString(),
      ...(revisionResolution ? { revisionResolution } : {}),
      ...(githubIssueAcknowledgment ? { githubIssueAcknowledgment } : {}),
    });
    if (eventName === "pull_request" && event.type === "com.github.pull_request.opened" && "reconcileGitHubPullRequestEffects" in store) {
      const data = event.data as { repository: { id: number }; pullRequest: { id: number; number: number } };
      await (store as GitHubWebhookApiStore & { reconcileGitHubPullRequestEffects: (tenantId: string, identity: { repositoryId: number; githubPullRequestId: string; pullRequestNumber: number }) => Promise<void> })
        .reconcileGitHubPullRequestEffects(TENANT_ID, { repositoryId: data.repository.id, githubPullRequestId: String(data.pullRequest.id), pullRequestNumber: data.pullRequest.number });
    }
    return context.body(null, 202);
  }), webhookValidationHook as never);
}

function githubRevisionResolution(data: JsonValue) {
  if (data === null || typeof data !== "object" || Array.isArray(data)) return undefined;
  const repository = data.repository;
  const installationId = data.installationId;
  if (repository === null || typeof repository !== "object" || Array.isArray(repository)
    || typeof installationId !== "number" || !Number.isSafeInteger(installationId) || installationId < 1
    || typeof repository.id !== "number" || !Number.isSafeInteger(repository.id) || repository.id < 1
    || typeof repository.fullName !== "string" || typeof repository.cloneUrl !== "string"
    || typeof repository.defaultBranch !== "string") {
    return undefined;
  }
  return {
    provider: "github" as const,
    installationId,
    repositoryId: repository.id,
    repositoryFullName: repository.fullName,
    cloneUrl: repository.cloneUrl,
    branch: repository.defaultBranch,
  };
}

function issueAcknowledgment(data: JsonValue, subjectKey: "issue" | "pullRequest") {
  if (data === null || typeof data !== "object" || Array.isArray(data)) return undefined;
  const repository = data.repository;
  const subject = data[subjectKey];
  const installationId = data.installationId;
  if (repository === null || typeof repository !== "object" || Array.isArray(repository)
    || subject === null || typeof subject !== "object" || Array.isArray(subject)
    || typeof installationId !== "number" || !Number.isSafeInteger(installationId) || installationId < 1
    || typeof repository.id !== "number" || !Number.isSafeInteger(repository.id) || repository.id < 1
    || typeof repository.fullName !== "string"
    || typeof subject.number !== "number" || !Number.isSafeInteger(subject.number) || subject.number < 1) return undefined;
  return {
    subjectType: subjectKey === "issue" ? "issue" as const : "pull_request" as const,
    installationId,
    repositoryId: repository.id,
    repositoryFullName: repository.fullName,
    issueNumber: subject.number,
  };
}

const webhookValidationHook: Hook<unknown, any, any, any> = (result, context) => {
  if (result.success) return;
  const signatureFailure = result.error.issues.some((issue) => issue.path.includes("X-Hub-Signature-256"));
  if (signatureFailure) return unauthorized(context);
  const contentTypeFailure = result.error.issues.some((issue) => issue.path.includes("Content-Type"));
  return contentTypeFailure
    ? context.json({ error: "Unsupported media type" }, 415)
    : context.json({ error: "Invalid request" }, 400);
};

function unauthorized(context: Context): Response {
  return context.json({ error: "Unauthorized" }, 401);
}

async function handle(context: Context, run: () => Promise<Response>): Promise<Response> {
  try {
    return await run();
  } catch (error) {
    if (error instanceof TriggerNotFoundError) return context.json({ error: "Trigger not found" }, 404);
    if (error instanceof IdempotencyConflictError) return context.json({ error: "Delivery conflict" }, 409);
    if (error instanceof WorkspaceResolutionError) {
      logger.warn("github webhook workspace resolution failed", { error: error.message });
      return context.json({ error: "Workspace could not be resolved from event data" }, 422);
    }
    return context.json({ error: "Internal server error" }, 500);
  }
}
