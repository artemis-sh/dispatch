import { randomUUID } from "node:crypto";
import pg from "pg";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { hashCanonicalJson, type JsonValue } from "../../src/json.js";
import { createPostgresRuntimeStore, type PostgresRuntimeStore } from "../../src/runtime/postgres.js";

const { Pool } = pg;

describe("supplied wake context persistence", () => {
  let container: StartedTestContainer;
  let store: PostgresRuntimeStore;
  let pool: pg.Pool;

  beforeAll(async () => {
    container = await new GenericContainer("postgres:17-alpine")
      .withEnvironment({ POSTGRES_DB: "dispatch", POSTGRES_PASSWORD: "dispatch-password", POSTGRES_USER: "dispatch" })
      .withExposedPorts(5432).withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2)).start();
    const connectionString = `postgres://dispatch:dispatch-password@${container.getHost()}:${container.getMappedPort(5432)}/dispatch`;
    store = await createPostgresRuntimeStore({ connectionString, runMigrations: true, ssl: false, sslRejectUnauthorized: false });
    pool = new Pool({ connectionString });
    await store.createTrigger({ config: { schemaVersion: 1 }, createdAt: new Date().toISOString(), disabledAt: null, enabled: true, id: "events", tenantId: "default", type: "cloudevents.http" });
    await store.publishProfileVersion({
      id: randomUUID(), profileId: "developer", tenantId: "default", version: 1, createdAt: new Date().toISOString(),
      definition: { schemaVersion: 1, runtime: { type: "opencode", agent: "developer", opencodeConfig: { agent: { developer: {} } } }, sandbox: { templateName: "developer", warmPool: "none" }, connections: [], permissions: { onRequest: "fail" }, timeoutSeconds: 3600 },
    });
  }, 120_000);

  afterAll(async () => { await pool?.end(); await store?.close(); await container?.stop(); });

  it("activates a pending-context wait after trusted slot binding", async () => {
    const executionId = await createDeveloper();
    const claimed = await runDeveloperToCompletion(executionId);
    expect((await pool.query("select state from dispatch_event_waits where execution_id=$1", [executionId])).rows[0]).toEqual({ state: "PENDING_CONTEXT" });

    await expect(store.bindExecutionWakeContextValue({
      authorityId: "effect-1", authorityType: "github.pull-request-effect", boundAt: new Date().toISOString(),
      executionId, slot: "primaryPullRequestNumber", tenantId: "default", value: 42, waitName: "developer-pr-lifecycle",
    })).resolves.toEqual({ correlation: { repositoryId: 7, pullRequestNumber: 42 }, ready: true });
    expect((await pool.query("select state, correlation from dispatch_event_waits where execution_id=$1", [executionId])).rows[0]).toEqual({ state: "ACTIVE", correlation: { repositoryId: 7, pullRequestNumber: 42 } });
    await expect(store.bindExecutionWakeContextValue({
      authorityId: "effect-1", authorityType: "github.pull-request-effect", boundAt: new Date().toISOString(),
      executionId, slot: "primaryPullRequestNumber", tenantId: "default", value: 42, waitName: "developer-pr-lifecycle",
    })).resolves.toMatchObject({ ready: true });
    await expect(store.bindExecutionWakeContextValue({
      authorityId: "effect-2", authorityType: "github.pull-request-effect", boundAt: new Date().toISOString(),
      executionId, slot: "primaryPullRequestNumber", tenantId: "default", value: 43, waitName: "developer-pr-lifecycle",
    })).rejects.toThrow();
    expect(claimed.executionId).toBe(executionId);
  });

  it("fails a PR lifecycle turn without a registered effect instead of creating a pending-context wait", async () => {
    const executionId = await createDeveloper({ requireGitHubPullRequestEffect: true });
    const claimed = await runDeveloperToCompletion(executionId);

    expect(await store.getExecution("default", executionId)).toMatchObject({ state: "FAILED", result: { error: "MISSING_REQUIRED_GITHUB_PR_EFFECT" } });
    expect((await pool.query("select state from dispatch_execution_attempts where execution_id=$1", [executionId])).rows[0]).toEqual({ state: "FAILED" });
    expect((await pool.query("select count(*)::int as count from dispatch_event_waits where execution_id=$1", [executionId])).rows[0]).toEqual({ count: 0 });
    expect((await pool.query("select reason from dispatch_execution_transitions where execution_id=$1 order by sequence desc limit 1", [executionId])).rows[0]).toEqual({ reason: "MISSING_REQUIRED_GITHUB_PR_EFFECT" });
    expect(claimed.executionId).toBe(executionId);
  });

  it("allows a registered effect to await later signed PR confirmation", async () => {
    const executionId = await createDeveloper({ requireGitHubPullRequestEffect: true });
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: `registered-effect-${randomUUID()}`, leaseDurationMs: 60_000 });
    if (!claimed || claimed.executionId !== executionId) throw new Error("Expected developer claim");
    await store.transitionLeasedExecution({ actor: claimed.lease.leaseOwner, attempt: claimed.lease.attempt, executionId,
      expectedAttemptState: "LEASED", expectedExecutionState: "PROVISIONING", fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner, reason: "ready", targetAttemptState: "RUNNING", targetExecutionState: "RUNNING", tenantId: "default" });
    const effect = await store.registerGitHubPullRequestEffect({ baseRef: "main", executionId, fencingToken: claimed.lease.fencingToken,
      headRef: "dispatch/issue-7", pullRequestTitle: "Fix issue 7", registeredAt: new Date().toISOString(), repositoryFullName: "acme/repo",
      repositoryId: 7, requestHash: hashCanonicalJson({ owner: "acme", repo: "repo", title: "Fix issue 7", head: "dispatch/issue-7", base: "main" }), tenantId: "default" });
    await store.completeLeasedExecutionTurn({ actor: claimed.lease.leaseOwner, attempt: claimed.lease.attempt, executionId,
      fencingToken: claimed.lease.fencingToken, leaseOwner: claimed.lease.leaseOwner, reason: "done", result: null, tenantId: "default" });

    expect(await store.getExecution("default", executionId)).toMatchObject({ state: "WAITING" });
    expect((await pool.query("select state from dispatch_event_waits where execution_id=$1", [executionId])).rows[0]).toEqual({ state: "PENDING_CONTEXT" });
    await store.requestExecutionCancellation({ actor: "test", executionId, reason: "cleanup", requestedAt: new Date().toISOString(), tenantId: "default", transitionId: randomUUID() });
    await pool.query("delete from dispatch_github_pull_request_effects where id=$1", [effect.id]);
  });

  it("reconciles a review offer admitted before the PR slot is bound", async () => {
    const executionId = await createDeveloper();
    await publishReviewBinding();
    const review = admissionCommand("work.review", {
      repository: { id: 7 }, pullRequest: { number: 51, head: { sha: "b".repeat(40), repository: { cloneUrl: "https://github.com/acme/repo.git" } } },
    });
    const admitted = await store.admitEvent(review);
    expect(admitted.pendingWakes).toEqual([]);
    expect((await pool.query("select count(*)::int as count from dispatch_event_wake_offers where event_id=$1", [review.internalEventId])).rows[0]).toEqual({ count: 1 });

    await store.bindExecutionWakeContextValue({
      authorityId: "effect-51", authorityType: "github.pull-request-effect", boundAt: new Date().toISOString(),
      executionId, slot: "primaryPullRequestNumber", tenantId: "default", value: 51, waitName: "developer-pr-lifecycle",
    });
    expect((await pool.query("select count(*)::int as count from dispatch_execution_pending_wakes where execution_id=$1", [executionId])).rows[0]).toEqual({ count: 1 });
    await runDeveloperToCompletion(executionId);
    expect(await store.getExecution("default", executionId)).toMatchObject({ state: "QUEUED", workspace: { revision: { commit: "b".repeat(40) } } });
    await store.requestExecutionCancellation({ actor: "test", executionId, reason: "cleanup", requestedAt: new Date().toISOString(), tenantId: "default", transitionId: randomUUID() });
  });

  it("does not deliver an easy review wake to a medium developer lifecycle", async () => {
    const executionId = await createDeveloper({ waitName: "developer-medium-pr-lifecycle" });
    await publishReviewBinding("developer-easy-pr-lifecycle");
    const review = admissionCommand("work.review", {
      repository: { id: 7 }, pullRequest: { number: 52, head: { sha: "c".repeat(40), repository: { cloneUrl: "https://github.com/acme/repo.git" } } },
    });

    await store.admitEvent(review);
    await store.bindExecutionWakeContextValue({
      authorityId: "effect-52", authorityType: "github.pull-request-effect", boundAt: new Date().toISOString(),
      executionId, slot: "primaryPullRequestNumber", tenantId: "default", value: 52, waitName: "developer-medium-pr-lifecycle",
    });
    await runDeveloperToCompletion(executionId);

    expect(await store.getExecution("default", executionId)).toMatchObject({ state: "WAITING" });
    expect((await pool.query("select count(*)::int as count from dispatch_execution_pending_wakes where execution_id=$1", [executionId])).rows[0]).toEqual({ count: 0 });
    await store.requestExecutionCancellation({ actor: "test", executionId, reason: "cleanup", requestedAt: new Date().toISOString(), tenantId: "default", transitionId: randomUUID() });
  });

  it("rejects a superseded fence when reporting a PR effect", async () => {
    const executionId = await createDeveloper();
    const first = await store.claimNextQueuedExecution({ leaseOwner: `effect-worker-${randomUUID()}`, leaseDurationMs: 60_000 });
    if (!first || first.executionId !== executionId) throw new Error("Expected developer claim");
    const request = { owner: "acme", repo: "repo", title: "PR", head: "feature", base: "main" };
    const effect = await store.registerGitHubPullRequestEffect({ baseRef: "main", executionId, fencingToken: first.lease.fencingToken, headRef: "feature", pullRequestTitle: "PR",
      registeredAt: new Date().toISOString(), repositoryFullName: "acme/repo", repositoryId: 7, requestHash: hashCanonicalJson(request), tenantId: "default" });
    expect(effect.created).toBe(true);
    await pool.query("update dispatch_execution_attempts set lease_expires_at=now()-interval '1 second' where execution_id=$1", [executionId]);
    await store.recoverExpiredExecutionLeases({ limit: 10, maxAttempts: 3, retryDelayMs: 0 });
    await store.promoteDueExecutionRetries({ limit: 10 });
    const second = await store.claimNextQueuedExecution({ leaseOwner: `effect-recovery-${randomUUID()}`, leaseDurationMs: 60_000 });
    if (!second || second.executionId !== executionId) throw new Error("Expected recovered developer claim");
    expect(second.lease.fencingToken).not.toBe(first.lease.fencingToken);
    await expect(store.reportGitHubPullRequestEffect({ effectId: effect.id, executionId, fencingToken: first.lease.fencingToken,
      githubPullRequestId: "9001", pullRequestNumber: 61, pullRequestUrl: "https://github.com/acme/repo/pull/61", reportedAt: new Date().toISOString(), tenantId: "default" }))
      .rejects.toThrow("Execution effect capability is invalid");
    expect((await pool.query("select state,github_pull_request_id,pull_request_number,pull_request_url from dispatch_github_pull_request_effects where id=$1", [effect.id])).rows[0])
      .toEqual({ state: "REGISTERED", github_pull_request_id: null, pull_request_number: null, pull_request_url: null });

    await expect(store.registerGitHubPullRequestEffect({ baseRef: "main", executionId, fencingToken: second.lease.fencingToken, headRef: "feature", pullRequestTitle: "PR",
      registeredAt: new Date().toISOString(), repositoryFullName: "acme/repo", repositoryId: 7, requestHash: hashCanonicalJson(request), tenantId: "default" }))
      .resolves.toMatchObject({ created: false, id: effect.id });
    await store.reportGitHubPullRequestEffect({ effectId: effect.id, executionId, fencingToken: second.lease.fencingToken,
      githubPullRequestId: "9001", pullRequestNumber: 61, pullRequestUrl: "https://github.com/acme/repo/pull/61", reportedAt: new Date().toISOString(), tenantId: "default" });
    expect((await pool.query("select state from dispatch_github_pull_request_effects where id=$1", [effect.id])).rows[0]).toEqual({ state: "REPORTED" });
    await store.requestExecutionCancellation({ actor: "test", executionId, reason: "cleanup", requestedAt: new Date().toISOString(), tenantId: "default", transitionId: randomUUID() });
    await pool.query("delete from dispatch_github_pull_request_effects where id=$1", [effect.id]);
  });

  it("recovers a registered effect from its exact signed PR event", async () => {
    const executionId = await createDeveloper();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: `recovery-worker-${randomUUID()}`, leaseDurationMs: 60_000 });
    if (!claimed || claimed.executionId !== executionId) throw new Error("Expected developer claim");
    const effect = await store.registerGitHubPullRequestEffect({ baseRef: "main", executionId, fencingToken: claimed.lease.fencingToken,
      headRef: "dispatch/issue-7", pullRequestTitle: "Fix issue 7", registeredAt: new Date().toISOString(), repositoryFullName: "acme/repo",
      repositoryId: 7, requestHash: hashCanonicalJson({ owner: "acme", repo: "repo", title: "Fix issue 7", head: "dispatch/issue-7", base: "main" }), tenantId: "default" });
    await store.admitEvent(admissionCommand("com.github.pull_request.opened", {
      repository: { id: 7 }, pullRequest: { id: 9010, number: 70, title: "Fix issue 7", head: { ref: "dispatch/issue-7" }, base: { ref: "main" } },
    }));
    await store.reconcileGitHubPullRequestEffects("default", { repositoryId: 7, githubPullRequestId: "9010", pullRequestNumber: 70 });
    expect((await pool.query("select state,pull_request_number from dispatch_github_pull_request_effects where id=$1", [effect.id])).rows[0]).toEqual({ state: "CONFIRMED", pull_request_number: 70 });
    expect((await pool.query("select correlation from dispatch_execution_wake_contexts where execution_id=$1", [executionId])).rows[0].correlation.pullRequestNumber).toBe(70);
    await store.requestExecutionCancellation({ actor: "test", executionId, reason: "cleanup", requestedAt: new Date().toISOString(), tenantId: "default", transitionId: randomUUID() });
    await pool.query("delete from dispatch_github_pull_request_effects where id=$1", [effect.id]);
  });

  async function createDeveloper(options: { requireGitHubPullRequestEffect?: boolean; waitName?: string } = {}): Promise<string> {
    const id = randomUUID();
    await store.publishBindingVersion({
      bindingId: `developer-${id}`, createdAt: new Date().toISOString(), disabledAt: null, enabled: true, id: randomUUID(), profile: { id: "developer", version: 1 }, tenantId: "default", triggerId: "events", version: 1,
      definition: {
        schemaVersion: 1, eventTypes: ["work.start"], filter: { all: [{ path: "/key", op: "eq", value: id }] }, prompt: { literal: "Develop.", includeEvent: "data" }, workspace: { type: "empty" },
        afterTurn: { disposition: "wait", wait: { name: options.waitName ?? "developer-pr-lifecycle", correlation: [
          { name: "repositoryId", source: "event", path: "/repository/id" },
          { name: "pullRequestNumber", source: "supplied", slot: "primaryPullRequestNumber" },
        ], deadlineSeconds: 600, admitWhileBusy: true } },
        ...(options.requireGitHubPullRequestEffect ? { requireGitHubPullRequestEffect: true } : {}),
      },
    });
    return (await store.admitEvent(admissionCommand("work.start", { key: id, repository: { id: 7, fullName: "acme/repo" } }))).executions[0]!.id;
  }

  async function publishReviewBinding(waitName = "developer-pr-lifecycle"): Promise<void> {
    const id = randomUUID();
    await store.publishBindingVersion({
      bindingId: `review-${id}`, createdAt: new Date().toISOString(), disabledAt: null, enabled: true, id: randomUUID(), profile: { id: "developer", version: 1 }, tenantId: "default", triggerId: "events", version: 1,
      definition: { schemaVersion: 1, disposition: "wake", eventTypes: ["work.review"], filter: { all: [] }, wake: {
        waitName, delivery: "active-or-coalesced", correlation: [
          { name: "repositoryId", path: "/repository/id" }, { name: "pullRequestNumber", path: "/pullRequest/number" },
        ], action: { type: "continue", prompt: { literal: "Address review.", includeEvent: "data" }, workspace: {
          type: "git", repository: { url: { path: "/pullRequest/head/repository/cloneUrl" } }, revision: { commit: { path: "/pullRequest/head/sha" } },
        } },
      } },
    });
  }

  async function runDeveloperToCompletion(executionId: string) {
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: `worker-${randomUUID()}`, leaseDurationMs: 60_000 });
    if (!claimed || claimed.executionId !== executionId) throw new Error("Expected developer claim");
    await store.transitionLeasedExecution({ actor: claimed.lease.leaseOwner, attempt: claimed.lease.attempt, executionId,
      expectedAttemptState: "LEASED", expectedExecutionState: "PROVISIONING", fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner, reason: "ready", targetAttemptState: "RUNNING", targetExecutionState: "RUNNING", tenantId: "default" });
    await store.completeLeasedExecutionTurn({ actor: claimed.lease.leaseOwner, attempt: claimed.lease.attempt, executionId,
      fencingToken: claimed.lease.fencingToken, leaseOwner: claimed.lease.leaseOwner, reason: "done", result: null, tenantId: "default" });
    return claimed;
  }

  function admissionCommand(type: string, data: JsonValue) {
    const event = { specversion: "1.0" as const, id: randomUUID(), source: "https://example.test/events", type, datacontenttype: "application/json", data };
    return { admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId: "events", event }), admittedAt: new Date().toISOString(), event, internalEventId: randomUUID(), sourceDeduplicationKey: randomUUID(), tenantId: "default", triggerId: "events" };
  }
});
