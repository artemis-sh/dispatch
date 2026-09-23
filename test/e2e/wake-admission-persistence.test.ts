import { randomUUID } from "node:crypto";
import pg from "pg";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { hashCanonicalJson, type JsonValue } from "../../src/json.js";
import type { BindingWorkspace } from "../../src/workspace/types.js";
import { WorkspaceResolutionError } from "../../src/workspace/resolver.js";
import { createPostgresRuntimeStore, type PostgresRuntimeStore } from "../../src/runtime/postgres.js";

const { Pool } = pg;

describe("wake admission persistence", () => {
  let postgres: StartedTestContainer;
  let store: PostgresRuntimeStore;
  let pool: pg.Pool;
  let profileId: string;

  beforeAll(async () => {
    postgres = await new GenericContainer("postgres:16-alpine")
      .withEnvironment({ POSTGRES_DB: "dispatch", POSTGRES_PASSWORD: "dispatch-password", POSTGRES_USER: "dispatch" })
      .withExposedPorts(5432)
      .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
      .start();
    const connectionString = `postgresql://dispatch:dispatch-password@${postgres.getHost()}:${postgres.getMappedPort(5432)}/dispatch`;
    store = await createPostgresRuntimeStore({ connectionString, runMigrations: true, ssl: false, sslRejectUnauthorized: false });
    pool = new Pool({ connectionString });
    profileId = randomUUID();
    const createdAt = new Date().toISOString();
    await store.publishProfileVersion({
      createdAt, id: profileId, profileId: "developer", tenantId: "default", version: 1,
      definition: {
        schemaVersion: 1, runtime: { type: "opencode", agent: "developer", opencodeConfig: { agent: { developer: {} } } },
        sandbox: { templateName: "developer", warmPool: "none" }, connections: [], permissions: { onRequest: "fail" }, timeoutSeconds: 3600,
      },
    });
    await store.createTrigger({ config: { schemaVersion: 1 }, createdAt, disabledAt: null, enabled: true, id: "events", tenantId: "default", type: "cloudevents.http" });
  }, 120_000);

  afterAll(async () => {
    await pool?.end();
    await store?.close();
    await postgres?.stop();
  });

  it("atomically consumes a wait, queues immutable continuation input, and replays the wake result", async () => {
    const correlation = randomUUID();
    const executionId = await createWaitingExecution(correlation);
    await publishWakeBinding("continue", "work.continue", {
      type: "continue", prompt: { literal: "Continue after review.", includeEvent: "data" },
    });
    const command = admissionCommand("work.continue", { key: correlation, review: "changes requested" });

    const first = await store.admitEvent(command);
    const replay = await store.admitEvent(command);

    expect(first).toMatchObject({
      replayed: false,
      executions: [],
      wakes: [{ executionId, action: "CONTINUED", inputSequence: 2, state: "QUEUED", binding: { id: "continue", version: 1 } }],
    });
    expect(replay).toEqual({ ...first, replayed: true });
    const wait = (await pool.query("select state, ended_at from dispatch_event_waits where execution_id = $1", [executionId])).rows[0];
    expect(wait).toMatchObject({ state: "CONSUMED", ended_at: expect.any(Date) });
    expect((await pool.query("select kind, sequence, input from dispatch_execution_inputs where execution_id = $1 order by sequence", [executionId])).rows).toEqual([
      expect.objectContaining({ kind: "INITIAL", sequence: 1 }),
      {
        kind: "WAKE", sequence: 2,
        input: expect.objectContaining({ text: expect.stringContaining("Continue after review."), context: { event: { key: correlation, review: "changes requested" }, includeEvent: "data" } }),
      },
    ]);
    expect((await pool.query("select count(*)::int as count from dispatch_event_wakes where execution_id = $1", [executionId])).rows[0]).toEqual({ count: 1 });
    expect((await pool.query("select count(*)::int as count from dispatch_outbox where aggregate_type = 'event-wake' and payload->>'executionId' = $1", [executionId])).rows[0]).toEqual({ count: 1 });
    expect(await store.getExecution("default", executionId)).toMatchObject({
      input: { text: expect.stringContaining("Continue after review.") }, state: "QUEUED",
    });
    const timeout = (await pool.query<{ timeout_at: Date }>("select timeout_at from dispatch_executions where id = $1", [executionId])).rows[0]!.timeout_at;
    expect(timeout.getTime()).toBeGreaterThan(Date.now() + 3_500_000);
    expect(timeout.getTime()).toBeLessThan(Date.now() + 3_700_000);

    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "continuation-worker", leaseDurationMs: 60_000 });
    expect(claimed).toMatchObject({ executionId, input: { text: expect.stringContaining("Continue after review.") }, lease: { attempt: 2 } });
  });

  it("pins a continuation and all retries to the wake event workspace revision", async () => {
    const correlation = randomUUID();
    const initialCommit = "a".repeat(40);
    const updatedCommit = "b".repeat(40);
    const executionId = await createWaitingExecution(correlation, {
      type: "git", repository: { url: { path: "/repositoryUrl" } }, revision: { commit: { path: "/commit" } },
    }, { repositoryUrl: "https://github.com/acme/repo.git", commit: initialCommit });
    await publishWakeBinding(`revision-${correlation}`, "work.synchronize", {
      type: "continue",
      prompt: { literal: "Review updated revision.", includeEvent: "data" },
      workspace: { type: "git", repository: { url: { path: "/repositoryUrl" } }, revision: { commit: { path: "/commit" } } },
    });

    await store.admitEvent(admissionCommand("work.synchronize", {
      key: correlation, repositoryUrl: "https://github.com/acme/repo.git", commit: updatedCommit,
    }));

    const expectedWorkspace = {
      type: "git", repository: { url: "https://github.com/acme/repo.git" }, revision: { type: "commit", commit: updatedCommit },
    };
    expect(await store.getExecution("default", executionId)).toMatchObject({ workspace: expectedWorkspace });
    expect((await pool.query("select sequence, workspace from dispatch_execution_inputs where execution_id = $1 order by sequence", [executionId])).rows).toEqual([
      { sequence: 1, workspace: { ...expectedWorkspace, revision: { type: "commit", commit: initialCommit } } },
      { sequence: 2, workspace: expectedWorkspace },
    ]);
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: `revision-worker-${correlation}`, leaseDurationMs: 60_000 });
    expect(claimed).toMatchObject({ executionId, workspace: expectedWorkspace, lease: { attempt: 2 } });
    await pool.query("update dispatch_execution_attempts set lease_expires_at = now() - interval '1 second' where execution_id = $1 and attempt = 2", [executionId]);
    await pool.query("update dispatch_executions set state = 'RUNNING' where id = $1", [executionId]);
    await pool.query(`update dispatch_execution_attempts set state = 'RUNNING', workload_name = 'sandbox', opencode_session_id = 'session'
      where execution_id = $1 and attempt = 2`, [executionId]);
    const retried = await store.claimExpiredRunningExecution({ leaseOwner: `retry-worker-${correlation}`, leaseDurationMs: 60_000 });
    expect(retried).toMatchObject({ executionId, workspace: expectedWorkspace, lease: { attempt: 2 } });
  });

  it("rolls back admission when a continuation workspace cannot be resolved", async () => {
    const correlation = randomUUID();
    const executionId = await createWaitingExecution(correlation);
    await publishWakeBinding(`invalid-revision-${correlation}`, "work.invalid-revision", {
      type: "continue",
      prompt: { literal: "Review updated revision.", includeEvent: "data" },
      workspace: { type: "git", repository: { url: { path: "/repositoryUrl" } }, revision: { commit: { path: "/commit" } } },
    });
    const command = admissionCommand("work.invalid-revision", {
      key: correlation, repositoryUrl: "https://github.com/acme/repo.git", commit: "mutable-branch",
    });

    await expect(store.admitEvent(command)).rejects.toBeInstanceOf(WorkspaceResolutionError);

    expect((await pool.query("select count(*)::int as count from dispatch_events where id = $1", [command.internalEventId])).rows[0]).toEqual({ count: 0 });
    expect((await pool.query("select state from dispatch_event_waits where execution_id = $1", [executionId])).rows[0]).toEqual({ state: "ACTIVE" });
    expect(await store.getExecution("default", executionId)).toMatchObject({ state: "WAITING" });
  });

  it("coalesces busy continuation events and applies the latest workspace at turn completion", async () => {
    const correlation = randomUUID();
    const executionId = await createQueuedExecution(correlation);
    await publishWakeBinding(`busy-${correlation}`, "work.busy", {
      type: "continue", prompt: { literal: "Use latest revision.", includeEvent: "data" },
      workspace: { type: "git", repository: { url: { path: "/repositoryUrl" } }, revision: { commit: { path: "/commit" } } },
    });
    const firstCommand = admissionCommand("work.busy", { key: correlation, repositoryUrl: "https://github.com/acme/repo.git", commit: "a".repeat(40) });
    const secondCommand = admissionCommand("work.busy", { key: correlation, repositoryUrl: "https://github.com/acme/repo.git", commit: "b".repeat(40) });

    const first = await store.admitEvent(firstCommand);
    const second = await store.admitEvent(secondCommand);
    expect(first.pendingWakes).toEqual([expect.objectContaining({ executionId, action: "CONTINUED", disposition: "PENDING" })]);
    expect(await store.admitEvent(firstCommand)).toEqual({ ...first, replayed: true });
    expect(second.pendingWakes).toEqual([expect.objectContaining({ executionId, action: "CONTINUED", disposition: "PENDING" })]);

    const claimed = await store.claimNextQueuedExecution({ leaseOwner: `busy-worker-${correlation}`, leaseDurationMs: 60_000 });
    if (!claimed || claimed.executionId !== executionId) throw new Error("Expected busy execution claim");
    await store.transitionLeasedExecution({
      actor: claimed.lease.leaseOwner, attempt: 1, executionId, expectedAttemptState: "LEASED", expectedExecutionState: "PROVISIONING",
      fencingToken: claimed.lease.fencingToken, leaseOwner: claimed.lease.leaseOwner, reason: "ready", targetAttemptState: "RUNNING", targetExecutionState: "RUNNING", tenantId: "default",
    });
    const completion = await store.completeLeasedExecutionTurn({
      actor: claimed.lease.leaseOwner, attempt: 1, executionId, fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner, reason: "complete", result: null, tenantId: "default",
    });
    expect(completion).toMatchObject({ applied: true, executionState: "QUEUED" });
    expect((await pool.query("select count(*)::int as count from dispatch_event_waits where execution_id=$1", [executionId])).rows[0]).toEqual({ count: 0 });
    expect(await store.getExecution("default", executionId)).toMatchObject({
      input: { text: expect.stringContaining("Use latest revision.") },
      workspace: { revision: { commit: "b".repeat(40) } },
    });
    expect((await pool.query("select count(*)::int as count from dispatch_execution_inputs where execution_id=$1", [executionId])).rows[0]).toEqual({ count: 2 });
    const appliedWake = (await pool.query<{ id: string }>("select id from dispatch_event_wakes where execution_id=$1 and wake_intent_id is not null", [executionId])).rows[0]!;
    expect((await pool.query<{ aggregate_id: string; payload: { wakeId: string } }>(
      "select aggregate_id, payload from dispatch_outbox where aggregate_type='event-wake' and aggregate_id=$1", [appliedWake.id],
    )).rows[0]).toMatchObject({ aggregate_id: appliedWake.id, payload: { wakeId: appliedWake.id } });
    const continuation = await store.claimNextQueuedExecution({ leaseOwner: `busy-continuation-${correlation}`, leaseDurationMs: 60_000 });
    if (!continuation || continuation.executionId !== executionId) throw new Error("Expected continuation claim");
    await store.transitionLeasedExecution({
      actor: continuation.lease.leaseOwner, attempt: 2, executionId, expectedAttemptState: "LEASED", expectedExecutionState: "PROVISIONING",
      fencingToken: continuation.lease.fencingToken, leaseOwner: continuation.lease.leaseOwner, reason: "ready", targetAttemptState: "RUNNING", targetExecutionState: "RUNNING", tenantId: "default",
    });
    await store.completeLeasedExecutionTurn({
      actor: continuation.lease.leaseOwner, attempt: 2, executionId, fencingToken: continuation.lease.fencingToken,
      leaseOwner: continuation.lease.leaseOwner, reason: "complete", result: null, tenantId: "default",
    });
  });

  it("lets a pending terminal wake dominate continuation and complete after the current turn", async () => {
    const correlation = randomUUID();
    const executionId = await createQueuedExecution(correlation);
    await publishWakeBinding(`continue-${correlation}`, "work.pending-continue", {
      type: "continue", prompt: { literal: "Continue.", includeEvent: "data" },
    });
    await publishWakeBinding(`terminal-${correlation}`, "work.pending-terminal", { type: "complete" });
    await store.admitEvent(admissionCommand("work.pending-continue", { key: correlation }));
    const terminal = await store.admitEvent(admissionCommand("work.pending-terminal", { key: correlation }));
    const dominated = await store.admitEvent(admissionCommand("work.pending-continue", { key: correlation, later: true }));
    expect(terminal.pendingWakes).toEqual([expect.objectContaining({ action: "COMPLETED", disposition: "PENDING" })]);
    expect(dominated.pendingWakes).toEqual([expect.objectContaining({ action: "CONTINUED", disposition: "DOMINATED" })]);

    const claimed = await store.claimNextQueuedExecution({ leaseOwner: `terminal-worker-${correlation}`, leaseDurationMs: 60_000 });
    if (!claimed || claimed.executionId !== executionId) throw new Error("Expected terminal execution claim");
    await store.transitionLeasedExecution({
      actor: claimed.lease.leaseOwner, attempt: 1, executionId, expectedAttemptState: "LEASED", expectedExecutionState: "PROVISIONING",
      fencingToken: claimed.lease.fencingToken, leaseOwner: claimed.lease.leaseOwner, reason: "ready", targetAttemptState: "RUNNING", targetExecutionState: "RUNNING", tenantId: "default",
    });
    const completion = await store.completeLeasedExecutionTurn({
      actor: claimed.lease.leaseOwner, attempt: 1, executionId, fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner, reason: "complete", result: null, tenantId: "default",
    });
    expect(completion).toMatchObject({ applied: true, executionState: "COMPLETED" });
    expect((await pool.query("select count(*)::int as count from dispatch_execution_inputs where execution_id=$1", [executionId])).rows[0]).toEqual({ count: 1 });
    expect((await pool.query("select count(*)::int as count from dispatch_execution_pending_wakes where execution_id=$1", [executionId])).rows[0]).toEqual({ count: 0 });
  });

  it("does not apply a wake binding to the execution created by the same event", async () => {
    const correlation = randomUUID();
    await store.publishBindingVersion({
      bindingId: `same-create-${correlation}`, createdAt: new Date().toISOString(), disabledAt: null, enabled: true, id: randomUUID(),
      profile: { id: "developer", version: 1 }, tenantId: "default", triggerId: "events", version: 1,
      definition: {
        schemaVersion: 1, eventTypes: ["work.same"], filter: { all: [{ path: "/key", op: "eq", value: correlation }] },
        prompt: { literal: "Start.", includeEvent: "data" }, workspace: { type: "empty" },
        afterTurn: { disposition: "wait", wait: { name: "work-lifecycle", correlation: [{ name: "key", path: "/key" }], deadlineSeconds: 600, admitWhileBusy: true } },
      },
    });
    await publishWakeBinding(`same-wake-${correlation}`, "work.same", { type: "continue", prompt: { literal: "Continue.", includeEvent: "data" } });

    const admitted = await store.admitEvent(admissionCommand("work.same", { key: correlation }));

    expect(admitted.executions).toHaveLength(1);
    expect(admitted.pendingWakes).toEqual([]);
    expect((await pool.query("select count(*)::int as count from dispatch_event_wake_intents where event_id=$1", [admitted.event.id])).rows[0]).toEqual({ count: 0 });
    await store.requestExecutionCancellation({
      actor: "test", executionId: admitted.executions[0]!.id, reason: "cleanup", requestedAt: new Date().toISOString(), tenantId: "default", transitionId: randomUUID(),
    });
  });

  it("clears a pending pointer on cancellation while retaining immutable intent history", async () => {
    const correlation = randomUUID();
    const executionId = await createQueuedExecution(correlation);
    await publishWakeBinding(`cancel-${correlation}`, "work.cancel-pending", { type: "continue", prompt: { literal: "Continue.", includeEvent: "data" } });
    await store.admitEvent(admissionCommand("work.cancel-pending", { key: correlation }));

    await store.requestExecutionCancellation({
      actor: "test", executionId, reason: "cancel", requestedAt: new Date().toISOString(), tenantId: "default", transitionId: randomUUID(),
    });

    expect((await pool.query("select count(*)::int as count from dispatch_execution_pending_wakes where execution_id=$1", [executionId])).rows[0]).toEqual({ count: 0 });
    expect((await pool.query("select count(*)::int as count from dispatch_event_wake_intents where execution_id=$1", [executionId])).rows[0]).toEqual({ count: 1 });
  });

  it("terminally completes a waiting execution without queueing another attempt", async () => {
    const correlation = randomUUID();
    const executionId = await createWaitingExecution(correlation);
    await publishWakeBinding(`complete-${correlation}`, "work.complete", { type: "complete" });
    const result = await store.admitEvent(admissionCommand("work.complete", { key: correlation }));

    expect(result.wakes).toEqual([expect.objectContaining({ executionId, action: "COMPLETED", inputSequence: null, state: "COMPLETED" })]);
    expect(await store.getExecution("default", executionId)).toMatchObject({ state: "COMPLETED" });
    expect((await pool.query("select count(*)::int as count from dispatch_execution_inputs where execution_id = $1", [executionId])).rows[0]).toEqual({ count: 1 });
    expect((await pool.query("select count(*)::int as count from dispatch_outbox where aggregate_type = 'event-wake' and payload->>'executionId' = $1", [executionId])).rows[0]).toEqual({ count: 0 });
  });

  it("cancels an unmerged pull request wait, releases its singleton, and replays the terminal result", async () => {
    const correlation = randomUUID();
    const executionId = await createWaitingExecution(correlation, { type: "empty" }, {}, "developer-issue");
    await publishWakeBinding(`cancel-${correlation}`, "com.github.pull_request.closed", {
      type: "cancel", reason: "PULL_REQUEST_CLOSED_UNMERGED",
    });
    const command = admissionCommand("com.github.pull_request.closed", { key: correlation, pullRequest: { merged: false } });

    const first = await store.admitEvent(command);
    const replay = await store.admitEvent(command);

    expect(first.wakes).toEqual([expect.objectContaining({ executionId, action: "CANCELLED", inputSequence: null, state: "CANCELLED" })]);
    expect(replay).toEqual({ ...first, replayed: true });
    expect(await store.getExecution("default", executionId)).toMatchObject({ state: "CANCELLED" });
    expect((await pool.query("select state from dispatch_event_waits where execution_id = $1", [executionId])).rows[0]).toEqual({ state: "CONSUMED" });
    expect((await pool.query("select state from dispatch_execution_attempts where execution_id = $1", [executionId])).rows).toEqual([{ state: "SUCCEEDED" }]);
    expect((await pool.query("select count(*)::int as count from dispatch_execution_inputs where execution_id = $1", [executionId])).rows[0]).toEqual({ count: 1 });
    expect((await pool.query("select count(*)::int as count from dispatch_outbox where aggregate_type = 'event-wake' and payload->>'executionId' = $1", [executionId])).rows[0]).toEqual({ count: 0 });
    expect((await pool.query("select from_state, to_state, actor, reason from dispatch_execution_transitions where execution_id = $1 order by sequence desc limit 1", [executionId])).rows[0]).toEqual({
      from_state: "WAITING", to_state: "CANCELLED", actor: "event-admission", reason: "PULL_REQUEST_CLOSED_UNMERGED",
    });

    const replacement = await store.admitEvent(admissionCommand("work.start", { key: correlation }));
    expect(replacement.executions).toHaveLength(1);
    expect(replacement.executions[0]!.id).not.toBe(executionId);
    await store.requestExecutionCancellation({
      actor: "test", executionId: replacement.executions[0]!.id, reason: "test cleanup", requestedAt: new Date().toISOString(), tenantId: "default", transitionId: randomUUID(),
    });
  });

  it("uses stable binding order when multiple wake policies match one wait", async () => {
    const correlation = randomUUID();
    const executionId = await createWaitingExecution(correlation);
    await publishWakeBinding(`a-complete-${correlation}`, "work.overlap", { type: "complete" });
    await publishWakeBinding(`z-continue-${correlation}`, "work.overlap", {
      type: "continue", prompt: { literal: "Deterministic winner.", includeEvent: "data" },
    });

    const result = await store.admitEvent(admissionCommand("work.overlap", { key: correlation }));

    expect(result.wakes).toEqual([expect.objectContaining({
      executionId, action: "COMPLETED", binding: { id: `a-complete-${correlation}`, version: 1 },
    })]);
    expect((await pool.query("select count(*)::int as count from dispatch_event_wakes where execution_id = $1", [executionId])).rows[0]).toEqual({ count: 1 });
  });

  it("does not consume an overdue active wait before maintenance expires it", async () => {
    const correlation = randomUUID();
    const executionId = await createWaitingExecution(correlation);
    await pool.query(`update dispatch_event_waits
      set activated_at = now() - interval '2 seconds', deadline_at = now() - interval '1 second'
      where execution_id = $1`, [executionId]);
    await publishWakeBinding(`late-${correlation}`, "work.late", {
      type: "continue", prompt: { literal: "Too late.", includeEvent: "data" },
    });

    const result = await store.admitEvent(admissionCommand("work.late", { key: correlation }));

    expect(result.wakes).toEqual([]);
    expect((await pool.query("select state from dispatch_event_waits where execution_id = $1", [executionId])).rows[0]).toEqual({ state: "ACTIVE" });
    expect(await store.getExecution("default", executionId)).toMatchObject({ state: "WAITING" });
  });

  it("does not consume a wait whose deadline elapses before the consume mutation", async () => {
    const correlation = randomUUID();
    const executionId = await createWaitingExecution(correlation);
    await pool.query(`update dispatch_event_waits
      set deadline_at = clock_timestamp() + interval '1 second'
      where execution_id = $1`, [executionId]);
    await publishWakeBinding(`deadline-race-${correlation}`, "work.deadline-race", {
      type: "continue", prompt: { literal: "Too late.", includeEvent: "data" },
    });
    await pool.query(`create function delay_event_wait_consume() returns trigger language plpgsql as $$
      begin
        perform pg_sleep(1.25);
        return null;
      end
    $$`);
    await pool.query(`create trigger delay_event_wait_consume
      before update on dispatch_event_waits for each statement execute function delay_event_wait_consume()`);

    const startedAt = Date.now();
    let result: Awaited<ReturnType<typeof store.admitEvent>>;
    try {
      result = await store.admitEvent(admissionCommand("work.deadline-race", { key: correlation }));
    } finally {
      await pool.query("drop trigger delay_event_wait_consume on dispatch_event_waits");
      await pool.query("drop function delay_event_wait_consume() ");
    }

    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(1_200);
    expect(result.wakes).toEqual([]);
    expect((await pool.query("select state from dispatch_event_waits where execution_id = $1", [executionId])).rows[0]).toEqual({ state: "ACTIVE" });
    expect(await store.getExecution("default", executionId)).toMatchObject({ state: "WAITING" });

    expect(await store.expireDueEventWaits({ limit: 1 })).toEqual([
      expect.objectContaining({ executionId, tenantId: "default" }),
    ]);
    expect((await pool.query("select state from dispatch_event_waits where execution_id = $1", [executionId])).rows[0]).toEqual({ state: "EXPIRED" });
    expect(await store.getExecution("default", executionId)).toMatchObject({ state: "TIMED_OUT" });
  });

  async function createWaitingExecution(
    correlation: string,
    workspace: BindingWorkspace = { type: "empty" },
    extraData: Record<string, JsonValue> = {},
    activeSingletonName?: string,
  ): Promise<string> {
    const bindingId = `start-${correlation}`;
    const createdAt = new Date().toISOString();
    await store.publishBindingVersion({
      bindingId, createdAt, disabledAt: null, enabled: true, id: randomUUID(), profile: { id: "developer", version: 1 }, tenantId: "default", triggerId: "events", version: 1,
      definition: {
        schemaVersion: 1, eventTypes: ["work.start"], filter: { all: [{ path: "/key", op: "eq", value: correlation }] },
        prompt: { literal: "Start work.", includeEvent: "data" }, workspace,
        ...(activeSingletonName ? { activeSingleton: { name: activeSingletonName, key: ["/key"] } } : {}),
        afterTurn: { disposition: "wait", wait: { name: "work-lifecycle", correlation: [{ name: "key", path: "/key" }], deadlineSeconds: 600 } },
      },
    });
    const admitted = await store.admitEvent(admissionCommand("work.start", { key: correlation, ...extraData }));
    const executionId = admitted.executions[0]!.id;
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: `worker-${correlation}`, leaseDurationMs: 60_000 });
    if (!claimed || claimed.executionId !== executionId) throw new Error("Expected execution claim");
    await store.transitionLeasedExecution({
      actor: claimed.lease.leaseOwner, attempt: claimed.lease.attempt, executionId, expectedAttemptState: "LEASED", expectedExecutionState: "PROVISIONING",
      fencingToken: claimed.lease.fencingToken, leaseOwner: claimed.lease.leaseOwner, reason: "sandbox ready", targetAttemptState: "RUNNING", targetExecutionState: "RUNNING", tenantId: "default",
    });
    const completed = await store.completeLeasedExecutionTurn({
      actor: claimed.lease.leaseOwner, attempt: claimed.lease.attempt, executionId, fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner, reason: "turn complete", result: null, tenantId: "default",
    });
    if (!completed.applied || completed.executionState !== "WAITING") throw new Error("Expected waiting execution");
    return executionId;
  }

  async function createQueuedExecution(correlation: string): Promise<string> {
    const bindingId = `busy-start-${correlation}`;
    await store.publishBindingVersion({
      bindingId, createdAt: new Date().toISOString(), disabledAt: null, enabled: true, id: randomUUID(),
      profile: { id: "developer", version: 1 }, tenantId: "default", triggerId: "events", version: 1,
      definition: {
        schemaVersion: 1, eventTypes: ["work.busy-start"], filter: { all: [{ path: "/key", op: "eq", value: correlation }] },
        prompt: { literal: "Start busy work.", includeEvent: "data" }, workspace: { type: "empty" },
        afterTurn: { disposition: "wait", wait: {
          name: "work-lifecycle", correlation: [{ name: "key", path: "/key" }], deadlineSeconds: 600, admitWhileBusy: true,
        } },
      },
    });
    return (await store.admitEvent(admissionCommand("work.busy-start", { key: correlation }))).executions[0]!.id;
  }

  async function publishWakeBinding(bindingId: string, eventType: string, action: { type: "complete" } | { type: "cancel"; reason: string } | {
    type: "continue"; prompt: { literal: string; includeEvent: "data" }; workspace?: BindingWorkspace;
  }): Promise<void> {
    await store.publishBindingVersion({
      bindingId, createdAt: new Date().toISOString(), disabledAt: null, enabled: true, id: randomUUID(),
      profile: { id: "developer", version: 1 }, tenantId: "default", triggerId: "events", version: 1,
      definition: {
        disposition: "wake", schemaVersion: 1, eventTypes: [eventType], filter: { all: [] },
        wake: { waitName: "work-lifecycle", ...(action.type === "cancel" ? {} : { delivery: "active-or-coalesced" }), correlation: [{ name: "key", path: "/key" }], action },
      },
    });
  }
});

function admissionCommand(type: string, data: JsonValue) {
  const event = { data, datacontenttype: "application/json", id: randomUUID(), source: "/test/wake", specversion: "1.0" as const, type };
  const command = {
    admittedAt: new Date().toISOString(), event, internalEventId: randomUUID(), sourceDeduplicationKey: randomUUID(), tenantId: "default", triggerId: "events",
  };
  return { ...command, admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId: command.triggerId, event } as JsonValue) };
}
