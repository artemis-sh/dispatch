import { randomUUID } from "node:crypto";
import pg from "pg";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { hashCanonicalJson } from "../../src/json.js";
import {
  createPostgresRuntimeStore,
  PersistedExecutionCorruptionError,
  type PostgresRuntimeStore,
} from "../../src/runtime/postgres.js";
import { ExecutionCancellationConflictError } from "../../src/execution/types.js";

const { Pool } = pg;

describe("dispatcher persistence", () => {
  let postgres: StartedTestContainer;
  let store: PostgresRuntimeStore;
  let pool: pg.Pool;
  let profileId: string;
  let eventSequence = 0;

  beforeAll(async () => {
    postgres = await startPostgres();
    const connectionString = postgresConnectionString(postgres);
    store = await createPostgresRuntimeStore({
      connectionString,
      runMigrations: true,
      ssl: false,
      sslRejectUnauthorized: false,
    });
    pool = new Pool({ connectionString });
    profileId = `profile-${randomUUID()}`;
    await store.publishProfileVersion({
      createdAt: new Date().toISOString(),
      definition: {
        schemaVersion: 1,
        runtime: { type: "opencode", agent: "coder", opencodeConfig: { agent: { coder: { prompt: "Test" } } } },
        sandbox: { templateName: "opencode", warmPool: "none" },
        connections: [],
        permissions: { onRequest: "fail" },
        timeoutSeconds: 3_600,
      },
      id: randomUUID(),
      profileId,
      tenantId: "default",
      version: 1,
    });
    const createdAt = new Date().toISOString();
    await store.createTrigger({
      config: { schemaVersion: 1 }, createdAt, disabledAt: null, enabled: true,
      id: "dispatcher-test", tenantId: "default", type: "cloudevents.http",
    });
    await store.publishBindingVersion({
      bindingId: "dispatcher", createdAt,
      definition: {
        eventTypes: ["dev.dispatch.execution.submitted"], filter: { all: [] },
        prompt: { includeEvent: "none", literal: "Run" }, schemaVersion: 1, workspace: { type: "empty" },
      },
      disabledAt: null, enabled: true, id: "dispatcher-v1", profile: { id: profileId, version: 1 },
      tenantId: "default", triggerId: "dispatcher-test", version: 1,
    });
    await store.publishBindingVersion({
      bindingId: "dispatcher-git", createdAt,
      definition: {
        eventTypes: ["dev.dispatch.git-execution.submitted"], filter: { all: [] },
        prompt: { includeEvent: "none", literal: "Run" }, schemaVersion: 1,
        workspace: {
          type: "git",
          repository: { url: { path: "/repository" } },
          revision: { commit: { path: "/commit" } },
        },
      },
      disabledAt: null, enabled: true, id: "dispatcher-git-v1", profile: { id: profileId, version: 1 },
      tenantId: "default", triggerId: "dispatcher-test", version: 1,
    });
  });

  afterAll(async () => {
    await pool?.end();
    await store?.close();
    await postgres?.stop();
  });

  it("atomically claims a queued execution and creates its first fenced attempt", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-a", leaseDurationMs: 60_000 });

    expect(claimed).toMatchObject({
      executionId,
      lease: { attempt: 1, leaseOwner: "dispatcher-a" },
      profileVersion: { profileId, version: 1 },
    });
    expect(claimed?.lease.fencingToken).toBeTruthy();

    const persisted = await executionSnapshot(executionId);
    expect(persisted.execution.state).toBe("PROVISIONING");
    expect(persisted.attempts).toEqual([expect.objectContaining({
      attempt: 1,
      fencing_token: claimed?.lease.fencingToken,
      lease_owner: "dispatcher-a",
      state: "LEASED",
    })]);
    expect(persisted.transitions.at(-1)).toMatchObject({
      attempt: 1,
      from_state: "QUEUED",
      sequence: 4,
      to_state: "PROVISIONING",
    });
  });

  it("persists, replays, and claims a canonical git workspace unchanged", async () => {
    const createdAt = new Date().toISOString();
    const sequence = ++eventSequence;
    const event = {
      data: {
        commit: "ABCDEF0123456789ABCDEF0123456789ABCDEF01",
        repository: "https://Git.Example.test/acme/repo name",
      },
      datacontenttype: "application/json",
      id: `git-event-${sequence}`,
      source: "/test/dispatcher",
      specversion: "1.0" as const,
      time: createdAt,
      type: "dev.dispatch.git-execution.submitted",
    };
    const command = {
      admittedAt: createdAt,
      admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId: "dispatcher-test", event }),
      event,
      internalEventId: randomUUID(),
      sourceDeduplicationKey: `git-delivery-${sequence}`,
      tenantId: "default",
      triggerId: "dispatcher-test",
    };
    const expectedWorkspace = {
      type: "git" as const,
      repository: { url: "https://git.example.test/acme/repo%20name" },
      revision: { type: "commit" as const, commit: "abcdef0123456789abcdef0123456789abcdef01" },
    };

    const admitted = await store.admitEvent(command);
    const replayed = await store.admitEvent(command);
    const execution = admitted.executions[0]!;
    expect(execution.workspace).toEqual(expectedWorkspace);
    expect(replayed).toEqual({ ...admitted, replayed: true });
    expect((await pool.query("select workspace from dispatch_executions where id = $1", [execution.id])).rows[0]).toEqual({
      workspace: expectedWorkspace,
    });
    expect((await store.getExecution("default", execution.id))?.workspace).toEqual(expectedWorkspace);

    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-git", leaseDurationMs: 60_000 });
    expect(claimed).toMatchObject({ executionId: execution.id, workspace: expectedWorkspace });
  });

  it("rejects malformed persisted workspaces when claiming without committing the claim", async () => {
    const executionId = await queueExecution();
    await pool.query("update dispatch_execution_inputs set workspace = '{}'::jsonb where execution_id = $1 and sequence = 1", [executionId]);

    await expect(store.claimNextQueuedExecution({ leaseOwner: "dispatcher-corrupt", leaseDurationMs: 60_000 }))
      .rejects.toBeInstanceOf(PersistedExecutionCorruptionError);
    expect((await pool.query("select state from dispatch_executions where id = $1", [executionId])).rows[0]).toEqual({ state: "QUEUED" });
    expect((await pool.query(
      "select count(*)::int as count from dispatch_execution_attempts where execution_id = $1",
      [executionId],
    )).rows[0]).toEqual({ count: 0 });

    await pool.query("update dispatch_execution_inputs set workspace = $1 where execution_id = $2 and sequence = 1", [{ type: "empty" }, executionId]);
    expect(await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-cleanup", leaseDurationMs: 60_000 }))
      .toMatchObject({ executionId });
  });

  it("allows only one dispatcher to claim one queued execution", async () => {
    const executionId = await queueExecution();
    const [first, second] = await Promise.all([
      store.claimNextQueuedExecution({ leaseOwner: "dispatcher-a", leaseDurationMs: 60_000 }),
      store.claimNextQueuedExecution({ leaseOwner: "dispatcher-b", leaseDurationMs: 60_000 }),
    ]);
    const claims = [first, second].filter((claim) => claim?.executionId === executionId);

    expect(claims).toHaveLength(1);
    expect((await pool.query(
      "select count(*)::int as count from dispatch_execution_attempts where execution_id = $1",
      [executionId],
    )).rows[0]).toEqual({ count: 1 });
    expect((await pool.query(
      "select count(*)::int as count from dispatch_execution_transitions where execution_id = $1 and to_state = 'PROVISIONING'",
      [executionId],
    )).rows[0]).toEqual({ count: 1 });
  });

  it("renews only the current unexpired execution lease", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-a", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution to be claimed");

    expect(await store.renewExecutionLease({
      executionId,
      tenantId: "default",
      attempt: claimed.lease.attempt,
      fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner,
      leaseDurationMs: 120_000,
    })).toMatchObject({ status: "RENEWED", leaseExpiresAt: expect.any(Date) });
    expect(await store.renewExecutionLease({
      executionId,
      tenantId: "default",
      attempt: claimed.lease.attempt,
      fencingToken: "stale-token",
      leaseOwner: claimed.lease.leaseOwner,
      leaseDurationMs: 120_000,
    })).toBe("LOST");

    await pool.query(
      "update dispatch_execution_attempts set lease_expires_at = now() - interval '1 second' where execution_id = $1",
      [executionId],
    );
    expect(await store.renewExecutionLease({
      executionId,
      tenantId: "default",
      attempt: claimed.lease.attempt,
      fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner,
      leaseDurationMs: 120_000,
    })).toBe("LOST");
    await pool.query(
      "update dispatch_execution_attempts set lease_expires_at = now() + interval '1 hour' where execution_id = $1",
      [executionId],
    );
  });

  it("starts and succeeds an execution under the active fence", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-a", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution to be claimed");
    const lease = claimed.lease;

    await expect(store.transitionLeasedExecution({
      executionId,
      tenantId: "default",
      attempt: lease.attempt,
      fencingToken: lease.fencingToken,
      leaseOwner: lease.leaseOwner,
      expectedExecutionState: "PROVISIONING",
      expectedAttemptState: "LEASED",
      targetExecutionState: "RUNNING",
      targetAttemptState: "RUNNING",
      actor: "dispatcher-a",
      reason: "sandbox ready",
      workloadName: "execution-attempt-1",
      opencodeSessionId: "session-1",
    })).resolves.toEqual({ applied: true, executionState: "RUNNING", attemptState: "RUNNING" });

    await expect(store.completeLeasedExecutionTurn({
      executionId, tenantId: "default", attempt: lease.attempt,
      fencingToken: lease.fencingToken, leaseOwner: lease.leaseOwner,
      actor: "dispatcher-a", reason: "agent completed", result: { output: "ok" },
    })).resolves.toEqual({ applied: true, executionState: "SUCCEEDED", attemptState: "SUCCEEDED" });

    const persisted = await executionSnapshot(executionId);
    expect(persisted.execution).toMatchObject({ state: "SUCCEEDED", result: { output: "ok" }, completed_at: null });
    expect(persisted.attempts[0]).toMatchObject({
      state: "SUCCEEDED",
      lease_owner: null,
      lease_expires_at: null,
      workload_name: "execution-attempt-1",
      opencode_session_id: "session-1",
    });
    expect(persisted.transitions.map((transition) => transition.to_state)).toEqual([
      "RECEIVED", "PLANNED", "QUEUED", "PROVISIONING", "RUNNING", "SUCCEEDED",
    ]);
  });

  it("skips unchanged checkpoints and prevents stale successful executions from moving them backward", async () => {
    const token = randomUUID();
    const bindingId = `checkpoint-${token}`;
    const eventType = `dev.dispatch.checkpoint.${token}`;
    const createdAt = new Date().toISOString();
    const definition = {
      schemaVersion: 1 as const, eventTypes: [eventType], filter: { all: [] },
      prompt: { includeEvent: "none" as const, literal: "Audit range" }, workspace: { type: "empty" as const },
      checkpoint: {
        name: "repository-audit", key: ["/repository/id"], value: { path: "/revision" },
        advanceOn: "succeeded" as const, unchanged: "skip" as const,
      },
    };
    await store.publishBindingVersion({
      bindingId, createdAt, disabledAt: null, enabled: true, id: randomUUID(), profile: { id: profileId, version: 1 },
      tenantId: "default", triggerId: "dispatcher-test", version: 1,
      definition,
    });
    const admit = async (revision: string) => {
      const eventId = randomUUID();
      const event = { data: { repository: { id: 42 }, revision }, datacontenttype: "application/json",
        id: eventId, source: "/test/checkpoint", specversion: "1.0" as const, type: eventType };
      return store.admitEvent({ admittedAt: new Date().toISOString(),
        admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId: "dispatcher-test", event }), event,
        internalEventId: randomUUID(), sourceDeduplicationKey: eventId, tenantId: "default", triggerId: "dispatcher-test" });
    };
    const complete = async (executionId: string, worker: string) => {
      const claimed = await startRunningExecution(executionId, worker);
      return store.completeLeasedExecutionTurn({ actor: worker, attempt: claimed.lease.attempt, executionId,
        fencingToken: claimed.lease.fencingToken, leaseOwner: claimed.lease.leaseOwner,
        reason: "audit completed", result: { ok: true }, tenantId: "default" });
    };

    const first = await admit("a".repeat(40));
    expect(first.executions[0]?.input).toMatchObject({
      text: expect.stringContaining(`"current":"${"a".repeat(40)}"`),
      context: { incremental: { checkpoint: "repository-audit", previous: null, current: "a".repeat(40), initial: true } },
    });
    await expect(complete(first.executions[0]!.id, "checkpoint-a")).resolves.toMatchObject({ executionState: "SUCCEEDED" });
    expect((await pool.query("select value from dispatch_execution_checkpoints where binding_id=$1", [bindingId])).rows).toEqual([{ value: "a".repeat(40) }]);
    await store.publishBindingVersion({ bindingId, createdAt: new Date().toISOString(), disabledAt: null, enabled: true,
      id: randomUUID(), profile: { id: profileId, version: 1 }, tenantId: "default", triggerId: "dispatcher-test",
      version: 2, definition });
    expect((await admit("a".repeat(40))).executions).toEqual([]);

    const failed = await admit("d".repeat(40));
    const failedClaim = await startRunningExecution(failed.executions[0]!.id, "checkpoint-failed");
    await expect(store.transitionLeasedExecution({ executionId: failed.executions[0]!.id, tenantId: "default",
      attempt: failedClaim.lease.attempt, fencingToken: failedClaim.lease.fencingToken,
      leaseOwner: failedClaim.lease.leaseOwner, expectedExecutionState: "RUNNING", expectedAttemptState: "RUNNING",
      targetExecutionState: "FAILED", targetAttemptState: "FAILED", actor: "checkpoint-failed", reason: "audit failed" }))
      .resolves.toMatchObject({ applied: true, executionState: "FAILED", attemptState: "FAILED" });
    expect((await pool.query("select value from dispatch_execution_checkpoints where binding_id=$1", [bindingId])).rows).toEqual([{ value: "a".repeat(40) }]);
    expect((await pool.query("select state from dispatch_execution_checkpoint_advances where execution_id=$1", [failed.executions[0]!.id])).rows)
      .toEqual([{ state: "PENDING" }]);

    const older = await admit("b".repeat(40));
    const olderClaim = await startRunningExecution(older.executions[0]!.id, "checkpoint-b");
    const newer = await admit("c".repeat(40));
    expect(newer.executions[0]?.input.context?.incremental).toEqual({
      checkpoint: "repository-audit", previous: "a".repeat(40), current: "c".repeat(40), initial: false,
    });
    await expect(complete(newer.executions[0]!.id, "checkpoint-c")).resolves.toMatchObject({ executionState: "SUCCEEDED" });
    await expect(store.completeLeasedExecutionTurn({ actor: "checkpoint-b", attempt: olderClaim.lease.attempt,
      executionId: older.executions[0]!.id, fencingToken: olderClaim.lease.fencingToken,
      leaseOwner: olderClaim.lease.leaseOwner, reason: "older audit completed", result: { ok: true }, tenantId: "default" }))
      .resolves.toMatchObject({ executionState: "SUCCEEDED" });

    expect((await pool.query("select value,advanced_by_execution_id from dispatch_execution_checkpoints where binding_id=$1", [bindingId])).rows)
      .toEqual([{ value: "c".repeat(40), advanced_by_execution_id: newer.executions[0]!.id }]);
    expect((await pool.query("select execution_id,state from dispatch_execution_checkpoint_advances where binding_id=$1 order by execution_id", [bindingId])).rows)
      .toEqual(expect.arrayContaining([
        { execution_id: first.executions[0]!.id, state: "APPLIED" },
        { execution_id: older.executions[0]!.id, state: "SUPERSEDED" },
        { execution_id: newer.executions[0]!.id, state: "APPLIED" },
      ]));
    expect((await admit("c".repeat(40))).executions).toEqual([]);
  });

  it("takes over an expired checkpointed running attempt without changing its identity or history", async () => {
    const executionId = await queueExecution();
    const original = await startRunningExecution(executionId, "dispatcher-stale", {
      workloadName: "execution-adoptable",
      opencodeSessionId: "session-adoptable",
    });
    await pool.query(
      "update dispatch_execution_attempts set lease_expires_at = now() - interval '1 second' where execution_id = $1",
      [executionId],
    );
    const before = await executionSnapshot(executionId);

    expect(await store.recoverExpiredExecutionLeases({ limit: 10, maxAttempts: 3, retryDelayMs: 0 })).toEqual([]);
    const adopted = await store.claimExpiredRunningExecution({ leaseOwner: "dispatcher-adopter", leaseDurationMs: 60_000 });

    expect(adopted).toMatchObject({
      adoption: { workloadName: "execution-adoptable", opencodeSessionId: "session-adoptable" },
      executionId,
      lease: { attempt: original.lease.attempt, leaseOwner: "dispatcher-adopter" },
      profileVersion: { profileId, version: 1 },
    });
    expect(adopted?.lease.fencingToken).not.toBe(original.lease.fencingToken);
    const persisted = await executionSnapshot(executionId);
    expect(persisted.execution.state).toBe("RUNNING");
    expect(persisted.attempts).toEqual([expect.objectContaining({
      attempt: original.lease.attempt,
      fencing_token: adopted?.lease.fencingToken,
      lease_owner: "dispatcher-adopter",
      opencode_session_id: "session-adoptable",
      state: "RUNNING",
      workload_name: "execution-adoptable",
    })]);
    expect(persisted.attempts[0]?.lease_expires_at.getTime()).toBeGreaterThan(Date.now());
    expect(persisted.transitions).toEqual(before.transitions);
  });

  it("does not take over live or non-checkpointed running attempts", async () => {
    const liveExecutionId = await queueExecution();
    const live = await startRunningExecution(liveExecutionId, "dispatcher-live", {
      workloadName: "execution-live",
      opencodeSessionId: "session-live",
    });
    expect(await store.renewExecutionLease({
      executionId: liveExecutionId,
      tenantId: "default",
      attempt: live.lease.attempt,
      fencingToken: live.lease.fencingToken,
      leaseOwner: live.lease.leaseOwner,
      leaseDurationMs: 120_000,
    })).toMatchObject({ status: "RENEWED", leaseExpiresAt: expect.any(Date) });
    const incompleteExecutionId = await queueExecution();
    await startRunningExecution(incompleteExecutionId, "dispatcher-incomplete");
    await pool.query(
      "update dispatch_execution_attempts set lease_expires_at = now() - interval '1 second' where execution_id = $1",
      [incompleteExecutionId],
    );
    const liveBefore = await executionSnapshot(liveExecutionId);
    const incompleteBefore = await executionSnapshot(incompleteExecutionId);

    expect(await store.claimExpiredRunningExecution({ leaseOwner: "dispatcher-adopter", leaseDurationMs: 60_000 }))
      .toBeUndefined();
    expect(await executionSnapshot(liveExecutionId)).toEqual(liveBefore);
    expect(await executionSnapshot(incompleteExecutionId)).toEqual(incompleteBefore);
    expect(await store.recoverExpiredExecutionLeases({ limit: 10, maxAttempts: 1, retryDelayMs: 0 }))
      .toEqual([expect.objectContaining({ executionId: incompleteExecutionId, executionState: "FAILED" })]);
  });

  it("allows only one dispatcher to take over an expired running attempt", async () => {
    const executionId = await queueExecution();
    const original = await startRunningExecution(executionId, "dispatcher-stale", {
      workloadName: "execution-contended",
      opencodeSessionId: "session-contended",
    });
    await pool.query(
      "update dispatch_execution_attempts set lease_expires_at = now() - interval '1 second' where execution_id = $1",
      [executionId],
    );

    const [first, second] = await Promise.all([
      store.claimExpiredRunningExecution({ leaseOwner: "dispatcher-a", leaseDurationMs: 60_000 }),
      store.claimExpiredRunningExecution({ leaseOwner: "dispatcher-b", leaseDurationMs: 60_000 }),
    ]);
    const claims = [first, second].filter((claim) => claim?.executionId === executionId);

    expect(claims).toHaveLength(1);
    expect(claims[0]).toMatchObject({
      adoption: { workloadName: "execution-contended", opencodeSessionId: "session-contended" },
      lease: { attempt: original.lease.attempt },
    });
    expect(claims[0]?.lease.fencingToken).not.toBe(original.lease.fencingToken);
    expect((await executionSnapshot(executionId)).attempts).toHaveLength(1);
  });

  it("fences a failed attempt into retry wait using database time", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-a", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution to be claimed");

    await expect(store.transitionLeasedExecution({
      executionId,
      tenantId: "default",
      attempt: claimed.lease.attempt,
      fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner,
      expectedExecutionState: "PROVISIONING",
      expectedAttemptState: "LEASED",
      targetExecutionState: "RETRY_WAIT",
      targetAttemptState: "FAILED",
      actor: "dispatcher-a",
      reason: "transient provisioning failure",
      result: { error: "unavailable" },
      retryDelayMs: 30_000,
    })).resolves.toEqual({ applied: true, executionState: "RETRY_WAIT", attemptState: "FAILED" });

    const persisted = await executionSnapshot(executionId);
    expect(persisted.execution.state).toBe("RETRY_WAIT");
    expect(persisted.execution.available_at.getTime()).toBeGreaterThan(Date.now() + 20_000);
    expect(persisted.attempts[0]).toMatchObject({ state: "FAILED", lease_owner: null, lease_expires_at: null });
  });

  it("authorizes leased timeout transitions with the database deadline", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-a", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution to be claimed");
    const timeoutCommand = {
      actor: "dispatcher-a",
      attempt: claimed.lease.attempt,
      executionId,
      expectedAttemptState: "LEASED" as const,
      expectedExecutionState: "PROVISIONING" as const,
      fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner,
      reason: "worker clock reports deadline elapsed",
      targetAttemptState: "TIMED_OUT" as const,
      targetExecutionState: "TIMED_OUT" as const,
      tenantId: "default",
    };
    const before = await executionSnapshot(executionId);

    await expect(store.transitionLeasedExecution(timeoutCommand)).resolves.toEqual({
      applied: false,
      reason: "DEADLINE_NOT_ELAPSED",
    });
    expect(await executionSnapshot(executionId)).toEqual(before);

    await pool.query(
      "update dispatch_executions set timeout_at = clock_timestamp() - interval '1 second' where id = $1",
      [executionId],
    );
    await expect(store.transitionLeasedExecution(timeoutCommand)).resolves.toEqual({
      applied: true,
      attemptState: "TIMED_OUT",
      executionState: "TIMED_OUT",
    });
  });

  it("rejects a stale fence without mutating execution history", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-a", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution to be claimed");
    const before = await executionSnapshot(executionId);

    await expect(store.transitionLeasedExecution({
      executionId,
      tenantId: "default",
      attempt: claimed.lease.attempt,
      fencingToken: "stale-token",
      leaseOwner: claimed.lease.leaseOwner,
      expectedExecutionState: "PROVISIONING",
      expectedAttemptState: "LEASED",
      targetExecutionState: "RUNNING",
      targetAttemptState: "RUNNING",
      actor: "stale-worker",
      reason: "late result",
    })).resolves.toEqual({ applied: false, reason: "LEASE_MISMATCH" });
    expect(await executionSnapshot(executionId)).toEqual(before);
  });

  it("recovers an expired lease and creates a newly fenced retry attempt", async () => {
    const executionId = await queueExecution();
    const first = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-stale", leaseDurationMs: 60_000 });
    if (!first) throw new Error("Expected first execution attempt");
    await pool.query(
      "update dispatch_execution_attempts set lease_expires_at = now() - interval '1 second' where execution_id = $1",
      [executionId],
    );

    expect(await store.recoverExpiredExecutionLeases({ limit: 10, maxAttempts: 3, retryDelayMs: 60_000 })).toEqual([
      expect.objectContaining({ attempt: 1, executionId, executionState: "RETRY_WAIT" }),
    ]);
    let persisted = await executionSnapshot(executionId);
    expect(persisted.execution).toMatchObject({ state: "RETRY_WAIT", completed_at: null });
    expect(persisted.attempts[0]).toMatchObject({
      attempt: 1,
      state: "FAILED",
      lease_owner: null,
      lease_expires_at: null,
    });
    expect(persisted.attempts[0]?.finished_at).toBeInstanceOf(Date);

    expect(await store.promoteDueExecutionRetries({ limit: 10 })).toEqual([]);
    await pool.query(
      "update dispatch_executions set available_at = now() - interval '1 second' where id = $1",
      [executionId],
    );
    expect(await store.promoteDueExecutionRetries({ limit: 10 })).toEqual([
      expect.objectContaining({ executionId }),
    ]);

    const second = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-current", leaseDurationMs: 60_000 });
    expect(second).toMatchObject({ executionId, lease: { attempt: 2, leaseOwner: "dispatcher-current" } });
    expect(second?.lease.fencingToken).not.toBe(first.lease.fencingToken);

    const beforeStaleWrite = await executionSnapshot(executionId);
    await expect(store.transitionLeasedExecution({
      executionId,
      tenantId: "default",
      attempt: 1,
      fencingToken: first.lease.fencingToken,
      leaseOwner: first.lease.leaseOwner,
      expectedExecutionState: "PROVISIONING",
      expectedAttemptState: "LEASED",
      targetExecutionState: "RUNNING",
      targetAttemptState: "RUNNING",
      actor: "dispatcher-stale",
      reason: "late attempt result",
    })).resolves.toEqual({ applied: false, reason: "LEASE_MISMATCH" });
    expect(await executionSnapshot(executionId)).toEqual(beforeStaleWrite);

    persisted = await executionSnapshot(executionId);
    expect(persisted.transitions.map((transition) => ({
      attempt: transition.attempt,
      from: transition.from_state,
      to: transition.to_state,
    }))).toEqual([
      { attempt: null, from: null, to: "RECEIVED" },
      { attempt: null, from: "RECEIVED", to: "PLANNED" },
      { attempt: null, from: "PLANNED", to: "QUEUED" },
      { attempt: 1, from: "QUEUED", to: "PROVISIONING" },
      { attempt: 1, from: "PROVISIONING", to: "RETRY_WAIT" },
      { attempt: null, from: "RETRY_WAIT", to: "QUEUED" },
      { attempt: 2, from: "QUEUED", to: "PROVISIONING" },
    ]);
  });

  it("fails an execution when its expired lease exhausts attempts", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-a", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution attempt");
    await pool.query(
      "update dispatch_execution_attempts set lease_expires_at = now() - interval '1 second' where execution_id = $1",
      [executionId],
    );

    expect(await store.recoverExpiredExecutionLeases({ limit: 10, maxAttempts: 1, retryDelayMs: 0 })).toEqual([
      expect.objectContaining({ attempt: 1, executionId, executionState: "FAILED" }),
    ]);
    const persisted = await executionSnapshot(executionId);
    expect(persisted.execution.state).toBe("FAILED");
    expect(persisted.execution.completed_at).toBeInstanceOf(Date);
    expect(persisted.attempts[0]).toMatchObject({ state: "FAILED", lease_owner: null, lease_expires_at: null });
    expect(persisted.transitions.at(-1)).toMatchObject({
      attempt: 1,
      from_state: "PROVISIONING",
      to_state: "FAILED",
    });
    expect(await store.promoteDueExecutionRetries({ limit: 10 })).toEqual([]);
  });

  it("does not recover a renewed unexpired lease", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-a", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution attempt");
    expect(await store.renewExecutionLease({
      executionId,
      tenantId: "default",
      attempt: claimed.lease.attempt,
      fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner,
      leaseDurationMs: 120_000,
    })).toMatchObject({ status: "RENEWED", leaseExpiresAt: expect.any(Date) });
    const before = await executionSnapshot(executionId);

    expect(await store.recoverExpiredExecutionLeases({ limit: 10, maxAttempts: 3, retryDelayMs: 0 })).toEqual([]);
    expect(await executionSnapshot(executionId)).toEqual(before);
  });

  it("times out a retry whose execution deadline elapsed while waiting", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-a", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution attempt");
    await pool.query(
      "update dispatch_execution_attempts set lease_expires_at = now() - interval '1 second' where execution_id = $1",
      [executionId],
    );
    await store.recoverExpiredExecutionLeases({ limit: 10, maxAttempts: 3, retryDelayMs: 60_000 });
    await pool.query(
      "update dispatch_executions set timeout_at = now() - interval '1 second' where id = $1",
      [executionId],
    );

    expect(await store.promoteDueExecutionRetries({ limit: 10 })).toEqual([
      expect.objectContaining({ executionId, executionState: "TIMED_OUT" }),
    ]);
    const persisted = await executionSnapshot(executionId);
    expect(persisted.execution.state).toBe("TIMED_OUT");
    expect(persisted.execution.completed_at).toBeInstanceOf(Date);
    expect(persisted.transitions.at(-1)).toMatchObject({
      attempt: null,
      from_state: "RETRY_WAIT",
      to_state: "TIMED_OUT",
    });
  });

  it("times out a queued execution whose deadline elapsed before it was claimed", async () => {
    const executionId = await queueExecution();
    await pool.query(
      "update dispatch_executions set timeout_at = now() - interval '1 second' where id = $1",
      [executionId],
    );

    expect(await store.claimNextQueuedExecution({ leaseOwner: "dispatcher", leaseDurationMs: 60_000 })).toBeUndefined();
    expect(await store.promoteDueExecutionRetries({ limit: 10 })).toEqual([
      expect.objectContaining({ executionId, executionState: "TIMED_OUT" }),
    ]);

    const persisted = await executionSnapshot(executionId);
    expect(persisted.execution).toMatchObject({ state: "TIMED_OUT" });
    expect(persisted.execution.completed_at).toBeInstanceOf(Date);
    expect(persisted.transitions.at(-1)).toMatchObject({
      attempt: null,
      from_state: "QUEUED",
      to_state: "TIMED_OUT",
    });
  });

  it("immediately and idempotently cancels queued executions with ordered history", async () => {
    const executionId = await queueExecution();
    const requestedAt = new Date().toISOString();
    const command = {
      actor: "test-user",
      executionId,
      reason: "no longer needed",
      requestedAt,
      tenantId: "default",
      transitionId: randomUUID(),
    };

    await expect(store.requestExecutionCancellation(command)).resolves.toEqual({
      id: executionId, outcome: "CANCELLED", state: "CANCELLED",
    });
    await expect(store.requestExecutionCancellation({ ...command, transitionId: randomUUID() })).resolves.toEqual({
      id: executionId, outcome: "CANCELLED", state: "CANCELLED",
    });

    const detail = await store.getExecutionDetail("default", executionId);
    expect(detail?.attempts).toEqual([]);
    expect(detail?.transitions.map(({ sequence, fromState, toState }) => ({ sequence, fromState, toState }))).toEqual([
      { sequence: 1, fromState: null, toState: "RECEIVED" },
      { sequence: 2, fromState: "RECEIVED", toState: "PLANNED" },
      { sequence: 3, fromState: "PLANNED", toState: "QUEUED" },
      { sequence: 4, fromState: "QUEUED", toState: "CANCEL_REQUESTED" },
      { sequence: 5, fromState: "CANCEL_REQUESTED", toState: "CANCELLED" },
    ]);
    expect((await pool.query("select completed_at from dispatch_executions where id = $1", [executionId])).rows[0]?.completed_at)
      .toBeInstanceOf(Date);
  });

  it("immediately cancels an execution awaiting approval without creating an attempt", async () => {
    const executionId = await queueExecution();
    await pool.query("update dispatch_executions set state = 'AWAITING_APPROVAL' where id = $1", [executionId]);

    await expect(store.requestExecutionCancellation({
      actor: "test-user", executionId, reason: "approval withdrawn", requestedAt: new Date().toISOString(),
      tenantId: "default", transitionId: randomUUID(),
    })).resolves.toEqual({ id: executionId, outcome: "CANCELLED", state: "CANCELLED" });

    const persisted = await executionSnapshot(executionId);
    expect(persisted.execution.state).toBe("CANCELLED");
    expect(persisted.attempts).toEqual([]);
    expect(persisted.transitions.slice(-2)).toMatchObject([
      { attempt: null, from_state: "AWAITING_APPROVAL", to_state: "CANCEL_REQUESTED" },
      { attempt: null, from_state: "CANCEL_REQUESTED", to_state: "CANCELLED" },
    ]);
  });

  it("rejects cancellation from a non-cancellable state", async () => {
    const executionId = await queueExecution();
    await pool.query("update dispatch_executions set state = 'SUCCEEDED' where id = $1", [executionId]);

    await expect(store.requestExecutionCancellation({
      actor: "test-user", executionId, reason: "too late", requestedAt: new Date().toISOString(),
      tenantId: "default", transitionId: randomUUID(),
    })).rejects.toBeInstanceOf(ExecutionCancellationConflictError);
    expect((await pool.query("select state from dispatch_executions where id = $1", [executionId])).rows[0])
      .toEqual({ state: "SUCCEEDED" });
  });

  it("serializes concurrent cancellation requests into one transition pair", async () => {
    const executionId = await queueExecution();
    const request = (actor: string) => store.requestExecutionCancellation({
      actor, executionId, reason: "concurrent request", requestedAt: new Date().toISOString(),
      tenantId: "default", transitionId: randomUUID(),
    });
    const results = await Promise.all([request("first"), request("second")]);

    expect(results).toEqual([
      { id: executionId, outcome: "CANCELLED", state: "CANCELLED" },
      { id: executionId, outcome: "CANCELLED", state: "CANCELLED" },
    ]);
    expect((await pool.query(
      "select count(*)::int as count from dispatch_execution_transitions where execution_id = $1 and to_state in ('CANCEL_REQUESTED', 'CANCELLED')",
      [executionId],
    )).rows[0]).toEqual({ count: 2 });
  });

  it("reports active cancellation through renewal and accepts only the live fence", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-cancel", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution attempt");
    const cancellation = await store.requestExecutionCancellation({
      actor: "test-user", executionId, reason: "stop", requestedAt: new Date().toISOString(),
      tenantId: "default", transitionId: randomUUID(),
    });
    expect(cancellation).toEqual({ id: executionId, outcome: "REQUESTED", state: "CANCEL_REQUESTED" });
    const leaseCommand = {
      attempt: claimed.lease.attempt, executionId, fencingToken: claimed.lease.fencingToken,
      leaseDurationMs: 60_000, leaseOwner: claimed.lease.leaseOwner, tenantId: "default",
    };
    await expect(store.renewExecutionLease(leaseCommand)).resolves.toBe("CANCEL_REQUESTED");
    await expect(store.renewExecutionLease({ ...leaseCommand, fencingToken: "stale" })).resolves.toBe("LOST");
    await expect(store.acknowledgeLeasedExecutionCancellation({
      ...leaseCommand, fencingToken: "stale", actor: "stale-dispatcher", reason: "late acknowledgement",
    })).resolves.toEqual({ applied: false, reason: "LEASE_MISMATCH" });
    await expect(store.acknowledgeLeasedExecutionCancellation({
      ...leaseCommand, actor: "dispatcher-cancel", reason: "stopped",
    })).resolves.toEqual({ applied: true });

    const persisted = await executionSnapshot(executionId);
    expect(persisted.execution).toMatchObject({ state: "CANCELLED" });
    expect(persisted.execution.completed_at).toBeInstanceOf(Date);
    expect(persisted.attempts[0]).toMatchObject({ state: "CANCELLED", lease_owner: null, lease_expires_at: null });
    const detail = await store.getExecutionDetail("default", executionId);
    expect(detail?.attempts[0]).not.toHaveProperty("fencingToken");
    expect(detail?.attempts[0]).not.toHaveProperty("leaseOwner");
  });

  it("lists expired cancellation cleanup without mutating it", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-expiring", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution attempt");
    await store.requestExecutionCancellation({
      actor: "test-user", executionId, reason: "stop", requestedAt: new Date().toISOString(),
      tenantId: "default", transitionId: randomUUID(),
    });

    expect(await store.listRequestedCancellationCleanups({ limit: 10 })).toEqual([]);
    await pool.query("update dispatch_execution_attempts set lease_expires_at = now() - interval '1 second' where execution_id = $1", [executionId]);
    const candidates = await store.listRequestedCancellationCleanups({ limit: 10 });
    const candidate = candidates.find((item) => item.executionId === executionId);
    expect(candidate).toEqual({ attempt: 1, executionId, tenantId: "default", workloadName: null });
    expect(await executionSnapshot(executionId)).toMatchObject({
      execution: { state: "CANCEL_REQUESTED" },
      attempts: [{ state: "LEASED" }],
    });
    if (!candidate) throw new Error("Expected cancellation cleanup candidate");
    await store.finalizeRequestedExecutionCancellation(candidate);
  });

  it("rotates expired cancellation cleanups after a failed cleanup without mutating state or history", async () => {
    const oldestExecutionId = await queueExecution();
    const oldestClaim = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-rotation-oldest", leaseDurationMs: 60_000 });
    if (!oldestClaim) throw new Error("Expected oldest execution attempt");
    await store.requestExecutionCancellation({
      actor: "test-user", executionId: oldestExecutionId, reason: "stop", requestedAt: new Date().toISOString(),
      tenantId: "default", transitionId: randomUUID(),
    });

    const otherExecutionId = await queueExecution();
    const otherClaim = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-rotation-other", leaseDurationMs: 60_000 });
    if (!otherClaim) throw new Error("Expected other execution attempt");
    await store.requestExecutionCancellation({
      actor: "test-user", executionId: otherExecutionId, reason: "stop", requestedAt: new Date().toISOString(),
      tenantId: "default", transitionId: randomUUID(),
    });

    await pool.query(
      `UPDATE dispatch_execution_attempts
       SET lease_expires_at = now() - interval '1 second'
       WHERE execution_id = ANY($1::text[])`,
      [[oldestExecutionId, otherExecutionId]],
    );
    await pool.query(
      `UPDATE dispatch_executions
       SET updated_at = CASE id WHEN $1 THEN '2000-01-01T00:00:00Z'::timestamptz
                                    ELSE '2000-01-02T00:00:00Z'::timestamptz END
       WHERE id = ANY($2::text[])`,
      [oldestExecutionId, [oldestExecutionId, otherExecutionId]],
    );
    const before = {
      oldest: await executionSnapshot(oldestExecutionId),
      other: await executionSnapshot(otherExecutionId),
    };
    const beforeUpdatedAt = await cancellationCleanupUpdatedAt(oldestExecutionId, otherExecutionId);

    await expect(store.listRequestedCancellationCleanups({ limit: 1 })).resolves.toEqual([{
      attempt: oldestClaim.lease.attempt,
      executionId: oldestExecutionId,
      tenantId: "default",
      workloadName: null,
    }]);
    const afterFirstUpdatedAt = await cancellationCleanupUpdatedAt(oldestExecutionId, otherExecutionId);
    expect(afterFirstUpdatedAt[oldestExecutionId]!.getTime()).toBeGreaterThan(beforeUpdatedAt[oldestExecutionId]!.getTime());
    expect(afterFirstUpdatedAt[otherExecutionId]).toEqual(beforeUpdatedAt[otherExecutionId]);

    await expect(store.listRequestedCancellationCleanups({ limit: 1 })).resolves.toEqual([{
      attempt: otherClaim.lease.attempt,
      executionId: otherExecutionId,
      tenantId: "default",
      workloadName: null,
    }]);
    const afterSecondUpdatedAt = await cancellationCleanupUpdatedAt(oldestExecutionId, otherExecutionId);
    expect(afterSecondUpdatedAt[oldestExecutionId]).toEqual(afterFirstUpdatedAt[oldestExecutionId]);
    expect(afterSecondUpdatedAt[otherExecutionId]!.getTime()).toBeGreaterThan(afterFirstUpdatedAt[otherExecutionId]!.getTime());
    expect({
      oldest: await executionSnapshot(oldestExecutionId),
      other: await executionSnapshot(otherExecutionId),
    }).toEqual(before);
  });

  it("finalizes an expired cancellation after cleanup", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-cleanup", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution attempt");
    await pool.query("update dispatch_execution_attempts set workload_name = $2 where execution_id = $1", [executionId, "sandbox-cleanup"]);
    await store.requestExecutionCancellation({
      actor: "test-user", executionId, reason: "stop", requestedAt: new Date().toISOString(),
      tenantId: "default", transitionId: randomUUID(),
    });
    await pool.query("update dispatch_execution_attempts set lease_expires_at = now() - interval '1 second' where execution_id = $1", [executionId]);

    const candidate = (await store.listRequestedCancellationCleanups({ limit: 10 }))
      .find((item) => item.executionId === executionId);
    expect(candidate).toEqual({ attempt: 1, executionId, tenantId: "default", workloadName: "sandbox-cleanup" });
    if (!candidate) throw new Error("Expected cancellation cleanup candidate");
    await expect(store.finalizeRequestedExecutionCancellation(candidate)).resolves.toEqual({
      ...candidate,
      finalizedAt: expect.any(Date),
    });
    expect((await executionSnapshot(executionId))).toMatchObject({
      execution: { state: "CANCELLED", completed_at: expect.any(Date) },
      attempts: [{ state: "CANCELLED", lease_owner: null, lease_expires_at: null }],
    });
  });

  it("rejects a stale cancellation cleanup candidate after the fence changes", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-stale", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution attempt");
    await store.requestExecutionCancellation({
      actor: "test-user", executionId, reason: "stop", requestedAt: new Date().toISOString(),
      tenantId: "default", transitionId: randomUUID(),
    });
    await pool.query("update dispatch_execution_attempts set lease_expires_at = now() - interval '1 second' where execution_id = $1", [executionId]);
    const candidate = (await store.listRequestedCancellationCleanups({ limit: 10 }))
      .find((item) => item.executionId === executionId);
    if (!candidate) throw new Error("Expected cancellation cleanup candidate");
    await pool.query("update dispatch_execution_attempts set fencing_token = $2 where execution_id = $1", [executionId, randomUUID()]);

    await expect(store.finalizeRequestedExecutionCancellation(candidate)).resolves.toBeUndefined();
    expect(await executionSnapshot(executionId)).toMatchObject({
      execution: { state: "CANCEL_REQUESTED" },
      attempts: [{ state: "LEASED" }],
    });
    const currentCandidate = (await store.listRequestedCancellationCleanups({ limit: 10 }))
      .find((item) => item.executionId === executionId);
    if (!currentCandidate) throw new Error("Expected current cancellation cleanup candidate");
    await store.finalizeRequestedExecutionCancellation(currentCandidate);
  });

  it("skips cancellation cleanup while the active lease is live", async () => {
    const executionId = await queueExecution();
    const claimed = await store.claimNextQueuedExecution({ leaseOwner: "dispatcher-live", leaseDurationMs: 60_000 });
    if (!claimed) throw new Error("Expected execution attempt");
    await store.requestExecutionCancellation({
      actor: "test-user", executionId, reason: "stop", requestedAt: new Date().toISOString(),
      tenantId: "default", transitionId: randomUUID(),
    });

    expect((await store.listRequestedCancellationCleanups({ limit: 10 }))
      .some((candidate) => candidate.executionId === executionId)).toBe(false);
    expect((await executionSnapshot(executionId)).execution.state).toBe("CANCEL_REQUESTED");
    await store.acknowledgeLeasedExecutionCancellation({
      actor: "dispatcher-live", attempt: claimed.lease.attempt, executionId,
      fencingToken: claimed.lease.fencingToken, leaseOwner: claimed.lease.leaseOwner,
      reason: "test cleanup", tenantId: "default",
    });
  });

  it("finalizes requested cancellation with no active attempt", async () => {
    const executionId = await queueExecution();
    await pool.query("update dispatch_executions set state = 'CANCEL_REQUESTED' where id = $1", [executionId]);

    const candidate = (await store.listRequestedCancellationCleanups({ limit: 10 }))
      .find((item) => item.executionId === executionId);
    expect(candidate).toEqual({ attempt: null, executionId, tenantId: "default", workloadName: null });
    if (!candidate) throw new Error("Expected cancellation cleanup candidate");
    await expect(store.finalizeRequestedExecutionCancellation(candidate)).resolves.toEqual({
      ...candidate,
      finalizedAt: expect.any(Date),
    });
    expect(await executionSnapshot(executionId)).toMatchObject({
      execution: { state: "CANCELLED", completed_at: expect.any(Date) },
      attempts: [],
    });
  });

  it("activates, exposes, and immediately cancels a policy-driven wait", async () => {
    const bindingId = `waiting-${randomUUID()}`;
    const eventType = `dev.dispatch.wait.${randomUUID()}`;
    const createdAt = new Date().toISOString();
    await store.publishBindingVersion({
      bindingId, createdAt, disabledAt: null, enabled: true, id: randomUUID(), profile: { id: profileId, version: 1 },
      tenantId: "default", triggerId: "dispatcher-test", version: 1,
      definition: {
        schemaVersion: 1, eventTypes: [eventType], filter: { all: [] },
        prompt: { includeEvent: "none", literal: "Run and wait" }, workspace: { type: "empty" },
        afterTurn: {
          disposition: "wait",
          wait: {
            name: "work-item-lifecycle",
            correlation: [{ name: "repositoryId", path: "/repository/id" }, { name: "workItem", path: "/issue/number" }],
            deadlineSeconds: 600,
          },
        },
      },
    });
    const persistedBinding = await store.getBindingVersion("default", bindingId, 1);
    if (!persistedBinding || "disposition" in persistedBinding.definition) throw new Error("Expected create binding");
    expect(persistedBinding.definition.afterTurn).toEqual({
      disposition: "wait",
      wait: {
        name: "work-item-lifecycle",
        correlation: [{ name: "repositoryId", path: "/repository/id" }, { name: "workItem", path: "/issue/number" }],
        deadlineSeconds: 600,
      },
    });
    const event = {
      data: { repository: { id: 123 }, issue: { number: 42 } }, datacontenttype: "application/json",
      id: randomUUID(), source: "/test/wait", specversion: "1.0" as const, type: eventType,
    };
    const admitted = await store.admitEvent({
      admittedAt: createdAt, admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId: "dispatcher-test", event }),
      event, internalEventId: randomUUID(), sourceDeduplicationKey: randomUUID(), tenantId: "default", triggerId: "dispatcher-test",
    });
    const executionId = admitted.executions[0]!.id;
    const claimed = await startRunningExecution(executionId, "wait-worker", { workloadName: "wait-workload", opencodeSessionId: "wait-session" });
    const completion = await store.completeLeasedExecutionTurn({
      actor: "wait-worker", attempt: claimed.lease.attempt, executionId, fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner, reason: "turn completed", result: { output: "PR opened" }, tenantId: "default",
    });
    expect(completion).toMatchObject({ applied: true, attemptState: "SUCCEEDED", executionState: "WAITING", eventWaitId: expect.any(String) });
    const waitingDetail = await store.getExecutionDetail("default", executionId);
    expect(waitingDetail).toMatchObject({
      state: "WAITING",
      attempts: [{ attempt: 1, state: "SUCCEEDED", leaseExpiresAt: null }],
      waits: [{
        attempt: 1, name: "work-item-lifecycle", state: "ACTIVE",
        correlation: { repositoryId: 123, workItem: 42 }, endedAt: null,
      }],
    });
    expect(waitingDetail?.transitions).toEqual(expect.arrayContaining([expect.objectContaining({ fromState: "RUNNING", toState: "WAITING", attempt: 1 })]));

    await expect(store.requestExecutionCancellation({
      actor: "test", executionId, reason: "stop waiting", requestedAt: "2020-01-01T00:00:00.000Z", tenantId: "default", transitionId: randomUUID(),
    })).resolves.toMatchObject({ outcome: "CANCELLED", state: "CANCELLED" });
    expect(await store.getExecutionDetail("default", executionId)).toMatchObject({
      state: "CANCELLED", waits: [{ state: "CANCELLED", endedAt: expect.any(String) }],
    });
  });

  it("expires due waits and rejects stale turn completion fences", async () => {
    const executionId = await queueExecution();
    const claimed = await startRunningExecution(executionId, "ordinary-worker");
    await expect(store.completeLeasedExecutionTurn({
      actor: "stale", attempt: claimed.lease.attempt, executionId, fencingToken: "wrong",
      leaseOwner: claimed.lease.leaseOwner, reason: "turn completed", result: null, tenantId: "default",
    })).resolves.toEqual({ applied: false, reason: "LEASE_MISMATCH" });
    await expect(store.completeLeasedExecutionTurn({
      actor: "ordinary-worker", attempt: claimed.lease.attempt, executionId, fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner, reason: "turn completed", result: { ok: true }, tenantId: "default",
    })).resolves.toEqual({ applied: true, attemptState: "SUCCEEDED", executionState: "SUCCEEDED" });
    expect((await store.getExecutionDetail("default", executionId))?.waits).toEqual([]);

    const waiting = await createWaitingExecution(store, profileId);
    await pool.query("UPDATE dispatch_event_waits SET activated_at = clock_timestamp() - interval '2 seconds', deadline_at = clock_timestamp() - interval '1 second' WHERE execution_id = $1", [waiting]);
    const expired = await store.expireDueEventWaits({ limit: 10 });
    expect(expired).toEqual([expect.objectContaining({ executionId: waiting, eventWaitId: expect.any(String) })]);
    const expiredDetail = await store.getExecutionDetail("default", waiting);
    expect(expiredDetail).toMatchObject({ state: "TIMED_OUT", waits: [{ state: "EXPIRED", endedAt: expect.any(String) }] });
    expect(expiredDetail?.transitions).toEqual(expect.arrayContaining([expect.objectContaining({ fromState: "WAITING", toState: "TIMED_OUT", attempt: null })]));
  });

  it("marks the attempt timed out when completion observes an elapsed execution deadline", async () => {
    const executionId = await queueExecution();
    const claimed = await startRunningExecution(executionId, "deadline-worker");

    await pool.query("UPDATE dispatch_executions SET timeout_at = clock_timestamp() WHERE id = $1", [executionId]);

    const completion = await store.completeLeasedExecutionTurn({
      actor: "deadline-worker", attempt: claimed.lease.attempt, executionId, fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner, reason: "turn completed", result: { output: "late" }, tenantId: "default",
    });

    expect(completion).toMatchObject({ applied: true, executionState: "TIMED_OUT", attemptState: "TIMED_OUT" });
    expect((await executionSnapshot(executionId)).attempts).toMatchObject([
      { attempt: claimed.lease.attempt, state: "TIMED_OUT" },
    ]);
  });

  it("rolls back turn completion when configured correlation is not a bounded primitive", async () => {
    const token = randomUUID();
    const eventType = `dev.dispatch.invalid-wait.${token}`;
    const createdAt = new Date().toISOString();
    await store.publishBindingVersion({
      bindingId: `invalid-wait-${token}`, createdAt, disabledAt: null, enabled: true, id: randomUUID(),
      profile: { id: profileId, version: 1 }, tenantId: "default", triggerId: "dispatcher-test", version: 1,
      definition: {
        schemaVersion: 1, eventTypes: [eventType], filter: { all: [] }, prompt: { includeEvent: "none", literal: "Wait" },
        workspace: { type: "empty" },
        afterTurn: { disposition: "wait", wait: { name: "invalid", correlation: [{ name: "key", path: "/nested" }], deadlineSeconds: 600 } },
      },
    });
    const event = { data: { nested: { value: token } }, datacontenttype: "application/json", id: token, source: "/test/wait", specversion: "1.0" as const, type: eventType };
    const admitted = await store.admitEvent({
      admittedAt: createdAt, admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId: "dispatcher-test", event }),
      event, internalEventId: randomUUID(), sourceDeduplicationKey: token, tenantId: "default", triggerId: "dispatcher-test",
    });
    const executionId = admitted.executions[0]!.id;
    const claimed = await startRunningExecution(executionId, `worker-${token}`);
    await expect(store.completeLeasedExecutionTurn({
      actor: `worker-${token}`, attempt: claimed.lease.attempt, executionId, fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner, reason: "turn completed", result: null, tenantId: "default",
    })).rejects.toBeInstanceOf(PersistedExecutionCorruptionError);
    expect(await store.getExecutionDetail("default", executionId)).toMatchObject({
      state: "RUNNING", attempts: [{ state: "RUNNING" }], waits: [],
    });
  });

  async function queueExecution(): Promise<string> {
    const createdAt = new Date().toISOString();
    const sequence = ++eventSequence;
    const event = {
      data: { sequence },
      datacontenttype: "application/json",
      id: `event-${sequence}`,
      source: "/test/dispatcher",
      specversion: "1.0" as const,
      time: createdAt,
      type: "dev.dispatch.execution.submitted",
    };
    const result = await store.admitEvent({
      admittedAt: createdAt,
      admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId: "dispatcher-test", event }),
      event,
      internalEventId: randomUUID(),
      sourceDeduplicationKey: `delivery-${sequence}`,
      tenantId: "default",
      triggerId: "dispatcher-test",
    });
    const execution = result.executions[0];
    if (!execution) throw new Error("Expected event to queue an execution");
    return execution.id;
  }

  async function createWaitingExecution(store: PostgresRuntimeStore, profileId: string): Promise<string> {
    const token = randomUUID();
    const eventType = `dev.dispatch.expiring-wait.${token}`;
    const createdAt = new Date().toISOString();
    await store.publishBindingVersion({
      bindingId: `expiring-${token}`, createdAt, disabledAt: null, enabled: true, id: randomUUID(),
      profile: { id: profileId, version: 1 }, tenantId: "default", triggerId: "dispatcher-test", version: 1,
      definition: {
        schemaVersion: 1, eventTypes: [eventType], filter: { all: [] }, prompt: { includeEvent: "none", literal: "Wait" },
        workspace: { type: "empty" },
        afterTurn: { disposition: "wait", wait: { name: "expiring", correlation: [{ name: "key", path: "/key" }], deadlineSeconds: 600 } },
      },
    });
    const event = { data: { key: token }, datacontenttype: "application/json", id: token, source: "/test/wait", specversion: "1.0" as const, type: eventType };
    const admitted = await store.admitEvent({
      admittedAt: createdAt, admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId: "dispatcher-test", event }),
      event, internalEventId: randomUUID(), sourceDeduplicationKey: token, tenantId: "default", triggerId: "dispatcher-test",
    });
    const executionId = admitted.executions[0]!.id;
    const claimed = await startRunningExecution(executionId, `worker-${token}`);
    const completion = await store.completeLeasedExecutionTurn({
      actor: `worker-${token}`, attempt: claimed.lease.attempt, executionId, fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner, reason: "turn completed", result: null, tenantId: "default",
    });
    if (!completion.applied || completion.executionState !== "WAITING") throw new Error("Expected waiting execution");
    return executionId;
  }

  async function startRunningExecution(
    executionId: string,
    leaseOwner: string,
    checkpoint?: { workloadName: string; opencodeSessionId: string },
  ) {
    const claimed = await store.claimNextQueuedExecution({ leaseOwner, leaseDurationMs: 60_000 });
    if (!claimed || claimed.executionId !== executionId) throw new Error("Expected execution attempt");
    await expect(store.transitionLeasedExecution({
      executionId,
      tenantId: "default",
      attempt: claimed.lease.attempt,
      fencingToken: claimed.lease.fencingToken,
      leaseOwner: claimed.lease.leaseOwner,
      expectedExecutionState: "PROVISIONING",
      expectedAttemptState: "LEASED",
      targetExecutionState: "RUNNING",
      targetAttemptState: "RUNNING",
      actor: leaseOwner,
      reason: "sandbox ready",
      ...checkpoint,
    })).resolves.toEqual({ applied: true, executionState: "RUNNING", attemptState: "RUNNING" });
    return claimed;
  }

  async function executionSnapshot(executionId: string) {
    const execution = (await pool.query(
      "select state, result, completed_at, available_at from dispatch_executions where id = $1",
      [executionId],
    )).rows[0];
    const attempts = (await pool.query(
      "select attempt, state, fencing_token, lease_owner, lease_expires_at, started_at, finished_at, workload_name, opencode_session_id from dispatch_execution_attempts where execution_id = $1 order by attempt",
      [executionId],
    )).rows;
    const transitions = (await pool.query(
      "select sequence, attempt, from_state, to_state from dispatch_execution_transitions where execution_id = $1 order by sequence",
      [executionId],
    )).rows;
    return { attempts, execution, transitions };
  }

  async function cancellationCleanupUpdatedAt(...executionIds: string[]): Promise<Record<string, Date>> {
    const rows = (await pool.query<{ id: string; updated_at: Date }>(
      "select id, updated_at from dispatch_executions where id = any($1::text[])",
      [executionIds],
    )).rows;
    return Object.fromEntries(rows.map((row) => [row.id, row.updated_at]));
  }
});

async function startPostgres(): Promise<StartedTestContainer> {
  return new GenericContainer("postgres:16-alpine")
    .withEnvironment({
      POSTGRES_DB: "dispatch",
      POSTGRES_PASSWORD: "dispatch-password",
      POSTGRES_USER: "dispatch",
    })
    .withExposedPorts(5432)
    .withHealthCheck({
      interval: 1_000,
      retries: 30,
      test: ["CMD-SHELL", "pg_isready -U dispatch -d dispatch"],
      timeout: 5_000,
    })
    .withWaitStrategy(Wait.forHealthCheck())
    .start();
}

function postgresConnectionString(container: StartedTestContainer): string {
  return `postgresql://dispatch:dispatch-password@${container.getHost()}:${container.getMappedPort(5432)}/dispatch`;
}
