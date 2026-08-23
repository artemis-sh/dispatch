import { randomUUID } from "node:crypto";
import pg from "pg";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createPostgresRuntimeStore, type PostgresRuntimeStore } from "../../src/runtime/postgres.js";
import { ScheduleWorker } from "../../src/schedule/worker.js";
import { nextCronOccurrence } from "../../src/schedule/cron.js";

describe("schedule persistence", () => {
  let postgres: StartedTestContainer;
  let store: PostgresRuntimeStore;

  beforeAll(async () => {
    postgres = await new GenericContainer("postgres:16-alpine")
      .withEnvironment({ POSTGRES_DB: "dispatch", POSTGRES_PASSWORD: "dispatch-password", POSTGRES_USER: "dispatch" })
      .withExposedPorts(5432)
      .withHealthCheck({ interval: 1_000, retries: 30, test: ["CMD-SHELL", "pg_isready -U dispatch -d dispatch"], timeout: 5_000 })
      .withWaitStrategy(Wait.forHealthCheck()).start();
    store = await createPostgresRuntimeStore({
      connectionString: `postgresql://dispatch:dispatch-password@${postgres.getHost()}:${postgres.getMappedPort(5432)}/dispatch`,
      runMigrations: true, ssl: false, sslRejectUnauthorized: false,
    });
  });

  afterAll(async () => { await store?.close(); await postgres?.stop(); });

  it("materializes one durable occurrence and gates execution on exact revision resolution", async () => {
    const now = new Date();
    const createdAt = new Date(now.getTime() - 60_000).toISOString();
    const triggerId = `schedule-${randomUUID()}`;
    await store.createTrigger({ id: triggerId, tenantId: "default", type: "schedule.cron", enabled: true,
      createdAt, disabledAt: null, config: { schemaVersion: 1, expression: "* * * * *", timezone: "UTC", misfirePolicy: "skip",
        repository: { installationId: 44, id: 10, fullName: "acme/widgets", defaultBranch: "main" } } });
    await store.publishProfileVersion({ id: randomUUID(), tenantId: "default", profileId: "bug-finder", version: 1, createdAt,
      definition: { schemaVersion: 1, runtime: { type: "opencode", agent: "audit", opencodeConfig: { agent: { audit: {} } } },
        sandbox: { templateName: "opencode", warmPool: "none" }, connections: [], permissions: { onRequest: "fail" }, timeoutSeconds: 1800 } });
    await store.publishBindingVersion({ id: randomUUID(), bindingId: "hourly-bug-finder", version: 1, tenantId: "default", triggerId,
      profile: { id: "bug-finder", version: 1 }, enabled: true, createdAt, disabledAt: null,
      definition: { schemaVersion: 1, eventTypes: ["dev.dispatch.schedule.triggered"], filter: { all: [] },
        activeSingleton: { name: "scheduled-bug-finder", key: ["/repository/id"] },
        prompt: { literal: "Audit", includeEvent: "data" }, workspace: { type: "git",
          repository: { url: { path: "/repository/cloneUrl" } }, revision: { commit: { path: "/repository/defaultBranchRevision/commit" } } } } });

    expect(await store.materializeDueScheduleOccurrences({ now: now.toISOString(), limit: 100 })).toBe(1);
    expect(await store.materializeDueScheduleOccurrences({ now: now.toISOString(), limit: 100 })).toBe(0);
    const worker = new ScheduleWorker({ store, workerId: "scheduler-1", leaseDurationMs: 60_000, idlePollMs: 10,
      retryDelayMs: 30_000, maxAttempts: 5, materializeBatchSize: 100 });
    expect(await worker.runOne()).toBe(true);
    const resolution = await store.claimRevisionResolution({ leaseOwner: "resolver-1", leaseDurationMs: 60_000 });
    expect(resolution).toMatchObject({ installationId: 44, repositoryId: 10, repositoryFullName: "acme/widgets", branch: "main" });
    const completed = await store.completeRevisionResolution({ eventId: resolution!.eventId, tenantId: "default",
      leaseOwner: resolution!.leaseOwner, leaseToken: resolution!.leaseToken, commit: "a".repeat(40), resolvedAt: new Date().toISOString() });
    expect(completed?.executions).toHaveLength(1);
    expect(completed?.executions[0]?.workspace).toMatchObject({ revision: { type: "commit", commit: "a".repeat(40) } });
  });

  it("skips missed occurrences and materializes the next on-time occurrence", async () => {
    const recovery = new Date();
    const triggerId = `skip-misfire-${randomUUID()}`;
    await store.createTrigger({ id: triggerId, tenantId: "default", type: "schedule.cron", enabled: true,
      createdAt: new Date(recovery.getTime() - 2 * 60_000).toISOString(), disabledAt: null,
      config: { schemaVersion: 1, expression: "* * * * *", timezone: "UTC", misfirePolicy: "skip",
        repository: { installationId: 44, id: 10, fullName: "acme/widgets", defaultBranch: "main" } } });

    expect(await store.materializeDueScheduleOccurrences({ now: recovery.toISOString(), limit: 100 })).toBe(0);
    expect(await store.claimScheduleOccurrence({ leaseOwner: "scheduler", leaseDurationMs: 60_000 })).toBeUndefined();

    const nextOccurrence = nextCronOccurrence("* * * * *", recovery);
    expect(await store.materializeDueScheduleOccurrences({ now: nextOccurrence.toISOString(), limit: 100 })).toBe(1);
  });

  it("does not lease a materialized occurrence after its trigger is disabled", async () => {
    const now = new Date();
    const triggerId = `disabled-${randomUUID()}`;
    await store.createTrigger({ id: triggerId, tenantId: "default", type: "schedule.cron", enabled: true,
      createdAt: new Date(now.getTime() - 60_000).toISOString(), disabledAt: null,
      config: { schemaVersion: 1, expression: "* * * * *", timezone: "UTC", misfirePolicy: "skip",
        repository: { installationId: 44, id: 10, fullName: "acme/widgets", defaultBranch: "main" } } });
    expect(await store.materializeDueScheduleOccurrences({ now: now.toISOString(), limit: 100 })).toBe(1);
    await store.disableTrigger("default", triggerId, new Date().toISOString());
    expect(await store.claimScheduleOccurrence({ leaseOwner: "scheduler-2", leaseDurationMs: 60_000 })).toBeUndefined();
  });

  it("does not finalize an expired occurrence lease with a skewed worker clock", async () => {
    const now = new Date();
    const triggerId = `expired-${randomUUID()}`;
    await store.createTrigger({ id: triggerId, tenantId: "default", type: "schedule.cron", enabled: true,
      createdAt: new Date(now.getTime() - 60_000).toISOString(), disabledAt: null,
      config: { schemaVersion: 1, expression: "* * * * *", timezone: "UTC", misfirePolicy: "skip",
        repository: { installationId: 44, id: 10, fullName: "acme/widgets", defaultBranch: "main" } } });
    expect(await store.materializeDueScheduleOccurrences({ now: now.toISOString(), limit: 100 })).toBe(1);
    const claimed = await store.claimScheduleOccurrence({ leaseOwner: "scheduler-3", leaseDurationMs: 60_000 });
    expect(claimed).toBeDefined();

    const pool = (store as unknown as { pool: pg.Pool }).pool;
    await pool.query(`UPDATE dispatch_schedule_occurrences
      SET lease_expires_at = clock_timestamp() - interval '1 second'
      WHERE id = $1`, [claimed!.id]);
    const historicalTimestamp = new Date(Date.now() - 120_000).toISOString();

    expect(await store.completeScheduleOccurrence({
      id: claimed!.id, leaseOwner: claimed!.leaseOwner, leaseToken: claimed!.leaseToken, completedAt: historicalTimestamp,
    })).toBe(false);
    expect((await pool.query<{ state: string }>("SELECT state FROM dispatch_schedule_occurrences WHERE id = $1", [claimed!.id])).rows[0]).toEqual({ state: "LEASED" });

    expect(await store.failScheduleOccurrence({
      id: claimed!.id, leaseOwner: claimed!.leaseOwner, leaseToken: claimed!.leaseToken, error: "late failure",
      failedAt: historicalTimestamp, retryAt: new Date().toISOString(), maxAttempts: 5,
    })).toBe(false);
    expect((await pool.query<{ state: string }>("SELECT state FROM dispatch_schedule_occurrences WHERE id = $1", [claimed!.id])).rows[0]).toEqual({ state: "LEASED" });
  });
});
