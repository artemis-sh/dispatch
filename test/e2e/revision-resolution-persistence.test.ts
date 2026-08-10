import { randomUUID } from "node:crypto";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { hashCanonicalJson, type JsonValue } from "../../src/json.js";
import { createPostgresRuntimeStore, type PostgresRuntimeStore } from "../../src/runtime/postgres.js";

describe("revision resolution persistence", () => {
  let postgres: StartedTestContainer;
  let store: PostgresRuntimeStore;

  beforeAll(async () => {
    postgres = await startPostgres();
    store = await createPostgresRuntimeStore({
      connectionString: connectionString(postgres), runMigrations: true, ssl: false, sslRejectUnauthorized: false,
    });
  });

  afterAll(async () => {
    await store?.close();
    await postgres?.stop();
  });

  it("durably queues, replays, fences, resolves, and atomically admits an issue workspace", async () => {
    const tenantId = "default";
    const triggerId = `trigger-${randomUUID()}`;
    const admittedAt = new Date().toISOString();
    await store.createTrigger({
      config: { schemaVersion: 1, webhookSecretEnv: "DISPATCH_GITHUB_WEBHOOK_SECRET_TEST" },
      createdAt: admittedAt, disabledAt: null, enabled: true, id: triggerId, tenantId, type: "github.app.webhook",
    });
    await store.publishProfileVersion({
      createdAt: admittedAt,
      definition: {
        schemaVersion: 1, runtime: { type: "opencode", agent: "coder", opencodeConfig: { agent: { coder: {} } } },
        sandbox: { templateName: "opencode", warmPool: "none" }, connections: [], permissions: { onRequest: "fail" }, timeoutSeconds: 3600,
      },
      id: randomUUID(), profileId: "developer", tenantId, version: 1,
    });
    await store.publishBindingVersion({
      bindingId: "develop-issue", createdAt: admittedAt, disabledAt: null, enabled: true, id: randomUUID(),
      tenantId, triggerId, version: 1, profile: { id: "developer", version: 1 },
      definition: {
        schemaVersion: 1, eventTypes: ["com.github.issues.opened"], filter: { all: [] },
        prompt: { literal: "Develop", includeEvent: "data" },
        workspace: {
          type: "git", repository: { url: { path: "/repository/cloneUrl" } },
          revision: { commit: { path: "/repository/defaultBranchRevision/commit" } },
        },
      },
    });
    await store.publishBindingVersion({
      bindingId: "notify-issue", createdAt: admittedAt, disabledAt: null, enabled: true, id: randomUUID(),
      tenantId, triggerId, version: 1, profile: { id: "developer", version: 1 },
      definition: {
        schemaVersion: 1, eventTypes: ["com.github.issues.opened"], filter: { all: [] },
        prompt: { literal: "Notify", includeEvent: "data" }, workspace: { type: "empty" },
      },
    });
    const event = {
      specversion: "1.0" as const,
      id: randomUUID(), source: "https://github.com/acme/widgets", type: "com.github.issues.opened",
      datacontenttype: "application/json",
      data: {
        schemaVersion: 1, installationId: 44,
        repository: { id: 10, fullName: "acme/widgets", cloneUrl: "https://github.com/acme/widgets.git", defaultBranch: "main", private: false },
        issue: { number: 7 },
      },
    };
    const internalEventId = randomUUID();
    const command = {
      tenantId, triggerId, internalEventId, event, sourceDeduplicationKey: randomUUID(), admittedAt,
      admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId, event } as JsonValue),
      revisionResolution: {
        provider: "github" as const, installationId: 44, repositoryId: 10,
        repositoryFullName: "acme/widgets", cloneUrl: "https://github.com/acme/widgets.git", branch: "main",
      },
    };

    const admitted = await store.admitEvent(command);
    expect(admitted.executions).toMatchObject([{ binding: { id: "notify-issue" }, workspace: { type: "empty" }, state: "QUEUED" }]);
    expect(admitted.executions).toHaveLength(1);
    expect((await store.admitEvent(command))).toMatchObject({ replayed: true, executions: admitted.executions });

    const claim = await store.claimRevisionResolution({ leaseOwner: "resolver-1", leaseDurationMs: 60_000 });
    expect(claim).toMatchObject({ eventId: internalEventId, installationId: 44, repositoryId: 10, branch: "main", attempt: 1 });
    expect(await store.completeRevisionResolution({
      eventId: internalEventId, tenantId, leaseOwner: "stale", leaseToken: claim!.leaseToken,
      commit: "a".repeat(40), resolvedAt: new Date().toISOString(),
    })).toBeUndefined();

    const completed = await store.completeRevisionResolution({
      eventId: internalEventId, tenantId, leaseOwner: claim!.leaseOwner, leaseToken: claim!.leaseToken,
      commit: "a".repeat(40), resolvedAt: new Date().toISOString(),
    });
    expect(completed?.executions).toEqual([expect.objectContaining({ binding: { id: "develop-issue" }, workspace: {
      type: "git", repository: { url: "https://github.com/acme/widgets.git" },
      revision: { type: "commit", commit: "a".repeat(40) },
    } })]);
    expect((await store.admitEvent(command)).executions).toHaveLength(2);
    expect(await store.claimRevisionResolution({ leaseOwner: "resolver-2", leaseDurationMs: 60_000 })).toBeUndefined();

    const failedEvent = { ...event, id: randomUUID() };
    const failedEventId = randomUUID();
    const failedCommand = {
      ...command,
      internalEventId: failedEventId,
      event: failedEvent,
      sourceDeduplicationKey: randomUUID(),
      admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId, event: failedEvent } as JsonValue),
    };
    const failedAdmission = await store.admitEvent(failedCommand);
    expect(failedAdmission.executions).toMatchObject([{ binding: { id: "notify-issue" }, workspace: { type: "empty" }, state: "QUEUED" }]);
    expect(failedAdmission.executions).toHaveLength(1);
    const failedClaim = await store.claimRevisionResolution({ leaseOwner: "resolver-3", leaseDurationMs: 60_000 });
    expect(failedClaim?.eventId).toBe(failedEventId);
    expect(await store.failRevisionResolution({
      eventId: failedEventId, tenantId, leaseOwner: failedClaim!.leaseOwner, leaseToken: failedClaim!.leaseToken,
      error: "GitHub unavailable", failedAt: new Date().toISOString(), retryAt: new Date().toISOString(), maxAttempts: 1,
    })).toBe(true);
    const replayedAfterFailure = await store.admitEvent(failedCommand);
    expect(replayedAfterFailure.executions).toMatchObject([{ binding: { id: "notify-issue" }, workspace: { type: "empty" }, state: "QUEUED" }]);
    expect(replayedAfterFailure.executions).toHaveLength(1);
  });

  it("rejects failure from a holder whose lease expired on the database clock", async () => {
    const tenantId = "default";
    const triggerId = `trigger-${randomUUID()}`;
    const admittedAt = new Date().toISOString();
    await store.createTrigger({
      config: { schemaVersion: 1, webhookSecretEnv: "DISPATCH_GITHUB_WEBHOOK_SECRET_TEST" },
      createdAt: admittedAt, disabledAt: null, enabled: true, id: triggerId, tenantId, type: "github.app.webhook",
    });
    await store.publishProfileVersion({
      createdAt: admittedAt,
      definition: {
        schemaVersion: 1, runtime: { type: "opencode", agent: "coder", opencodeConfig: { agent: { coder: {} } } },
        sandbox: { templateName: "opencode", warmPool: "none" }, connections: [], permissions: { onRequest: "fail" }, timeoutSeconds: 3600,
      },
      id: randomUUID(), profileId: "developer", tenantId, version: 1,
    });
    await store.publishBindingVersion({
      bindingId: "develop-issue", createdAt: admittedAt, disabledAt: null, enabled: true, id: randomUUID(),
      tenantId, triggerId, version: 1, profile: { id: "developer", version: 1 },
      definition: {
        schemaVersion: 1, eventTypes: ["com.github.issues.opened"], filter: { all: [] },
        prompt: { literal: "Develop", includeEvent: "data" },
        workspace: {
          type: "git", repository: { url: { path: "/repository/cloneUrl" } },
          revision: { commit: { path: "/repository/defaultBranchRevision/commit" } },
        },
      },
    });
    const event = {
      specversion: "1.0" as const,
      id: randomUUID(), source: "https://github.com/acme/widgets", type: "com.github.issues.opened",
      datacontenttype: "application/json",
      data: {
        schemaVersion: 1, installationId: 44,
        repository: { id: 10, fullName: "acme/widgets", cloneUrl: "https://github.com/acme/widgets.git", defaultBranch: "main", private: false },
        issue: { number: 7 },
      },
    };
    const eventId = randomUUID();
    await store.admitEvent({
      tenantId, triggerId, internalEventId: eventId, event, sourceDeduplicationKey: randomUUID(), admittedAt,
      admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId, event } as JsonValue),
      revisionResolution: {
        provider: "github", installationId: 44, repositoryId: 10,
        repositoryFullName: "acme/widgets", cloneUrl: "https://github.com/acme/widgets.git", branch: "main",
      },
    });

    const claim = await store.claimRevisionResolution({ leaseOwner: "resolver-1", leaseDurationMs: 60_000 });
    expect(claim).toMatchObject({ eventId, tenantId });
    const pool = store as unknown as { pool: { query(text: string, values: readonly unknown[]): Promise<unknown> } };
    await pool.pool.query(
      `UPDATE dispatch_event_revision_resolutions
       SET lease_expires_at = clock_timestamp() - interval '1 second'
       WHERE event_id = $1 AND tenant_id = $2`,
      [eventId, tenantId],
    );

    const applied = await store.failRevisionResolution({
      eventId, tenantId, leaseOwner: claim!.leaseOwner, leaseToken: claim!.leaseToken,
      error: "late resolver failure",
      failedAt: new Date(Date.now() - 120_000).toISOString(),
      retryAt: new Date(Date.now() - 60_000).toISOString(),
      maxAttempts: 1,
    });

    expect(applied).toBe(false);
    expect(await store.claimRevisionResolution({ leaseOwner: "resolver-2", leaseDurationMs: 60_000 })).toMatchObject({ eventId, tenantId });
  });
});

async function startPostgres(): Promise<StartedTestContainer> {
  return new GenericContainer("postgres:16-alpine")
    .withEnvironment({ POSTGRES_DB: "dispatch", POSTGRES_PASSWORD: "dispatch-password", POSTGRES_USER: "dispatch" })
    .withExposedPorts(5432)
    .withHealthCheck({ interval: 1_000, retries: 30, test: ["CMD-SHELL", "pg_isready -U dispatch -d dispatch"], timeout: 5_000 })
    .withWaitStrategy(Wait.forHealthCheck())
    .start();
}

function connectionString(container: StartedTestContainer): string {
  return `postgresql://dispatch:dispatch-password@${container.getHost()}:${container.getMappedPort(5432)}/dispatch`;
}
