import { createHash, createHmac, randomUUID } from "node:crypto";
import pg from "pg";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { mountGitHubWebhookApi } from "../../src/connectors/github/api.js";
import { createOpenApiApp } from "../../src/openapi.js";
import { createPostgresRuntimeStore, type PostgresRuntimeStore } from "../../src/runtime/postgres.js";

const { Pool } = pg;
const SECRET_ENV = "DISPATCH_GITHUB_WEBHOOK_SECRET_PERSISTENCE_TEST";
const SECRET = "a sufficiently long persistence webhook secret";
const TRIGGER_ID = "github-persistence";

describe("GitHub webhook persistence", () => {
  let postgres: StartedTestContainer;
  let store: PostgresRuntimeStore;
  let pool: pg.Pool;

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
  });

  afterAll(async () => {
    await pool?.end();
    await store?.close();
    await postgres?.stop();
  });

  it("persists signed fork pull requests with durable replay and trigger lifecycle semantics", async () => {
    const createdAt = new Date().toISOString();
    const profileVersion = await store.publishProfileVersion({
      createdAt,
      definition: {
        schemaVersion: 1,
        runtime: { type: "opencode", agent: "coder", opencodeConfig: { agent: { coder: { prompt: "Review" } } } },
        sandbox: { templateName: "opencode", warmPool: "none" },
        connections: [],
        permissions: { onRequest: "fail" },
        timeoutSeconds: 3_600,
      },
      id: randomUUID(),
      profileId: "github-reviewer",
      tenantId: "default",
      version: 1,
    });
    const trigger = await store.createTrigger({
      config: { schemaVersion: 1, webhookSecretEnv: SECRET_ENV },
      createdAt,
      disabledAt: null,
      enabled: true,
      id: TRIGGER_ID,
      tenantId: "default",
      type: "github.app.webhook",
    });
    await store.publishBindingVersion({
      bindingId: "github-fork-review",
      createdAt,
      definition: {
        eventTypes: ["com.github.pull_request.synchronize"],
        filter: { all: [] },
        prompt: { includeEvent: "data", literal: "Review the updated pull request" },
        schemaVersion: 1,
        workspace: {
          type: "git",
          repository: { url: { path: "/pullRequest/head/repository/cloneUrl" } },
          revision: { commit: { path: "/pullRequest/head/sha" } },
        },
      },
      disabledAt: null,
      enabled: true,
      id: randomUUID(),
      profile: { id: "github-reviewer", version: 1 },
      tenantId: "default",
      triggerId: TRIGGER_ID,
      version: 1,
    });
    await store.publishBindingVersion({
      bindingId: "github-default-branch-review",
      createdAt,
      definition: {
        eventTypes: ["com.github.pull_request.synchronize"],
        filter: { all: [] },
        prompt: { includeEvent: "data", literal: "Review the default branch workspace" },
        schemaVersion: 1,
        workspace: {
          type: "git",
          repository: { url: { path: "/repository/cloneUrl" } },
          revision: { commit: { path: "/repository/defaultBranchRevision/commit" } },
        },
      },
      disabledAt: null,
      enabled: true,
      id: randomUUID(),
      profile: { id: "github-reviewer", version: 1 },
      tenantId: "default",
      triggerId: TRIGGER_ID,
      version: 1,
    });

    expect(await store.getProfileVersion("default", "github-reviewer", 1)).toEqual(profileVersion);
    expect(await store.getTrigger("default", TRIGGER_ID)).toEqual(trigger);
    const persistedTrigger = (await pool.query(
      "select config, config::text as config_text from dispatch_triggers where tenant_id = $1 and id = $2",
      ["default", TRIGGER_ID],
    )).rows[0];
    expect(persistedTrigger.config).toEqual({ schemaVersion: 1, webhookSecretEnv: SECRET_ENV });
    expect(persistedTrigger.config_text).not.toContain(SECRET);

    const disabledResolverApp = createOpenApiApp();
    mountGitHubWebhookApi(disabledResolverApp, store, (name) => name === SECRET_ENV ? SECRET : undefined);
    const delivery = "fork-delivery-1";
    const payload = pullRequestPayload();
    const raw = JSON.stringify(payload);
    expect((await webhook(disabledResolverApp, delivery, raw)).status).toBe(422);
    expect(await persistedAdmission()).toEqual({ events: [], executions: [] });

    const app = createOpenApiApp();
    mountGitHubWebhookApi(app, store, (name) => name === SECRET_ENV ? SECRET : undefined, undefined, false, true);
    expect((await webhook(app, delivery, raw)).status).toBe(202);
    expect(await store.claimRevisionResolution({
      leaseOwner: "resolver",
      leaseDurationMs: 60_000,
    })).toMatchObject({
      installationId: 44,
      repositoryId: 10,
      branch: "main",
    });

    const first = await persistedAdmission();
    const expectedPayloadHash = createHash("sha256").update(raw).digest("hex");
    expect(first.events).toEqual([expect.objectContaining({
      event_id: delivery,
      source: "https://github.com/acme/widgets",
      type: "com.github.pull_request.synchronize",
      extensions: {
        githubevent: "pull_request",
        githubpayloadsha256: expectedPayloadHash,
      },
    })]);
    expect(first.events[0]?.admission_hash).toMatch(/^[0-9a-f]{64}$/);
    expect(first.executions).toEqual([expect.objectContaining({
      event_id: first.events[0]?.id,
      state: "QUEUED",
      workspace: {
        type: "git",
        repository: { url: "https://github.com/contributor/widgets.git" },
        revision: { type: "commit", commit: "abcdef0123456789abcdef0123456789abcdef01" },
      },
    })]);

    expect((await webhook(app, delivery, raw)).status).toBe(202);
    const replay = await persistedAdmission();
    expect(replay.events.map((event) => event.id)).toEqual(first.events.map((event) => event.id));
    expect(replay.executions.map((execution) => execution.id)).toEqual(first.executions.map((execution) => execution.id));
    expect(replay.events).toHaveLength(1);
    expect(replay.executions).toHaveLength(1);

    const changedRaw = JSON.stringify(pullRequestPayload({ title: "Changed after delivery" }));
    expect((await webhook(app, delivery, changedRaw)).status).toBe(409);
    expect(await persistedAdmission()).toEqual(replay);

    await store.disableTrigger("default", TRIGGER_ID, new Date().toISOString());
    expect((await webhook(app, delivery, raw)).status).toBe(202);
    expect(await persistedAdmission()).toEqual(replay);
    expect((await webhook(app, "fork-delivery-2", raw)).status).toBe(404);
    expect(await persistedAdmission()).toEqual(replay);
  });

  async function persistedAdmission() {
    const events = (await pool.query(
      "select id, event_id, source, type, admission_hash, extensions from dispatch_events where tenant_id = $1 and trigger_id = $2 order by id",
      ["default", TRIGGER_ID],
    )).rows as Array<{
      admission_hash: string;
      event_id: string;
      extensions: Record<string, unknown>;
      id: string;
      source: string;
      type: string;
    }>;
    const executions = (await pool.query(
      `select execution.id, execution.event_id, execution.state, execution.workspace
       from dispatch_executions as execution
       join dispatch_events as event on event.id = execution.event_id and event.tenant_id = execution.tenant_id
       where event.tenant_id = $1 and event.trigger_id = $2 order by execution.id`,
      ["default", TRIGGER_ID],
    )).rows as Array<{ event_id: string; id: string; state: string; workspace: Record<string, unknown> }>;
    return { events, executions };
  }
});

async function webhook(
  app: ReturnType<typeof createOpenApiApp>,
  delivery: string,
  raw: string,
) {
  return app.request(`/hooks/github/${TRIGGER_ID}`, {
    method: "POST",
    body: raw,
    headers: {
      "content-type": "application/json",
      "x-github-delivery": delivery,
      "x-github-event": "pull_request",
      "x-hub-signature-256": `sha256=${createHmac("sha256", SECRET).update(raw).digest("hex")}`,
    },
  });
}

function pullRequestPayload(overrides: { title?: string } = {}) {
  const actor = { id: 2, login: "contributor", type: "User" };
  const repository = (id: number, fullName: string) => ({
    id,
    full_name: fullName,
    clone_url: `https://github.com/${fullName}.git`,
  });
  return {
    action: "synchronize",
    installation: { id: 44 },
    repository: {
      ...repository(10, "acme/widgets"),
      default_branch: "main",
      private: false,
    },
    sender: actor,
    pull_request: {
      id: 700,
      number: 17,
      title: overrides.title ?? "Update widgets",
      body: "Please review this fork update",
      draft: false,
      state: "open",
      merged: false,
      user: actor,
      head: {
        sha: "ABCDEF0123456789ABCDEF0123456789ABCDEF01",
        ref: "feature/fork-update",
        repo: repository(20, "contributor/widgets"),
      },
      base: {
        sha: "1234567890abcdef1234567890abcdef12345678",
        ref: "main",
        repo: repository(10, "acme/widgets"),
      },
      labels: [],
      assignees: [],
      requested_reviewers: [],
      created_at: "2026-07-17T10:00:00Z",
      updated_at: "2026-07-18T10:00:00Z",
      closed_at: null,
      merged_at: null,
    },
  };
}

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
