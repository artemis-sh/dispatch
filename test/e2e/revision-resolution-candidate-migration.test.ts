import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import pg from "pg";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createPostgresRuntimeStore } from "../../src/runtime/postgres.js";

const { Client } = pg;

describe("revision resolution candidate migration", () => {
  let postgres: StartedTestContainer;
  let client: pg.Client;

  beforeAll(async () => {
    postgres = await new GenericContainer("postgres:16-alpine")
      .withEnvironment({ POSTGRES_DB: "dispatch", POSTGRES_PASSWORD: "dispatch-password", POSTGRES_USER: "dispatch" })
      .withExposedPorts(5432)
      .withHealthCheck({ interval: 1_000, retries: 30, test: ["CMD-SHELL", "pg_isready -U dispatch -d dispatch"], timeout: 5_000 })
      .withWaitStrategy(Wait.forHealthCheck())
      .start();
    client = new Client({ connectionString: connectionString(postgres) });
    await client.connect();
  });

  afterAll(async () => {
    await client?.end();
    await postgres?.stop();
  });

  it("backfills only bindings that matched and required default-branch resolution at admission", async () => {
    const migrationsDirectory = path.resolve(process.cwd(), "drizzle");
    const migrations = (await readdir(migrationsDirectory)).filter((file) => /^\d{4}.*\.sql$/.test(file)).sort();
    for (const migration of migrations.filter((file) => file < "0018")) {
      await executeMigration(client, path.join(migrationsDirectory, migration));
    }

    const admittedAt = "2026-08-31T06:00:00.000Z";
    await client.query(`INSERT INTO dispatch_agent_profile_versions
      (id, tenant_id, profile_id, version, definition, created_at)
      VALUES ('profile-version', 'default', 'developer', 1,
        '{"schemaVersion":1,"runtime":{"type":"opencode","agent":"coder","opencodeConfig":{"agent":{"coder":{}}}},"sandbox":{"templateName":"opencode","warmPool":"none"},"connections":[],"permissions":{"onRequest":"fail"},"timeoutSeconds":3600}', $1)`, [admittedAt]);
    await client.query(`INSERT INTO dispatch_triggers
      (id, tenant_id, type, config, enabled, created_at)
      VALUES ('trigger', 'default', 'github.app.webhook', '{"schemaVersion":1,"webhookSecretEnv":"SECRET"}', true, $1)`, [admittedAt]);

    const definition = (workspace: unknown, filter: unknown) => JSON.stringify({
      schemaVersion: 1,
      filter,
      prompt: { literal: "Develop", includeEvent: "data" },
      workspace,
    });
    const defaultBranchWorkspace = {
      type: "git", repository: { url: { path: "/repository/cloneUrl" } },
      revision: { commit: { path: "/repository/defaultBranchRevision/commit" } },
    };
    const bindings = [
      ["ordinary", { type: "empty" }, { all: [] }],
      ["deferred", defaultBranchWorkspace, { all: [{ path: "/issue/number", op: "eq", value: 7 }] }],
      ["filter-mismatch", defaultBranchWorkspace, { all: [{ path: "/issue/number", op: "eq", value: 8 }] }],
    ] as const;
    for (const [id, workspace, filter] of bindings) {
      await client.query(`INSERT INTO dispatch_binding_versions
        (id, tenant_id, binding_id, version, trigger_id, profile_version_id, definition, event_types, enabled, created_at)
        VALUES ($1, 'default', $1, 1, 'trigger', 'profile-version', $2::jsonb, ARRAY['com.github.issues.opened'], true, $3)`,
      [id, definition(workspace, filter), admittedAt]);
    }

    await client.query(`INSERT INTO dispatch_events
      (id, tenant_id, trigger_id, event_id, source, source_deduplication_key, admission_hash, type, data, ingested_at)
      VALUES ('event', 'default', 'trigger', 'delivery', 'https://github.com/acme/widgets', 'dedupe', 'hash',
        'com.github.issues.opened', '{"schemaVersion":1,"repository":{"cloneUrl":"https://github.com/acme/widgets.git"},"issue":{"number":7}}', $1)`, [admittedAt]);
    await client.query(`INSERT INTO dispatch_event_revision_resolutions
      (event_id, tenant_id, provider, installation_id, repository_id, repository_full_name, clone_url, branch, state, created_at, updated_at)
      VALUES ('event', 'default', 'github', '44', '10', 'acme/widgets', 'https://github.com/acme/widgets.git', 'main', 'PENDING', $1, $1)`, [admittedAt]);

    await executeMigration(client, path.join(migrationsDirectory, "0018_revision_resolution_candidates.sql"));

    const candidates = await client.query<{ binding_version_id: string }>(
      "SELECT binding_version_id FROM dispatch_event_revision_resolution_candidates ORDER BY binding_version_id",
    );
    expect(candidates.rows).toEqual([{ binding_version_id: "deferred" }]);
    expect((await client.query("SELECT to_regprocedure('dispatch_migration_0018_resolve_json_pointer(jsonb,text)') AS helper")).rows[0]).toEqual({ helper: null });

    const store = await createPostgresRuntimeStore({ connectionString: connectionString(postgres), runMigrations: false, ssl: false, sslRejectUnauthorized: false });
    try {
      const claim = await store.claimRevisionResolution({ leaseOwner: "migration-test", leaseDurationMs: 60_000 });
      const completed = await store.completeRevisionResolution({
        eventId: "event", tenantId: "default", leaseOwner: claim!.leaseOwner, leaseToken: claim!.leaseToken,
        commit: "a".repeat(40), resolvedAt: "2026-08-31T06:01:00.000Z",
      });
      expect(completed?.executions).toEqual([expect.objectContaining({ binding: { id: "deferred", version: 1 } })]);
      expect((await client.query("SELECT binding_version_id FROM dispatch_executions ORDER BY binding_version_id")).rows)
        .toEqual([{ binding_version_id: "deferred" }]);
    } finally {
      await store.close();
    }
  });
});

async function executeMigration(client: pg.Client, migrationPath: string): Promise<void> {
  const sql = await readFile(migrationPath, "utf8");
  for (const statement of sql.split("--> statement-breakpoint").map((part) => part.trim()).filter(Boolean)) {
    await client.query(statement);
  }
}

function connectionString(container: StartedTestContainer): string {
  return `postgresql://dispatch:dispatch-password@${container.getHost()}:${container.getMappedPort(5432)}/dispatch`;
}
