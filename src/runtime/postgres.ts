import path from "node:path";
import { randomUUID } from "node:crypto";
import { and, eq, type InferSelectModel } from "drizzle-orm";
import { drizzle, type NodePgDatabase } from "drizzle-orm/node-postgres";
import { migrate } from "drizzle-orm/node-postgres/migrator";
import pg from "pg";
import { isExecutionState, isTerminalExecutionState } from "../execution/states.js";
import { isAttemptState } from "../dispatch/states.js";
import {
  isValidTransitionLeasedExecutionCommand,
  type DispatcherExecutionStore,
} from "../dispatch/store.js";
import type {
  AcknowledgeLeasedExecutionCancellationCommand,
  AcknowledgeLeasedExecutionCancellationResult,
  ClaimedExecution,
  CheckpointLeasedExecutionAttemptDiagnosticCommand,
  ExecutionLeaseRenewalResult,
  FinalizeRequestedExecutionCancellationCommand,
  FinalizedRequestedExecutionCancellation,
  PromotedExecutionRetry,
  RecoveredExecutionLease,
  RequestedCancellationCleanup,
  TransitionLeasedExecutionCommand,
  TransitionLeasedExecutionResult,
  CompleteLeasedExecutionTurnCommand,
  CompleteLeasedExecutionTurnResult,
  ExpiredEventWait,
} from "../dispatch/types.js";
import type {
  EventAdmissionStore,
  ExecutionStore,
  PublishProfileVersionCommand,
} from "../execution/store.js";
import type {
  ClaimedOutboxMessage,
  OutboxStore,
} from "../outbox/types.js";
import {
  IdempotencyConflictError,
  ExecutionCancellationConflictError,
  ProfileVersionAlreadyExistsError,
  ProfileVersionNotFoundError,
  type AgentProfileVersion,
  type Execution,
  type ExecutionAttempt,
  type ExecutionDetail,
  type ExecutionStateTransition,
  type RequestExecutionCancellationCommand,
  type RequestExecutionCancellationResult,
} from "../execution/types.js";
import { addCheckpointInput, planExecution, projectActiveSingleton, projectCheckpoint, projectWakeCorrelation, renderPromptInput, type AdmissionCommand, type AdmissionResult, type AdmissionWakeResult, type PendingWakeResult } from "../control/admission.js";
import {
  ConnectionAlreadyExistsError,
  ConnectionNotFoundError,
  type Connection,
  type ConnectionStore,
  parseConnection,
  type CreateConnectionCommand,
} from "../connection/index.js";
import { bindingDefinitionSchema, isWakeBinding, publishedBindingVersionSchema, BindingVersionAlreadyExistsError, type BindingStore, type PublishedBindingVersion, type WakeBindingDefinition } from "../control/binding.js";
import { triggerSchema, TriggerAlreadyExistsError, TriggerNotFoundError, type Trigger, type TriggerStore } from "../control/trigger.js";
import { agentProfileDefinitionSchema } from "../execution/api-schema.js";
import { hashCanonicalJson, resolveJsonPointer, type JsonPrimitive, type JsonValue } from "../json.js";
import { normalizedCloudEventSchema, type NormalizedCloudEvent } from "../execution/events.js";
import type { ClaimedRevisionResolution, RevisionAwareAdmissionCommand, RevisionResolutionStore } from "../revision/types.js";
import { matchesBinding } from "../control/admission.js";
import { resolvedWorkspaceSchema } from "../workspace/schema.js";
import { resolveWorkspace } from "../workspace/resolver.js";
import { nextCronOccurrence } from "../schedule/cron.js";
import type { ClaimedScheduleOccurrence, ScheduleStore } from "../schedule/types.js";
import type { ObservabilitySnapshot, ObservabilitySnapshotRow, ObservabilityStore } from "../observability/types.js";
import { checkpointAdvances, eventsAdmitted, executionsCreated, githubEffects } from "../observability/metrics.js";
import * as schema from "./schema.js";
import {
  agentProfileVersions,
  bindingVersions,
  events,
  executions,
  executionTransitions,
  outboxEntries,
  triggers,
} from "./schema.js";

const cancellationCleanupFence = Symbol("cancellationCleanupFence");
type CancellationCleanupCandidate = RequestedCancellationCleanup & {
  [cancellationCleanupFence]?: string | null;
};

const { Pool } = pg;

type RuntimeDatabase = NodePgDatabase<typeof schema>;

function countMissedCronIntervals(expression: string, nextFireAt: Date, now: Date): number {
  let count = 0;
  let occurrence = nextFireAt;
  while (occurrence <= now && count < 10_000) {
    count += 1;
    occurrence = nextCronOccurrence(expression, occurrence);
  }
  return count;
}

export type PostgresRuntimeStoreOptions = {
  connectionString?: string;
  database?: string;
  host?: string;
  migrationsFolder?: string;
  password?: string;
  port?: number;
  runMigrations?: boolean;
  ssl: boolean;
  sslRejectUnauthorized: boolean;
  user?: string;
};

export class PersistedExecutionCorruptionError extends Error {
  constructor(executionId: string, field: string, options?: ErrorOptions) {
    super(`Execution ${executionId} has an invalid persisted ${field}`, options);
    this.name = "PersistedExecutionCorruptionError";
  }
}

export async function createPostgresRuntimeStore(options: PostgresRuntimeStoreOptions): Promise<PostgresRuntimeStore> {
  const pool = new Pool({
    connectionTimeoutMillis: 10_000,
    connectionString: options.connectionString,
    database: options.database,
    host: options.host,
    password: options.password,
    port: options.port,
    ssl: options.ssl ? { rejectUnauthorized: options.sslRejectUnauthorized } : undefined,
    statement_timeout: 30_000,
    user: options.user,
  });
  const db = drizzle(pool, { schema });
  const store = new PostgresRuntimeStore(pool, db, options.migrationsFolder ?? path.resolve(process.cwd(), "drizzle"));
  if (options.runMigrations) await store.initialize();
  return store;
}

export async function migratePostgresRuntimeStore(options: PostgresRuntimeStoreOptions): Promise<void> {
  const store = await createPostgresRuntimeStore({ ...options, runMigrations: false });
  try {
    await store.initialize();
  } finally {
    await store.close();
  }
}

export class PostgresRuntimeStore implements ExecutionStore, TriggerStore, BindingStore, ConnectionStore, EventAdmissionStore, OutboxStore, DispatcherExecutionStore, RevisionResolutionStore, ScheduleStore, ObservabilityStore {
  constructor(
    private readonly pool: pg.Pool,
    private readonly db: RuntimeDatabase,
    private readonly migrationsFolder: string,
  ) {}

  async initialize(): Promise<void> {
    await migrate(this.db, { migrationsFolder: this.migrationsFolder });
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  async collectObservabilitySnapshot(signal?: AbortSignal): Promise<ObservabilitySnapshot> {
    const collectedAt = new Date();
    const result = await this.pool.query<{ kind: ObservabilitySnapshotRow["kind"]; tenant_id: string; label: string; value: string; secondary_value: string | null; extra_value: string | null }>({
      text: `WITH clock AS MATERIALIZED (SELECT clock_timestamp() AS now)
        SELECT 'execution_state' AS kind, tenant_id, state AS label, count(*)::text AS value, NULL::text AS secondary_value, NULL::text AS extra_value
          FROM dispatch_executions GROUP BY tenant_id,state
        UNION ALL
        SELECT 'execution_oldest_active_age',tenant_id,'',COALESCE(EXTRACT(EPOCH FROM ((SELECT now FROM clock)-min(created_at))),0)::text,NULL,NULL
          FROM dispatch_executions WHERE state NOT IN ('SUCCEEDED','COMPLETED','CANCELLED','TIMED_OUT','FAILED','DEAD_LETTERED') GROUP BY tenant_id
        UNION ALL
        SELECT 'execution_overdue',tenant_id,'',count(*)::text,NULL,NULL FROM dispatch_executions
          WHERE state NOT IN ('SUCCEEDED','COMPLETED','CANCELLED','TIMED_OUT','FAILED','DEAD_LETTERED') AND timeout_at < (SELECT now FROM clock) GROUP BY tenant_id
        UNION ALL
        SELECT 'outbox_pending',tenant_id,topic,count(*)::text,COALESCE(EXTRACT(EPOCH FROM ((SELECT now FROM clock)-min(created_at))),0)::text,NULL
          FROM dispatch_outbox WHERE published_at IS NULL GROUP BY tenant_id,topic
        UNION ALL
        SELECT 'schedule_lateness',state.tenant_id,state.trigger_id,GREATEST(0,EXTRACT(EPOCH FROM ((SELECT now FROM clock)-state.next_fire_at)))::text,
          EXTRACT(EPOCH FROM state.next_fire_at)::text,trigger.config->>'expression'
          FROM dispatch_schedule_states state JOIN dispatch_triggers trigger ON trigger.tenant_id=state.tenant_id AND trigger.id=state.trigger_id WHERE trigger.enabled
        UNION ALL
        SELECT 'checkpoint_age',tenant_id,binding_id,GREATEST(0,EXTRACT(EPOCH FROM ((SELECT now FROM clock)-min(advanced_at))))::text,NULL,NULL
          FROM dispatch_execution_checkpoints GROUP BY tenant_id,binding_id
        UNION ALL
        SELECT 'active_workloads',tenant_id,'',count(*)::text,NULL,NULL FROM dispatch_execution_attempts
          WHERE state IN ('LEASED','RUNNING') GROUP BY tenant_id
        UNION ALL
        SELECT 'revision_pending',tenant_id,'',count(*)::text,NULL,NULL FROM dispatch_event_revision_resolutions
          WHERE state IN ('PENDING','LEASED','RETRY_WAIT') GROUP BY tenant_id`,
      values: [],
      ...(signal ? { signal } : {}),
    });
    return {
      collectedAt,
      rows: result.rows.map((row) => {
        const value = Number(row.value);
        const missedIntervals = row.kind === "schedule_lateness" && row.extra_value && row.secondary_value
          ? countMissedCronIntervals(row.extra_value, new Date(Number(row.secondary_value) * 1_000), collectedAt)
          : undefined;
        return {
          kind: row.kind,
          tenantId: row.tenant_id,
          label: row.label,
          value,
          ...(missedIntervals === undefined
            ? row.secondary_value === null ? {} : { secondaryValue: Number(row.secondary_value) }
            : { secondaryValue: missedIntervals }),
        };
      }),
    };
  }

  async claimAvailable(options: {
    claimToken: string;
    limit: number;
    leaseDurationMs: number;
    topics?: readonly string[];
    signal?: AbortSignal;
  }): Promise<ClaimedOutboxMessage[]> {
    assertPositiveInteger(options.limit, "Outbox claim limit");
    assertPositiveInteger(options.leaseDurationMs, "Outbox lease duration");
    if (options.topics !== undefined && options.topics.length === 0) throw new RangeError("Outbox claim topics must not be empty");
    options.signal?.throwIfAborted();

    const result = await this.pool.query<ClaimedOutboxRow>({
      text: `
      WITH claim_clock AS MATERIALIZED (
        SELECT clock_timestamp() AS claimed_at
      ), candidates AS (
        SELECT outbox.id
        FROM dispatch_outbox AS outbox, claim_clock
        WHERE outbox.published_at IS NULL
          AND outbox.available_at <= claim_clock.claimed_at
          AND (outbox.lease_expires_at IS NULL OR outbox.lease_expires_at <= claim_clock.claimed_at)
          AND ($4::text[] IS NULL OR outbox.topic = ANY($4::text[]))
        ORDER BY outbox.available_at, outbox.created_at, outbox.id
        FOR UPDATE OF outbox SKIP LOCKED
        LIMIT $1
      )
      UPDATE dispatch_outbox AS outbox
      SET lease_token = $2,
          lease_expires_at = claim_clock.claimed_at + ($3::double precision * interval '1 millisecond'),
          publish_attempts = outbox.publish_attempts + 1
      FROM candidates, claim_clock
      WHERE outbox.id = candidates.id
      RETURNING outbox.aggregate_id, outbox.aggregate_type, outbox.available_at, outbox.created_at,
                outbox.headers, outbox.id, outbox.lease_expires_at, outbox.lease_token,
                outbox.payload, outbox.publish_attempts, outbox.tenant_id, outbox.topic
      `,
      values: [options.limit, options.claimToken, options.leaseDurationMs, options.topics ?? null],
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    });

    return result.rows.map(outboxMessageFromRow);
  }

  async markPublished(command: { id: string; claimToken: string }): Promise<boolean> {
    const result = await this.pool.query(`
      UPDATE dispatch_outbox
      SET published_at = clock_timestamp(), lease_token = NULL, lease_expires_at = NULL, last_error = NULL
      WHERE id = $1 AND published_at IS NULL AND lease_token = $2 AND lease_expires_at > clock_timestamp()
      RETURNING id
    `, [command.id, command.claimToken]);
    return result.rowCount === 1;
  }

  async markFailed(command: { id: string; claimToken: string; error: string; retryDelayMs: number }): Promise<boolean> {
    assertNonnegativeInteger(command.retryDelayMs, "Outbox retry delay");
    const result = await this.pool.query(`
      UPDATE dispatch_outbox
      SET available_at = clock_timestamp() + ($3::double precision * interval '1 millisecond'),
          lease_token = NULL, lease_expires_at = NULL, last_error = $4
      WHERE id = $1 AND published_at IS NULL AND lease_token = $2 AND lease_expires_at > clock_timestamp()
      RETURNING id
    `, [command.id, command.claimToken, command.retryDelayMs, command.error]);
    return result.rowCount === 1;
  }

  async publishProfileVersion(command: PublishProfileVersionCommand): Promise<AgentProfileVersion> {
    const definition = agentProfileDefinitionSchema.parse(command.definition);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const inserted = await client.query<AgentProfileVersionSqlRow>({
        text: `INSERT INTO dispatch_agent_profile_versions
          (id, tenant_id, profile_id, version, definition, created_at)
          VALUES ($1, $2, $3, $4, $5::jsonb, $6)
          ON CONFLICT DO NOTHING RETURNING *`,
        values: [command.id, command.tenantId, command.profileId, command.version, JSON.stringify(definition), new Date(command.createdAt)],
      });
      const row = inserted.rows[0];
      if (!row) throw new ProfileVersionAlreadyExistsError(command.profileId, command.version);

      for (const [ordinal, grant] of definition.connections.entries()) {
        const connection = await client.query<{ id: string }>(
          "SELECT id FROM dispatch_connections WHERE tenant_id = $1 AND connection_id = $2 FOR SHARE",
          [command.tenantId, grant.id],
        );
        if (!connection.rows[0]) throw new ConnectionNotFoundError(grant.id);
        await client.query(`INSERT INTO dispatch_agent_profile_version_connections
          (profile_version_id, tenant_id, connection_id, sidecar, ordinal)
          VALUES ($1, $2, $3, $4, $5)`,
        [command.id, command.tenantId, connection.rows[0].id, grant.sidecar, ordinal]);
      }

      await client.query("COMMIT");
      return profileVersionFromSqlRow(row);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async getProfileVersion(tenantId: string, profileId: string, version: number): Promise<AgentProfileVersion | undefined> {
    const rows = await this.db
      .select()
      .from(agentProfileVersions)
      .where(and(
        eq(agentProfileVersions.tenantID, tenantId),
        eq(agentProfileVersions.profileID, profileId),
        eq(agentProfileVersions.version, version),
      ))
      .limit(1);
    return rows[0] ? profileVersionFromRow(rows[0]) : undefined;
  }

  async createConnection(command: CreateConnectionCommand): Promise<Connection> {
    command = parseConnection(command);
    const result = await this.pool.query<ConnectionRow>(`INSERT INTO dispatch_connections
      (id, tenant_id, connection_id, type, created_at)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT DO NOTHING RETURNING *`,
    [command.id, command.tenantId, command.connection.id, command.connection.type, new Date(command.createdAt)]);
    if (!result.rows[0]) throw new ConnectionAlreadyExistsError(command.connection.id);
    return connectionFromRow(result.rows[0]);
  }

  async getConnection(tenantId: string, connectionId: string): Promise<Connection | undefined> {
    const result = await this.pool.query<ConnectionRow>(
      "SELECT * FROM dispatch_connections WHERE tenant_id = $1 AND connection_id = $2",
      [tenantId, connectionId],
    );
    return result.rows[0] ? connectionFromRow(result.rows[0]) : undefined;
  }

  async createTrigger(value: Trigger): Promise<Trigger> {
    const trigger = triggerSchema.parse(value);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<TriggerSqlRow>(`INSERT INTO dispatch_triggers
        (id, tenant_id, type, config, enabled, created_at, disabled_at)
        VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7) ON CONFLICT (id) DO NOTHING RETURNING *`,
      [trigger.id, trigger.tenantId, trigger.type, JSON.stringify(trigger.config), trigger.enabled, new Date(trigger.createdAt),
        trigger.disabledAt ? new Date(trigger.disabledAt) : null]);
      if (!result.rows[0]) throw new TriggerAlreadyExistsError(trigger.id);
      if (trigger.type === "schedule.cron") {
        await client.query(`INSERT INTO dispatch_schedule_states (tenant_id, trigger_id, next_fire_at, updated_at)
          VALUES ($1,$2,$3,$4)`, [trigger.tenantId, trigger.id,
          nextCronOccurrence(trigger.config.expression, new Date(trigger.createdAt)), new Date(trigger.createdAt)]);
      }
      await client.query("COMMIT");
      return triggerFromRow(result.rows[0]);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async materializeDueScheduleOccurrences(input: { now: string; limit: number }): Promise<number> {
    const now = new Date(input.now);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const due = await client.query<{ tenant_id: string; trigger_id: string; next_fire_at: Date; config: unknown }>(`
        SELECT schedule.tenant_id, schedule.trigger_id, schedule.next_fire_at, trigger.config
        FROM dispatch_schedule_states AS schedule
        JOIN dispatch_triggers AS trigger ON trigger.id = schedule.trigger_id AND trigger.tenant_id = schedule.tenant_id
        WHERE trigger.enabled AND trigger.type = 'schedule.cron' AND schedule.next_fire_at <= $1
        ORDER BY schedule.next_fire_at, schedule.trigger_id FOR UPDATE OF schedule SKIP LOCKED LIMIT $2`, [now, input.limit]);
      for (const row of due.rows) {
        const trigger = triggerSchema.parse({ id: row.trigger_id, tenantId: row.tenant_id, type: "schedule.cron", config: row.config,
          enabled: true, createdAt: now.toISOString(), disabledAt: null });
        if (trigger.type !== "schedule.cron") throw new Error("Persisted schedule trigger is invalid");
        await client.query(`INSERT INTO dispatch_schedule_occurrences
          (id, tenant_id, trigger_id, scheduled_at, state, available_at, created_at, updated_at)
          VALUES ($1,$2,$3,$4,'PENDING',$5,$5,$5) ON CONFLICT (tenant_id, trigger_id, scheduled_at) DO NOTHING`,
        [randomUUID(), row.tenant_id, row.trigger_id, row.next_fire_at, now]);
        await client.query(`UPDATE dispatch_schedule_states SET next_fire_at=$3, updated_at=$4
          WHERE tenant_id=$1 AND trigger_id=$2`, [row.tenant_id, row.trigger_id,
          nextCronOccurrence(trigger.config.expression, now), now]);
      }
      await client.query("COMMIT");
      return due.rowCount ?? 0;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async claimScheduleOccurrence(input: { leaseOwner: string; leaseDurationMs: number }): Promise<ClaimedScheduleOccurrence | undefined> {
    const leaseToken = randomUUID();
    const result = await this.pool.query<ScheduleOccurrenceRow>(`WITH candidate AS (
      SELECT occurrence.id FROM dispatch_schedule_occurrences AS occurrence
      JOIN dispatch_triggers AS trigger ON trigger.id=occurrence.trigger_id AND trigger.tenant_id=occurrence.tenant_id
      WHERE trigger.enabled AND ((occurrence.state IN ('PENDING','RETRY_WAIT') AND occurrence.available_at <= now())
        OR (occurrence.state='LEASED' AND occurrence.lease_expires_at <= now()))
      ORDER BY occurrence.available_at, occurrence.scheduled_at FOR UPDATE OF occurrence SKIP LOCKED LIMIT 1
    ) UPDATE dispatch_schedule_occurrences AS occurrence SET state='LEASED', lease_owner=$1, lease_token=$2,
      lease_expires_at=now()+($3*interval '1 millisecond'), attempt=attempt+1, updated_at=now()
      FROM candidate WHERE occurrence.id=candidate.id RETURNING occurrence.*`, [input.leaseOwner, leaseToken, input.leaseDurationMs]);
    const row = result.rows[0];
    if (!row) return undefined;
    const trigger = await this.getTrigger(row.tenant_id, row.trigger_id);
    if (!trigger || trigger.type !== "schedule.cron") throw new Error("Claimed schedule trigger is unavailable");
    return { id: row.id, tenantId: row.tenant_id, triggerId: row.trigger_id, scheduledAt: row.scheduled_at.toISOString(),
      leaseOwner: row.lease_owner!, leaseToken: row.lease_token!, attempt: row.attempt, config: trigger.config };
  }

  async completeScheduleOccurrence(input: { id: string; leaseOwner: string; leaseToken: string; completedAt: string }): Promise<boolean> {
    const result = await this.pool.query(`UPDATE dispatch_schedule_occurrences SET state='SUCCEEDED', completed_at=$4,
      lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL, last_error=NULL, updated_at=$4
      WHERE id=$1 AND state='LEASED' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at>$4`,
    [input.id, input.leaseOwner, input.leaseToken, new Date(input.completedAt)]);
    return result.rowCount === 1;
  }

  async failScheduleOccurrence(input: { id: string; leaseOwner: string; leaseToken: string; error: string; failedAt: string; retryAt: string; maxAttempts: number }): Promise<boolean> {
    const result = await this.pool.query(`UPDATE dispatch_schedule_occurrences SET
      state=CASE WHEN attempt >= $7 THEN 'DEAD_LETTERED' ELSE 'RETRY_WAIT' END, available_at=$6, last_error=$5,
      lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL, updated_at=$4
      WHERE id=$1 AND state='LEASED' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at>$4`,
    [input.id, input.leaseOwner, input.leaseToken, new Date(input.failedAt), input.error, new Date(input.retryAt), input.maxAttempts]);
    return result.rowCount === 1;
  }

  async getTrigger(tenantId: string, triggerId: string): Promise<Trigger | undefined> {
    const result = await this.pool.query<TriggerSqlRow>("SELECT * FROM dispatch_triggers WHERE tenant_id = $1 AND id = $2", [tenantId, triggerId]);
    return result.rows[0] ? triggerFromRow(result.rows[0]) : undefined;
  }

  async disableTrigger(tenantId: string, triggerId: string, disabledAt: string): Promise<Trigger | undefined> {
    const result = await this.pool.query<TriggerSqlRow>(`UPDATE dispatch_triggers
      SET enabled = false, disabled_at = COALESCE(disabled_at, $3)
      WHERE tenant_id = $1 AND id = $2 RETURNING *`, [tenantId, triggerId, new Date(disabledAt)]);
    return result.rows[0] ? triggerFromRow(result.rows[0]) : undefined;
  }

  async publishBindingVersion(value: PublishedBindingVersion): Promise<PublishedBindingVersion> {
    const binding = publishedBindingVersionSchema.parse(value);
    const definition = binding.definition;
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [triggerControlLock(binding.tenantId, binding.triggerId)]);
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [`binding:${binding.tenantId}:${binding.bindingId}`]);
      const trigger = await client.query("SELECT id FROM dispatch_triggers WHERE tenant_id = $1 AND id = $2", [binding.tenantId, binding.triggerId]);
      if (trigger.rowCount !== 1) throw new TriggerNotFoundError(binding.triggerId);
      const profile = await client.query<{ id: string }>(
        "SELECT id FROM dispatch_agent_profile_versions WHERE tenant_id = $1 AND profile_id = $2 AND version = $3",
        [binding.tenantId, binding.profile.id, binding.profile.version],
      );
      if (!profile.rows[0]) throw new ProfileVersionNotFoundError(binding.profile.id, binding.profile.version);
      const publishedAt = new Date(binding.createdAt);
      await client.query(
        "UPDATE dispatch_binding_versions SET enabled = false, disabled_at = $3 WHERE tenant_id = $1 AND binding_id = $2 AND enabled",
        [binding.tenantId, binding.bindingId, publishedAt],
      );
      const inserted = await client.query<{ id: string }>({
        text: `INSERT INTO dispatch_binding_versions
          (id, tenant_id, binding_id, version, trigger_id, profile_version_id, definition, event_types, enabled, disabled_at, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, true, NULL, $9)
          ON CONFLICT (tenant_id, binding_id, version) DO NOTHING RETURNING id`,
        values: [binding.id, binding.tenantId, binding.bindingId, binding.version, binding.triggerId, profile.rows[0].id,
          JSON.stringify({
            filter: definition.filter,
            schemaVersion: definition.schemaVersion,
            ...("disposition" in definition
              ? { disposition: definition.disposition, wake: definition.wake }
              : { workspace: definition.workspace, prompt: definition.prompt, afterTurn: definition.afterTurn, activeSingleton: definition.activeSingleton, checkpoint: definition.checkpoint, requireGitHubPullRequestEffect: definition.requireGitHubPullRequestEffect }),
          }), definition.eventTypes, publishedAt],
      });
      if (inserted.rowCount !== 1) throw new BindingVersionAlreadyExistsError(binding.bindingId, binding.version);
      const persisted = await client.query<BindingRow>(BINDING_SELECT + `
        WHERE binding.tenant_id = $1 AND binding.id = $2`, [binding.tenantId, inserted.rows[0]!.id]);
      await client.query("COMMIT");
      return bindingFromRow(persisted.rows[0]!);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async getBindingVersion(tenantId: string, bindingId: string, version: number): Promise<PublishedBindingVersion | undefined> {
    const result = await this.pool.query<BindingRow>(BINDING_SELECT + `
      WHERE binding.tenant_id = $1 AND binding.binding_id = $2 AND binding.version = $3`, [tenantId, bindingId, version]);
    return result.rows[0] ? bindingFromRow(result.rows[0]) : undefined;
  }

  async disableBindingVersion(tenantId: string, bindingId: string, version: number, disabledAt: string): Promise<PublishedBindingVersion | undefined> {
    const result = await this.pool.query<{ id: string }>(`UPDATE dispatch_binding_versions
      SET enabled = false, disabled_at = COALESCE(disabled_at, $4)
      WHERE tenant_id = $1 AND binding_id = $2 AND version = $3 RETURNING id`, [tenantId, bindingId, version, new Date(disabledAt)]);
    return result.rows[0] ? this.getBindingVersion(tenantId, bindingId, version) : undefined;
  }

  async listBindingCandidates(tenantId: string, triggerId: string, eventType: string): Promise<readonly PublishedBindingVersion[]> {
    const result = await this.pool.query<BindingRow>(BINDING_SELECT + `
      WHERE binding.tenant_id = $1 AND binding.trigger_id = $2 AND binding.enabled AND $3 = ANY(binding.event_types)
      ORDER BY binding.binding_id, binding.version, binding.id`, [tenantId, triggerId, eventType]);
    return result.rows.map(bindingFromRow);
  }

  async admitEvent(command: AdmissionCommand | RevisionAwareAdmissionCommand): Promise<AdmissionResult> {
    const admissionHash = hashCanonicalJson({ schemaVersion: 1, triggerId: command.triggerId, event: command.event } as JsonValue);
    if (command.admissionHash !== admissionHash) throw new IdempotencyConflictError();

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [triggerControlLock(command.tenantId, command.triggerId)]);
      const lockKeys = [
        `event:${command.tenantId}:${command.triggerId}:${command.event.source}:${command.event.id}`,
        `dedup:${command.tenantId}:${command.triggerId}:${command.sourceDeduplicationKey}`,
      ].sort();
      for (const key of lockKeys) await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [key]);

      const existing = await client.query<EventRow>(`SELECT * FROM dispatch_events WHERE tenant_id = $1 AND trigger_id = $2
        AND ((source = $3 AND event_id = $4) OR source_deduplication_key = $5) FOR UPDATE`,
      [command.tenantId, command.triggerId, command.event.source, command.event.id, command.sourceDeduplicationKey]);
      if (existing.rows.length > 0) {
        if (existing.rows.length !== 1 || existing.rows[0]!.admission_hash !== command.admissionHash) throw new IdempotencyConflictError();
        const persisted = await loadEventExecutions(client, command.tenantId, existing.rows[0]!.id);
        const wakes = await loadEventWakes(client, command.tenantId, existing.rows[0]!.id);
        const pendingWakes = await loadEventWakeIntents(client, command.tenantId, existing.rows[0]!.id);
        await client.query("COMMIT");
        return { event: eventSummary(existing.rows[0]!), executions: persisted, wakes, pendingWakes, replayed: true };
      }

      const trigger = await client.query("SELECT id FROM dispatch_triggers WHERE tenant_id = $1 AND id = $2 AND enabled FOR SHARE",
        [command.tenantId, command.triggerId]);
       if (trigger.rowCount !== 1) throw new TriggerNotFoundError(command.triggerId);
      const candidates = await client.query<BindingProfileRow>(`SELECT binding.*, profile.definition AS profile_definition,
          profile.profile_id, profile.version AS profile_version
        FROM dispatch_binding_versions AS binding
        JOIN dispatch_agent_profile_versions AS profile ON profile.id = binding.profile_version_id AND profile.tenant_id = binding.tenant_id
        WHERE binding.tenant_id = $1 AND binding.trigger_id = $2 AND binding.enabled AND $3 = ANY(binding.event_types)
        ORDER BY binding.binding_id, binding.version, binding.id FOR SHARE OF binding, profile`,
      [command.tenantId, command.triggerId, command.event.type]);
      const revisionResolution = "revisionResolution" in command ? command.revisionResolution : undefined;
      const requiresResolution = revisionResolution !== undefined && candidates.rows.some((row) =>
        isDefaultBranchRevisionBinding(row) && matchesBinding(bindingFromRow(row), command.event));
      const extensions = eventExtensions(command.event);
      await acquireWakeContextLocks(client, command, candidates.rows);
      await client.query(`INSERT INTO dispatch_events
        (id, tenant_id, trigger_id, event_id, source, source_deduplication_key, admission_hash, type, subject, event_time,
         data_content_type, data_schema, data, extensions, raw_payload_ref, ingested_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15,$16)`, [
        command.internalEventId, command.tenantId, command.triggerId, command.event.id, command.event.source,
        command.sourceDeduplicationKey, command.admissionHash, command.event.type, command.event.subject ?? null,
        command.event.time ? new Date(command.event.time) : null, command.event.datacontenttype ?? "application/json",
        command.event.dataschema ?? null, JSON.stringify(command.event.data), JSON.stringify(extensions), null, new Date(command.admittedAt),
      ]);
      const githubIssueAcknowledgment = "githubIssueAcknowledgment" in command ? command.githubIssueAcknowledgment : undefined;
      if (githubIssueAcknowledgment) {
        await client.query(`INSERT INTO dispatch_outbox
          (id, tenant_id, topic, aggregate_type, aggregate_id, payload, available_at, created_at)
          VALUES ($1,$2,'github.issue-reaction.requested','github-issue-reaction',$3,$4::jsonb,$5,$5)`,
        [randomUUID(), command.tenantId, command.internalEventId, JSON.stringify({
          schemaVersion: 1,
          tenantId: command.tenantId,
          eventId: command.internalEventId,
          ...githubIssueAcknowledgment,
          content: "eyes",
        }), new Date(command.admittedAt)]);
      }
      if (requiresResolution) {
        await client.query(`INSERT INTO dispatch_event_revision_resolutions
          (event_id, tenant_id, provider, installation_id, repository_id, repository_full_name, clone_url, branch,
           state, available_at, created_at, updated_at)
          VALUES ($1,$2,'github',$3,$4,$5,$6,$7,'PENDING',$8,$8,$8)`, [
          command.internalEventId, command.tenantId, String(revisionResolution.installationId),
          String(revisionResolution.repositoryId), revisionResolution.repositoryFullName, revisionResolution.cloneUrl,
          revisionResolution.branch, new Date(command.admittedAt),
        ]);
      }
      const created = await this.createExecutions(
        client,
        command,
        requiresResolution ? candidates.rows.filter((row) => !isDefaultBranchRevisionBinding(row)) : candidates.rows,
      );
      await this.persistWakeOffers(client, command, candidates.rows);
      const pendingWakes = await this.admitPendingWakes(client, command, candidates.rows);
      const wakes = await this.consumeEventWaits(client, command, candidates.rows);
      await client.query("COMMIT");
      eventsAdmitted.inc({ tenant: command.tenantId, trigger_id: command.triggerId, event_type: command.event.type });
      for (const execution of created) executionsCreated.inc({ tenant: execution.tenantId, binding_id: execution.binding.id, profile_id: execution.profile.id });
      return {
        event: {
          admissionHash: command.admissionHash,
          admittedAt: command.admittedAt,
          eventId: command.event.id,
          id: command.internalEventId,
          source: command.event.source,
          sourceDeduplicationKey: command.sourceDeduplicationKey,
          tenantId: command.tenantId,
          triggerId: command.triggerId,
          type: command.event.type,
        },
        executions: created,
        wakes,
        pendingWakes,
        replayed: false,
      };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async claimRevisionResolution(input: { leaseOwner: string; leaseDurationMs: number }): Promise<ClaimedRevisionResolution | undefined> {
    const leaseToken = randomUUID();
    const result = await this.pool.query<RevisionResolutionRow>(`WITH candidate AS (
      SELECT event_id, tenant_id FROM dispatch_event_revision_resolutions
      WHERE ((state IN ('PENDING','RETRY_WAIT') AND available_at <= now()) OR (state = 'LEASED' AND lease_expires_at <= now()))
      ORDER BY available_at, created_at, event_id FOR UPDATE SKIP LOCKED LIMIT 1
    ) UPDATE dispatch_event_revision_resolutions AS resolution
      SET state = 'LEASED', lease_owner = $1, lease_token = $2,
          lease_expires_at = now() + ($3 * interval '1 millisecond'), attempt = attempt + 1, updated_at = now()
      FROM candidate WHERE resolution.event_id = candidate.event_id AND resolution.tenant_id = candidate.tenant_id
      RETURNING resolution.*`, [input.leaseOwner, leaseToken, input.leaseDurationMs]);
    const row = result.rows[0];
    return row ? revisionResolutionFromRow(row) : undefined;
  }

  async completeRevisionResolution(input: {
    eventId: string; tenantId: string; leaseOwner: string; leaseToken: string; commit: string; resolvedAt: string;
  }): Promise<AdmissionResult | undefined> {
    if (!/^[0-9a-f]{40}$/.test(input.commit)) throw new Error("Resolved revision must be a lowercase full commit SHA");
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const eventIdentity = await client.query<{ trigger_id: string }>(
        "SELECT trigger_id FROM dispatch_events WHERE id = $1 AND tenant_id = $2",
        [input.eventId, input.tenantId],
      );
      if (!eventIdentity.rows[0]) throw new Error("Revision resolution event disappeared");
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
        triggerControlLock(input.tenantId, eventIdentity.rows[0].trigger_id),
      ]);
      const resolutionResult = await client.query<RevisionResolutionRow>(`SELECT * FROM dispatch_event_revision_resolutions
        WHERE event_id = $1 AND tenant_id = $2 AND state = 'LEASED' AND lease_owner = $3 AND lease_token = $4
          AND lease_expires_at > now() FOR UPDATE`, [input.eventId, input.tenantId, input.leaseOwner, input.leaseToken]);
      const resolution = resolutionResult.rows[0];
      if (!resolution) {
        await client.query("ROLLBACK");
        return undefined;
      }
      const eventResult = await client.query<EventRow>("SELECT * FROM dispatch_events WHERE id = $1 AND tenant_id = $2 FOR UPDATE", [input.eventId, input.tenantId]);
      const eventRow = eventResult.rows[0];
      if (!eventRow) throw new Error("Revision resolution event disappeared");
      const candidates = await client.query<BindingProfileRow>(`SELECT binding.*, profile.definition AS profile_definition,
          profile.profile_id, profile.version AS profile_version
        FROM dispatch_binding_versions AS binding
        JOIN dispatch_agent_profile_versions AS profile ON profile.id = binding.profile_version_id AND profile.tenant_id = binding.tenant_id
        WHERE binding.tenant_id = $1 AND binding.trigger_id = $2 AND binding.enabled AND $3 = ANY(binding.event_types)
        ORDER BY binding.binding_id, binding.version, binding.id FOR SHARE OF binding, profile`,
      [input.tenantId, eventRow.trigger_id, eventRow.type]);
      const event = eventFromRow(eventRow);
      const data = event.data as Record<string, JsonValue>;
      const repository = data.repository;
      if (repository === null || typeof repository !== "object" || Array.isArray(repository)) throw new Error("Persisted GitHub repository data is invalid");
      const enrichedEvent = normalizedCloudEventSchema.parse({
        ...event,
        data: {
          ...data,
          repository: {
            ...repository,
            defaultBranchRevision: {
              type: "commit",
              commit: input.commit,
              ref: `refs/heads/${resolution.branch}`,
              resolvedAt: input.resolvedAt,
              source: "github-api",
            },
          },
        },
      });
      const command: AdmissionCommand = {
        tenantId: input.tenantId,
        triggerId: eventRow.trigger_id,
        internalEventId: input.eventId,
        event: enrichedEvent,
        sourceDeduplicationKey: eventRow.source_deduplication_key,
        admissionHash: eventRow.admission_hash,
        admittedAt: input.resolvedAt,
      };
      const created = await this.createExecutions(client, command, candidates.rows.filter(isDefaultBranchRevisionBinding));
      await client.query(`UPDATE dispatch_event_revision_resolutions SET state = 'SUCCEEDED', commit = $3,
        resolved_at = $4, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, last_error = NULL, updated_at = $4
        WHERE event_id = $1 AND tenant_id = $2`, [input.eventId, input.tenantId, input.commit, new Date(input.resolvedAt)]);
      await client.query("COMMIT");
      for (const execution of created) executionsCreated.inc({ tenant: execution.tenantId, binding_id: execution.binding.id, profile_id: execution.profile.id });
      return { event: eventSummary(eventRow), executions: created, wakes: await loadEventWakes(client, input.tenantId, input.eventId), pendingWakes: await loadEventWakeIntents(client, input.tenantId, input.eventId), replayed: false };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async failRevisionResolution(input: {
    eventId: string; tenantId: string; leaseOwner: string; leaseToken: string; error: string;
    failedAt: string; retryAt: string; maxAttempts: number;
  }): Promise<boolean> {
    const result = await this.pool.query(`UPDATE dispatch_event_revision_resolutions SET
      state = CASE WHEN attempt >= $7 THEN 'DEAD_LETTERED' ELSE 'RETRY_WAIT' END,
      available_at = $6, last_error = $5, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, updated_at = $4
      WHERE event_id = $1 AND tenant_id = $2 AND state = 'LEASED' AND lease_owner = $3 AND lease_token = $8
        AND lease_expires_at > $4`, [input.eventId, input.tenantId, input.leaseOwner, new Date(input.failedAt),
      input.error, new Date(input.retryAt), input.maxAttempts, input.leaseToken]);
    return result.rowCount === 1;
  }

  private async createExecutions(client: pg.PoolClient, command: AdmissionCommand, rows: BindingProfileRow[]): Promise<Execution[]> {
    const created: Execution[] = [];
    const plans = rows.flatMap((row) => {
      const binding = bindingFromRow(row);
      const planned = planExecution(binding, command);
      if (!planned || "disposition" in binding.definition) return [];
      const singleton = projectActiveSingleton(binding.definition, command.event);
      const checkpoint = projectCheckpoint(binding.definition, command.event);
      return [{ row, binding, planned, singleton: singleton ? { ...singleton, key: hashCanonicalJson(singleton.values) } : undefined,
        checkpoint: checkpoint ? { ...checkpoint, key: hashCanonicalJson(checkpoint.keyValues) } : undefined }];
    });
    const admissionLocks = [...new Set(plans.flatMap((plan) => [
      ...(plan.singleton ? [`active-execution-singleton:${command.tenantId}:${plan.singleton.name}:${plan.singleton.key}`] : []),
      ...(plan.checkpoint ? [`execution-checkpoint:${command.tenantId}:${plan.binding.bindingId}:${plan.checkpoint.name}:${plan.checkpoint.key}`] : []),
    ]))].sort();
    for (const lock of admissionLocks) {
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [lock]);
    }
    for (const { row, binding, planned, singleton, checkpoint } of plans) {
      if (singleton) {
        const owner = await client.query(`SELECT 1 FROM dispatch_executions
          WHERE tenant_id = $1 AND active_singleton_name = $2 AND active_singleton_key = $3
            AND state NOT IN ('SUCCEEDED', 'COMPLETED', 'CANCELLED', 'TIMED_OUT', 'FAILED', 'DEAD_LETTERED')
          LIMIT 1`, [command.tenantId, singleton.name, singleton.key]);
        if (owner.rowCount) continue;
      }
      let checkpointPrevious: JsonPrimitive | undefined;
      if (checkpoint) {
        const current = await client.query<{ value: JsonPrimitive }>(`SELECT value FROM dispatch_execution_checkpoints
          WHERE tenant_id=$1 AND binding_id=$2 AND checkpoint_name=$3 AND checkpoint_key_hash=$4 FOR UPDATE`,
        [command.tenantId, binding.bindingId, checkpoint.name, checkpoint.key]);
        checkpointPrevious = current.rows[0]?.value;
        if (current.rows[0] && hashCanonicalJson(current.rows[0].value) === hashCanonicalJson(checkpoint.value)) continue;
        planned.input = addCheckpointInput(planned.input, {
          name: checkpoint.name, previous: checkpointPrevious ?? null, current: checkpoint.value, initial: current.rowCount === 0,
        });
      }
      const now = new Date(command.admittedAt);
      const executionId = randomUUID();
      const execution = await client.query<ExecutionJoinedRow>({
        text: `INSERT INTO dispatch_executions
          (id, tenant_id, event_id, binding_version_id, profile_version_id, idempotency_key, request_hash,
           active_singleton_name, active_singleton_key, input,
           workspace, resolved_policy, state, timeout_at, created_at, updated_at, available_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,'QUEUED',$13,$14,$14,$14)
          RETURNING *, $15::text AS binding_id, $16::integer AS binding_version,
            $17::text AS profile_id, $18::integer AS profile_version`,
        values: [executionId, command.tenantId, command.internalEventId, row.id, row.profile_version_id,
          planned.id, command.admissionHash, singleton?.name ?? null, singleton?.key ?? null,
          JSON.stringify(planned.input), JSON.stringify(planned.workspace),
          JSON.stringify(row.profile_definition), new Date(now.getTime() + profileTimeoutSeconds(row.profile_definition) * 1_000), now,
          row.binding_id, row.version, planned.profile.id, planned.profile.version],
      });
      await client.query(`INSERT INTO dispatch_execution_transitions
        (id, tenant_id, execution_id, sequence, from_state, to_state, actor, reason, created_at) VALUES
        ($1,$2,$3,1,NULL,'RECEIVED','event-admission','event admitted',$6),
        ($4,$2,$3,2,'RECEIVED','PLANNED','event-admission','binding and profile resolved',$6),
        ($5,$2,$3,3,'PLANNED','QUEUED','event-admission','execution queued',$6)`,
      [randomUUID(), command.tenantId, executionId, randomUUID(), randomUUID(), now]);
      await client.query(`INSERT INTO dispatch_execution_inputs
        (tenant_id, execution_id, sequence, kind, event_id, input, workspace, created_at)
        VALUES ($1,$2,1,'INITIAL',$3,$4::jsonb,$5::jsonb,$6)`,
      [command.tenantId, executionId, command.internalEventId, JSON.stringify(planned.input), JSON.stringify(planned.workspace), now]);
      if (checkpoint) {
        await client.query(`INSERT INTO dispatch_execution_checkpoint_advances
          (execution_id, tenant_id, binding_id, checkpoint_name, checkpoint_key_hash, checkpoint_key_values,
           expected_previous_exists, expected_previous_value, target_value, state, created_at)
          VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8::jsonb,$9::jsonb,'PENDING',$10)`,
        [executionId, command.tenantId, binding.bindingId, checkpoint.name, checkpoint.key,
          JSON.stringify(checkpoint.keyValues), checkpointPrevious !== undefined,
          checkpointPrevious === undefined ? null : JSON.stringify(checkpointPrevious), JSON.stringify(checkpoint.value), now]);
      }
      if (!("disposition" in binding.definition) && binding.definition.afterTurn?.wait.admitWhileBusy) {
        const wait = binding.definition.afterTurn.wait;
        const correlation = resolveCorrelation(wait.correlation.filter((item): item is Extract<typeof item, { path: string }> => "path" in item), command.event.data);
        const requiredNames = wait.correlation.map((item) => item.name).sort();
        const state = Object.keys(correlation).length === requiredNames.length ? "READY" : "BUILDING";
        const contextId = randomUUID();
        await client.query(`INSERT INTO dispatch_execution_wake_contexts
          (id, tenant_id, execution_id, name, correlation, required_names, state, created_at)
          VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8)`,
        [contextId, command.tenantId, executionId, wait.name, JSON.stringify(correlation), JSON.stringify(requiredNames), state, now]);
        for (const [name, value] of Object.entries(correlation)) {
          await client.query(`INSERT INTO dispatch_execution_wake_context_values
            (context_id, tenant_id, execution_id, name, value, authority_type, authority_id, created_at)
            VALUES ($1,$2,$3,$4,$5::jsonb,'EVENT',$6,$7)`,
          [contextId, command.tenantId, executionId, name, JSON.stringify(value), command.internalEventId, now]);
        }
      }
      await client.query(`INSERT INTO dispatch_outbox
        (id, tenant_id, topic, aggregate_type, aggregate_id, payload, available_at, created_at)
        VALUES ($1,$2,'execution.requested','execution',$3,$4::jsonb,$5,$5)`,
      [randomUUID(), command.tenantId, executionId, JSON.stringify({ schemaVersion: 1, tenantId: command.tenantId, executionId }), now]);
      created.push(executionRecordFromJoined(execution.rows[0]!));
    }
    return created;
  }

  private async consumeEventWaits(
    client: pg.PoolClient,
    command: AdmissionCommand,
    rows: BindingProfileRow[],
  ): Promise<AdmissionWakeResult[]> {
    const plans: Array<{ binding: PublishedBindingVersion & { definition: WakeBindingDefinition }; correlation: Record<string, JsonPrimitive> }> = [];
    for (const row of rows) {
      const binding = bindingFromRow(row);
      if (!isWakeBinding(binding) || !matchesBinding(binding, command.event)) continue;
      const correlation = projectWakeCorrelation(binding.definition, command.event);
      if (correlation) plans.push({ binding, correlation });
    }
    if (plans.length === 0) return [];

    const selected = new Map<string, { executionId: string; waitId: string; binding: typeof plans[number]["binding"]; correlation: Record<string, JsonPrimitive> }>();
    for (const plan of plans) {
      const candidates = await client.query<{ execution_id: string; id: string }>(`SELECT wait.execution_id, wait.id
        FROM dispatch_event_waits AS wait
        JOIN dispatch_executions AS execution ON execution.id = wait.execution_id AND execution.tenant_id = wait.tenant_id
        WHERE wait.tenant_id = $1 AND wait.name = $2 AND wait.correlation = $3::jsonb
          AND wait.state = 'ACTIVE' AND wait.deadline_at > clock_timestamp() AND execution.state = 'WAITING'
        ORDER BY wait.execution_id, wait.id`, [command.tenantId, plan.binding.definition.wake.waitName, JSON.stringify(plan.correlation)]);
      for (const candidate of candidates.rows) {
        if (!selected.has(candidate.id)) selected.set(candidate.id, {
          binding: plan.binding, correlation: plan.correlation, executionId: candidate.execution_id, waitId: candidate.id,
        });
      }
    }

    const results: AdmissionWakeResult[] = [];
    const ordered = [...selected.values()].sort((left, right) => left.executionId.localeCompare(right.executionId) || left.waitId.localeCompare(right.waitId));
    for (const item of ordered) {
      const executionResult = await client.query<{ current_input_sequence: number; state: string; workspace: unknown }>(
        `SELECT execution.state, execution.current_input_sequence, current_input.workspace
          FROM dispatch_executions AS execution
          JOIN dispatch_execution_inputs AS current_input
            ON current_input.execution_id = execution.id AND current_input.sequence = execution.current_input_sequence
          WHERE execution.tenant_id = $1 AND execution.id = $2 FOR UPDATE OF execution`,
        [command.tenantId, item.executionId],
      );
      if (executionResult.rows[0]?.state !== "WAITING") continue;
      const waitResult = await client.query<{ state: string; name: string; correlation: Record<string, JsonPrimitive>; deadline_at: Date }>(`SELECT state, name, correlation, deadline_at
        FROM dispatch_event_waits WHERE tenant_id = $1 AND id = $2 AND execution_id = $3 FOR UPDATE`,
      [command.tenantId, item.waitId, item.executionId]);
      const wait = waitResult.rows[0];
      const now = (await client.query<{ now: Date }>("SELECT clock_timestamp() AS now")).rows[0]!.now;
      if (!wait || wait.state !== "ACTIVE" || wait.name !== item.binding.definition.wake.waitName
        || hashCanonicalJson(wait.correlation) !== hashCanonicalJson(item.correlation) || wait.deadline_at <= now) continue;

      const wakeId = randomUUID();
      const wakeAction = item.binding.definition.wake.action;
      const continuation = wakeAction.type === "continue";
      const cancellation = wakeAction.type === "cancel";
      const inputSequence = continuation ? executionResult.rows[0]!.current_input_sequence + 1 : null;
      const action = continuation ? "CONTINUED" : cancellation ? "CANCELLED" : "COMPLETED";
      const targetState = continuation ? "QUEUED" : cancellation ? "CANCELLED" : "COMPLETED";
      await client.query("UPDATE dispatch_event_waits SET state = 'CONSUMED', ended_at = $3 WHERE tenant_id = $1 AND id = $2 AND state = 'ACTIVE'", [command.tenantId, item.waitId, now]);
      if (continuation) {
        if (wakeAction.type !== "continue") throw new Error("Wake action changed during admission");
        const input = renderPromptInput(wakeAction.prompt, command.event);
        const workspace = wakeAction.workspace
          ? resolveWorkspace(wakeAction.workspace, command.event.data)
          : persistedWorkspace(item.executionId, executionResult.rows[0]!.workspace);
        await client.query(`INSERT INTO dispatch_execution_inputs
          (tenant_id, execution_id, sequence, kind, event_id, input, workspace, created_at)
          VALUES ($1,$2,$3,'WAKE',$4,$5::jsonb,$6::jsonb,$7)`,
        [command.tenantId, item.executionId, inputSequence, command.internalEventId, JSON.stringify(input), JSON.stringify(workspace), now]);
      }
      await client.query(`UPDATE dispatch_executions SET state = $3::text, updated_at = $4::timestamptz, available_at = $4::timestamptz,
          current_input_sequence = COALESCE($5::integer, current_input_sequence), completed_at = CASE WHEN $3::text IN ('COMPLETED', 'CANCELLED') THEN $4::timestamptz ELSE NULL END,
          timeout_at = CASE WHEN $3::text = 'QUEUED'
            THEN $4::timestamptz + (((resolved_policy->>'timeoutSeconds')::integer) * interval '1 second')
            ELSE timeout_at END
        WHERE tenant_id = $1 AND id = $2 AND state = 'WAITING'`,
      [command.tenantId, item.executionId, targetState, now, inputSequence]);
      await client.query(`INSERT INTO dispatch_event_wakes
        (id, tenant_id, event_id, event_wait_id, execution_id, binding_version_id, action, input_sequence, to_state, consumed_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [wakeId, command.tenantId, command.internalEventId, item.waitId, item.executionId, item.binding.id, action, inputSequence, targetState, now]);
      await client.query(`INSERT INTO dispatch_execution_transitions
        (id, tenant_id, execution_id, attempt, sequence, from_state, to_state, actor, reason, created_at)
        VALUES ($1,$2,$3,NULL,(SELECT COALESCE(MAX(sequence),0)+1 FROM dispatch_execution_transitions WHERE tenant_id=$2 AND execution_id=$3),
          'WAITING',$4,'event-admission',$5,$6)`,
      [randomUUID(), command.tenantId, item.executionId, targetState, cancellation ? wakeAction.reason : "matching event consumed active wait", now]);
      if (continuation) {
        await client.query(`INSERT INTO dispatch_outbox
          (id, tenant_id, topic, aggregate_type, aggregate_id, payload, available_at, created_at)
          VALUES ($1,$2,'execution.requested','event-wake',$3,$4::jsonb,$5,$5)`,
        [randomUUID(), command.tenantId, wakeId, JSON.stringify({ schemaVersion: 1, tenantId: command.tenantId, executionId: item.executionId, wakeId, inputSequence }), now]);
      }
      results.push({
        action, binding: { id: item.binding.bindingId, version: item.binding.version }, consumedAt: now.toISOString(),
        eventWaitId: item.waitId, executionId: item.executionId, id: wakeId, inputSequence, state: targetState,
      });
    }
    return results;
  }

  private async persistWakeOffers(client: pg.PoolClient, command: AdmissionCommand, rows: BindingProfileRow[]): Promise<void> {
    for (const row of rows) {
      const binding = bindingFromRow(row);
      if (!isWakeBinding(binding) || binding.definition.wake.delivery !== "active-or-coalesced" || !matchesBinding(binding, command.event)) continue;
      const correlation = projectWakeCorrelation(binding.definition, command.event);
      if (!correlation) continue;
      const action = binding.definition.wake.action;
      const input = action.type === "continue" ? renderPromptInput(action.prompt, command.event) : null;
      const workspace = action.type === "continue" && action.workspace ? resolveWorkspace(action.workspace, command.event.data) : null;
      await client.query(`INSERT INTO dispatch_event_wake_offers
        (id, tenant_id, event_id, binding_version_id, wait_name, correlation, action, input, workspace, admitted_at)
        VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8::jsonb,$9::jsonb,$10)`,
      [randomUUID(), command.tenantId, command.internalEventId, binding.id, binding.definition.wake.waitName,
        JSON.stringify(correlation), action.type === "continue" ? "CONTINUED" : "COMPLETED",
        input ? JSON.stringify(input) : null, workspace ? JSON.stringify(workspace) : null, new Date(command.admittedAt)]);
    }
  }

  private async admitPendingWakes(
    client: pg.PoolClient,
    command: AdmissionCommand,
    rows: BindingProfileRow[],
  ): Promise<PendingWakeResult[]> {
    const plans: Array<{ binding: PublishedBindingVersion & { definition: WakeBindingDefinition }; correlation: Record<string, JsonPrimitive> }> = [];
    for (const row of rows) {
      const binding = bindingFromRow(row);
      if (!isWakeBinding(binding) || binding.definition.wake.delivery !== "active-or-coalesced" || !matchesBinding(binding, command.event)) continue;
      const correlation = projectWakeCorrelation(binding.definition, command.event);
      if (correlation) plans.push({ binding, correlation });
    }
    if (plans.length === 0) return [];

    const selected = new Map<string, typeof plans[number]>();
    for (const plan of plans) {
      const candidates = await client.query<{ execution_id: string }>(`SELECT context.execution_id
        FROM dispatch_execution_wake_contexts AS context
        JOIN dispatch_executions AS execution ON execution.id = context.execution_id AND execution.tenant_id = context.tenant_id
        WHERE context.tenant_id = $1 AND context.name = $2 AND context.correlation = $3::jsonb
          AND execution.state IN ('QUEUED','PROVISIONING','RUNNING','RETRY_WAIT') AND execution.event_id <> $4
          AND NOT EXISTS (SELECT 1 FROM dispatch_event_wakes wake WHERE wake.tenant_id=$1 AND wake.event_id=$4 AND wake.execution_id=context.execution_id)
        ORDER BY context.execution_id`,
      [command.tenantId, plan.binding.definition.wake.waitName, JSON.stringify(plan.correlation), command.internalEventId]);
      for (const candidate of candidates.rows) {
        const current = selected.get(candidate.execution_id);
        if (!current || (plan.binding.definition.wake.action.type === "complete" && current.binding.definition.wake.action.type !== "complete")) {
          selected.set(candidate.execution_id, plan);
        }
      }
    }

    const results: PendingWakeResult[] = [];
    for (const executionId of [...selected.keys()].sort()) {
      const plan = selected.get(executionId)!;
      const executionResult = await client.query<{ state: string; workspace: unknown }>(`SELECT execution.state, input.workspace
        FROM dispatch_executions execution
        JOIN dispatch_execution_inputs input ON input.execution_id=execution.id AND input.sequence=execution.current_input_sequence
        WHERE execution.tenant_id=$1 AND execution.id=$2 FOR UPDATE OF execution`, [command.tenantId, executionId]);
      const execution = executionResult.rows[0];
      if (!execution || !["QUEUED", "PROVISIONING", "RUNNING", "RETRY_WAIT"].includes(execution.state)) continue;
      const existing = await client.query<{ intent_id: string; action: string }>(`SELECT pending.intent_id, intent.action
        FROM dispatch_execution_pending_wakes pending JOIN dispatch_event_wake_intents intent ON intent.id=pending.intent_id
        WHERE pending.tenant_id=$1 AND pending.execution_id=$2`, [command.tenantId, executionId]);
      const action = plan.binding.definition.wake.action;
      const dominated = existing.rows[0]?.action === "COMPLETED" && action.type === "continue";
      const intentId = randomUUID();
      const rendered = action.type === "continue" ? renderPromptInput(action.prompt, command.event) : null;
      const workspace = action.type === "continue"
        ? action.workspace ? resolveWorkspace(action.workspace, command.event.data) : persistedWorkspace(executionId, execution.workspace)
        : null;
      const actionName = action.type === "continue" ? "CONTINUED" : "COMPLETED";
      const disposition = dominated ? "DOMINATED" : "PENDING";
      await client.query(`INSERT INTO dispatch_event_wake_intents
        (id, tenant_id, event_id, execution_id, binding_version_id, action, disposition, input, workspace, admitted_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10)`,
      [intentId, command.tenantId, command.internalEventId, executionId, plan.binding.id, actionName, disposition,
        rendered ? JSON.stringify(rendered) : null, workspace ? JSON.stringify(workspace) : null, new Date(command.admittedAt)]);
      if (!dominated) {
        await client.query(`INSERT INTO dispatch_execution_pending_wakes (tenant_id, execution_id, intent_id, updated_at)
          VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id, execution_id) DO UPDATE SET intent_id=EXCLUDED.intent_id, updated_at=EXCLUDED.updated_at`,
        [command.tenantId, executionId, intentId, new Date(command.admittedAt)]);
      }
      results.push({ id: intentId, executionId, binding: { id: plan.binding.bindingId, version: plan.binding.version }, action: actionName, disposition, admittedAt: command.admittedAt });
    }
    return results;
  }

  async bindExecutionWakeContextValue(command: {
    authorityId: string;
    authorityType: string;
    boundAt: string;
    executionId: string;
    slot: string;
    tenantId: string;
    value: JsonPrimitive;
    waitName: string;
  }): Promise<{ correlation: Record<string, JsonPrimitive>; ready: boolean }> {
    if (Buffer.byteLength(JSON.stringify(command.value), "utf8") > 1_024) throw new Error("Supplied correlation value is too large");
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const initial = await client.query<{ binding_version_id: string; correlation: Record<string, JsonPrimitive> }>(`SELECT execution.binding_version_id, context.correlation
        FROM dispatch_executions execution JOIN dispatch_execution_wake_contexts context
          ON context.execution_id=execution.id AND context.tenant_id=execution.tenant_id
        WHERE execution.tenant_id=$1 AND execution.id=$2 AND context.name=$3`, [command.tenantId, command.executionId, command.waitName]);
      if (!initial.rows[0]) throw new Error("Execution wake context not found");
      const bindingResult = await client.query<BindingRow>(BINDING_SELECT + " WHERE binding.tenant_id=$1 AND binding.id=$2", [command.tenantId, initial.rows[0].binding_version_id]);
      const binding = bindingResult.rows[0] ? bindingFromRow(bindingResult.rows[0]) : undefined;
      if (!binding || "disposition" in binding.definition) throw new Error("Execution create binding not found");
      const waitPolicy = binding.definition.afterTurn?.wait;
      const supplied = waitPolicy?.correlation.find((item) => "slot" in item && item.slot === command.slot);
      if (!supplied || waitPolicy?.name !== command.waitName) {
        throw new Error("Supplied correlation slot is not declared by the execution binding");
      }
      const prospective = { ...initial.rows[0].correlation, [supplied.name]: command.value };
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [wakeContextLock(command.tenantId, command.waitName, prospective)]);
      await client.query("SELECT id FROM dispatch_executions WHERE tenant_id=$1 AND id=$2 FOR UPDATE", [command.tenantId, command.executionId]);
      const contextResult = await client.query<{ id: string; correlation: Record<string, JsonPrimitive>; required_names: string[]; state: string }>(
        "SELECT id, correlation, required_names, state FROM dispatch_execution_wake_contexts WHERE tenant_id=$1 AND execution_id=$2 AND name=$3 FOR UPDATE",
        [command.tenantId, command.executionId, command.waitName],
      );
      const context = contextResult.rows[0];
      if (!context) throw new Error("Execution wake context not found");
      const existing = await client.query<{ value: JsonPrimitive; authority_id: string | null; authority_type: string }>(
        "SELECT value, authority_id, authority_type FROM dispatch_execution_wake_context_values WHERE context_id=$1 AND name=$2",
        [context.id, supplied.name],
      );
      let inserted = false;
      if (existing.rows[0]) {
        if (hashCanonicalJson(existing.rows[0].value) !== hashCanonicalJson(command.value)
          || existing.rows[0].authority_id !== command.authorityId || existing.rows[0].authority_type !== command.authorityType) {
          throw new IdempotencyConflictError();
        }
      } else {
        inserted = true;
        await client.query(`INSERT INTO dispatch_execution_wake_context_values
          (context_id, tenant_id, execution_id, name, value, authority_type, authority_id, created_at)
          VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8)`,
        [context.id, command.tenantId, command.executionId, supplied.name, JSON.stringify(command.value), command.authorityType, command.authorityId, new Date(command.boundAt)]);
      }
      const correlation = { ...context.correlation, [supplied.name]: command.value };
      const ready = context.required_names.every((name) => Object.hasOwn(correlation, name));
      await client.query("UPDATE dispatch_execution_wake_contexts SET correlation=$3::jsonb, state=$4 WHERE tenant_id=$1 AND id=$2",
        [command.tenantId, context.id, JSON.stringify(correlation), ready ? "READY" : "BUILDING"]);
      if (ready && inserted) await this.activateContextAndOffers(client, command.tenantId, command.executionId, command.waitName, correlation, new Date(command.boundAt));
      await client.query("COMMIT");
      return { correlation, ready };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally { client.release(); }
  }

  async registerGitHubPullRequestEffect(command: {
    baseRef: string; executionId: string; fencingToken: string; headRef: string; pullRequestTitle: string; registeredAt: string; repositoryFullName: string;
    repositoryId: number; requestHash: string; tenantId: string;
  }): Promise<{ created: boolean; id: string; state: string }> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const execution = await client.query<{ data: JsonValue; state: string }>(`SELECT event.data, execution.state
        FROM dispatch_executions execution
        JOIN dispatch_execution_attempts attempt ON attempt.execution_id=execution.id AND attempt.tenant_id=execution.tenant_id
        JOIN dispatch_events event ON event.id=execution.event_id AND event.tenant_id=execution.tenant_id
        WHERE execution.tenant_id=$1 AND execution.id=$2 AND attempt.fencing_token=$3
          AND attempt.state IN ('LEASED','RUNNING') AND attempt.lease_expires_at > clock_timestamp()
        FOR UPDATE OF execution`, [command.tenantId, command.executionId, command.fencingToken]);
      if (!execution.rows[0]) throw new Error("Execution effect capability is not current");
      const repository = jsonObject(jsonObject(execution.rows[0].data, "event data").repository, "event repository");
      if (repository.id !== command.repositoryId || typeof repository.fullName !== "string"
        || repository.fullName.toLowerCase() !== command.repositoryFullName.toLowerCase()) throw new Error("Effect repository does not match execution origin");
      const existing = await client.query<{ id: string; request_hash: string; state: string }>(
        "SELECT id,request_hash,state FROM dispatch_github_pull_request_effects WHERE tenant_id=$1 AND execution_id=$2 AND state IN ('REGISTERED','REPORTED','CONFIRMED') FOR UPDATE",
        [command.tenantId, command.executionId],
      );
      if (existing.rows[0]) {
        if (existing.rows[0].request_hash !== command.requestHash) throw new IdempotencyConflictError();
        await client.query("COMMIT");
        return { created: false, id: existing.rows[0].id, state: existing.rows[0].state };
      }
      const id = randomUUID();
      await client.query(`INSERT INTO dispatch_github_pull_request_effects
        (id,tenant_id,execution_id,repository_id,repository_full_name,request_hash,fence_hash,pull_request_title,head_ref,base_ref,state,created_at,attempted_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'REGISTERED',$11,$11)`,
      [id, command.tenantId, command.executionId, String(command.repositoryId), command.repositoryFullName, command.requestHash, hashCanonicalJson(command.fencingToken),
        command.pullRequestTitle, command.headRef, command.baseRef, new Date(command.registeredAt)]);
      await client.query("COMMIT");
      githubEffects.inc({ tenant: command.tenantId, effect_type: "pull_request", result: "registered" });
      return { created: true, id, state: "REGISTERED" };
    } catch (error) { await client.query("ROLLBACK"); throw error; } finally { client.release(); }
  }

  async reportGitHubPullRequestEffect(command: {
    effectId: string; executionId: string; fencingToken: string; githubPullRequestId: string;
    pullRequestNumber: number; pullRequestUrl: string; reportedAt: string; tenantId: string;
  }): Promise<{ id: string; state: string }> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const effect = await client.query<{ fence_hash: string; github_pull_request_id: string | null; pull_request_number: number | null; pull_request_url: string | null; repository_full_name: string; state: string }>(`SELECT effect.*
        FROM dispatch_github_pull_request_effects effect
        WHERE effect.tenant_id=$1 AND effect.id=$2 AND effect.execution_id=$3 FOR UPDATE`,
      [command.tenantId, command.effectId, command.executionId]);
      const row = effect.rows[0];
      if (!row || row.fence_hash !== hashCanonicalJson(command.fencingToken)) throw new Error("Execution effect capability is invalid");
      validateGitHubPullRequestUrl(command.pullRequestUrl, row.repository_full_name, command.pullRequestNumber);
      if (row.state !== "REGISTERED") {
        if (row.github_pull_request_id !== command.githubPullRequestId || row.pull_request_number !== command.pullRequestNumber || row.pull_request_url !== command.pullRequestUrl) throw new IdempotencyConflictError();
        await client.query("COMMIT");
        if (row.state === "REPORTED") await this.reconcileGitHubPullRequestEffects(command.tenantId, { repositoryId: undefined, githubPullRequestId: command.githubPullRequestId, pullRequestNumber: command.pullRequestNumber });
        return { id: command.effectId, state: row.state };
      }
      await client.query(`UPDATE dispatch_github_pull_request_effects SET state='REPORTED',github_pull_request_id=$3,
        pull_request_number=$4,pull_request_url=$5,attempted_at=$6 WHERE tenant_id=$1 AND id=$2`,
      [command.tenantId, command.effectId, command.githubPullRequestId, command.pullRequestNumber, command.pullRequestUrl, new Date(command.reportedAt)]);
      await client.query("COMMIT");
      githubEffects.inc({ tenant: command.tenantId, effect_type: "pull_request", result: "reported" });
      await this.reconcileGitHubPullRequestEffects(command.tenantId, { repositoryId: undefined, githubPullRequestId: command.githubPullRequestId, pullRequestNumber: command.pullRequestNumber });
      return { id: command.effectId, state: "REPORTED" };
    } catch (error) { await client.query("ROLLBACK"); throw error; } finally { client.release(); }
  }

  async reconcileGitHubPullRequestEffects(tenantId: string, identity?: { repositoryId: number | undefined; githubPullRequestId: string; pullRequestNumber: number }): Promise<void> {
    const candidates = await this.pool.query<{ base_ref: string; execution_id: string; github_pull_request_id: string | null; head_ref: string; id: string; pull_request_number: number | null; pull_request_title: string; repository_full_name: string; repository_id: string; state: string }>(
      `SELECT id,execution_id,repository_id,repository_full_name,pull_request_title,head_ref,base_ref,state,github_pull_request_id,pull_request_number
       FROM dispatch_github_pull_request_effects WHERE tenant_id=$1 AND (
         (state='REPORTED' AND ($2::text IS NULL OR github_pull_request_id=$2) AND ($3::integer IS NULL OR pull_request_number=$3) AND ($4::text IS NULL OR repository_id=$4))
         OR (state='REGISTERED' AND $4::text IS NOT NULL AND repository_id=$4)
       ) ORDER BY created_at,id`,
      [tenantId, identity?.githubPullRequestId ?? null, identity?.pullRequestNumber ?? null, identity?.repositoryId === undefined ? null : String(identity.repositoryId)],
    );
    const matches: Array<{ candidate: typeof candidates.rows[number]; event: { id: string; pr_id: string; pr_number: number } }> = [];
    for (const candidate of candidates.rows) {
      const expectedPullRequestID = candidate.github_pull_request_id ?? identity?.githubPullRequestId;
      const expectedPullRequestNumber = candidate.pull_request_number ?? identity?.pullRequestNumber;
      if (!expectedPullRequestID || !expectedPullRequestNumber) continue;
      const event = await this.pool.query<{ id: string; pr_id: string; pr_number: number }>(`SELECT id,data->'pullRequest'->>'id' AS pr_id,(data->'pullRequest'->>'number')::integer AS pr_number
        FROM dispatch_events WHERE tenant_id=$1 AND type='com.github.pull_request.opened' AND data->'repository'->>'id'=$2
          AND data->'pullRequest'->>'id'=$3 AND (data->'pullRequest'->>'number')::integer=$4
          AND data->'pullRequest'->>'title'=$5 AND data->'pullRequest'->'head'->>'ref'=$6 AND data->'pullRequest'->'base'->>'ref'=$7
        ORDER BY ingested_at,id LIMIT 1`,
      [tenantId, candidate.repository_id, expectedPullRequestID, expectedPullRequestNumber, candidate.pull_request_title, candidate.head_ref, candidate.base_ref]);
      if (!event.rows[0]) continue;
      matches.push({ candidate, event: event.rows[0] });
    }
    const registeredMatches = matches.filter(({ candidate }) => candidate.state === "REGISTERED");
    for (const { candidate, event } of matches) {
      if (candidate.state === "REGISTERED" && registeredMatches.length !== 1) continue;
      const pullRequestNumber = candidate.pull_request_number ?? event.pr_number;
      const githubPullRequestID = candidate.github_pull_request_id ?? event.pr_id;
      const pullRequestURL = `https://github.com/${candidate.repository_full_name}/pull/${pullRequestNumber}`;
      const supplied = await this.pool.query<{ value: JsonPrimitive }>(`SELECT value FROM dispatch_execution_wake_context_values
        WHERE tenant_id=$1 AND execution_id=$2 AND authority_type='github.pull-request-effect' AND authority_id=$3 AND name='pullRequestNumber'`,
      [tenantId, candidate.execution_id, candidate.id]);
      if (!supplied.rows[0]) {
        await this.bindExecutionWakeContextValue({ authorityId: candidate.id, authorityType: "github.pull-request-effect", boundAt: new Date().toISOString(),
          executionId: candidate.execution_id, slot: "primaryPullRequestNumber", tenantId, value: pullRequestNumber, waitName: "developer-pr-lifecycle" });
      } else if (supplied.rows[0].value !== pullRequestNumber) throw new IdempotencyConflictError();
      await this.pool.query(`UPDATE dispatch_github_pull_request_effects SET state='CONFIRMED',github_pull_request_id=$4,pull_request_number=$5,pull_request_url=$6,
        opened_event_id=$3,confirmed_at=clock_timestamp() WHERE tenant_id=$1 AND id=$2 AND state IN ('REGISTERED','REPORTED')`,
      [tenantId, candidate.id, event.id, githubPullRequestID, pullRequestNumber, pullRequestURL]);
    }
  }

  private async activateContextAndOffers(
    client: pg.PoolClient,
    tenantId: string,
    executionId: string,
    waitName: string,
    correlation: Record<string, JsonPrimitive>,
    now: Date,
  ): Promise<void> {
    const executionResult = await client.query<{ current_input_sequence: number; state: string }>(
      "SELECT state, current_input_sequence FROM dispatch_executions WHERE tenant_id=$1 AND id=$2",
      [tenantId, executionId],
    );
    const execution = executionResult.rows[0];
    if (!execution || !["QUEUED", "PROVISIONING", "RUNNING", "RETRY_WAIT", "WAITING"].includes(execution.state)) return;
    const offers = await client.query<{
      action: "CONTINUED" | "COMPLETED"; admitted_at: Date; binding_version_id: string; event_id: string; id: string; input: unknown; workspace: unknown;
    }>(`SELECT id, event_id, binding_version_id, action, input, workspace, admitted_at FROM dispatch_event_wake_offers
      WHERE tenant_id=$1 AND wait_name=$2 AND correlation=$3::jsonb
        AND NOT EXISTS (SELECT 1 FROM dispatch_event_wake_intents intent WHERE intent.offer_id=dispatch_event_wake_offers.id AND intent.execution_id=$4)
        AND NOT EXISTS (SELECT 1 FROM dispatch_event_wakes wake WHERE wake.offer_id=dispatch_event_wake_offers.id AND wake.execution_id=$4)
        AND dispatch_event_wake_offers.event_id <> (SELECT event_id FROM dispatch_executions WHERE tenant_id=$1 AND id=$4)
      ORDER BY admitted_at, id`, [tenantId, waitName, JSON.stringify(correlation), executionId]);
    let selected: typeof offers.rows[number] | undefined;
    for (const offer of offers.rows) {
      if (!selected || offer.action === "COMPLETED" || selected.action !== "COMPLETED") selected = offer;
    }
    if (!selected) {
      if (execution.state === "WAITING") {
        await client.query(`UPDATE dispatch_event_waits SET state='ACTIVE', correlation=$4::jsonb
          WHERE tenant_id=$1 AND execution_id=$2 AND name=$3 AND state='PENDING_CONTEXT' AND deadline_at > clock_timestamp()`,
        [tenantId, executionId, waitName, JSON.stringify(correlation)]);
      }
      return;
    }
    if (execution.state === "WAITING") {
      const wait = await client.query<{ id: string }>(`UPDATE dispatch_event_waits SET state='CONSUMED', correlation=$4::jsonb, ended_at=$5
        WHERE tenant_id=$1 AND execution_id=$2 AND name=$3 AND state='PENDING_CONTEXT' AND deadline_at > clock_timestamp() RETURNING id`,
      [tenantId, executionId, waitName, JSON.stringify(correlation), now]);
      if (!wait.rows[0]) return;
      const continuation = selected.action === "CONTINUED";
      const sequence = continuation ? execution.current_input_sequence + 1 : null;
      const inheritedWorkspace = continuation && selected.workspace === null
        ? (await client.query<{ workspace: unknown }>("SELECT workspace FROM dispatch_execution_inputs WHERE execution_id=$1 AND sequence=$2", [executionId, execution.current_input_sequence])).rows[0]!.workspace
        : selected.workspace;
      if (continuation) await client.query(`INSERT INTO dispatch_execution_inputs
        (tenant_id, execution_id, sequence, kind, event_id, input, workspace, created_at) VALUES ($1,$2,$3,'WAKE',$4,$5::jsonb,$6::jsonb,$7)`,
      [tenantId, executionId, sequence, selected.event_id, JSON.stringify(selected.input), JSON.stringify(inheritedWorkspace), now]);
      const target = continuation ? "QUEUED" : "COMPLETED";
      await client.query(`UPDATE dispatch_executions SET state=$3, current_input_sequence=COALESCE($4,current_input_sequence), available_at=$5,
        updated_at=$5, completed_at=CASE WHEN $3='COMPLETED' THEN $5 ELSE NULL END,
        timeout_at=CASE WHEN $3='QUEUED' THEN $5 + (((resolved_policy->>'timeoutSeconds')::integer)*interval '1 second') ELSE timeout_at END
        WHERE tenant_id=$1 AND id=$2`, [tenantId, executionId, target, sequence, now]);
      const wakeId = randomUUID();
      await client.query(`INSERT INTO dispatch_event_wakes
        (id, tenant_id, event_id, event_wait_id, offer_id, execution_id, binding_version_id, action, input_sequence, to_state, consumed_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [wakeId, tenantId, selected.event_id, wait.rows[0].id, selected.id, executionId, selected.binding_version_id, selected.action, sequence, target, now]);
      await client.query(`INSERT INTO dispatch_execution_transitions
        (id,tenant_id,execution_id,attempt,sequence,from_state,to_state,actor,reason,created_at)
        VALUES ($1,$2,$3,NULL,(SELECT COALESCE(MAX(sequence),0)+1 FROM dispatch_execution_transitions WHERE tenant_id=$2 AND execution_id=$3),'WAITING',$4,'context-binding','deferred event consumed completed correlation',$5)`,
      [randomUUID(), tenantId, executionId, target, now]);
      if (continuation) await client.query(`INSERT INTO dispatch_outbox
        (id, tenant_id, topic, aggregate_type, aggregate_id, payload, available_at, created_at)
        VALUES ($1,$2,'execution.requested','event-wake',$3,$4::jsonb,$5,$5)`,
      [randomUUID(), tenantId, wakeId, JSON.stringify({ schemaVersion: 1, tenantId, executionId, wakeId, inputSequence: sequence }), now]);
      for (const offer of offers.rows.filter((offer) => offer.id !== selected!.id)) {
        await client.query(`INSERT INTO dispatch_event_wake_intents
          (id,tenant_id,event_id,execution_id,binding_version_id,offer_id,action,disposition,input,workspace,admitted_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,'DOMINATED',$8::jsonb,$9::jsonb,$10)`,
        [randomUUID(), tenantId, offer.event_id, executionId, offer.binding_version_id, offer.id, offer.action,
          offer.input ? JSON.stringify(offer.input) : null,
          offer.action === "CONTINUED" ? JSON.stringify(offer.workspace ?? inheritedWorkspace) : null, offer.admitted_at]);
      }
      return;
    }
    const existing = await client.query<{ action: string }>(`SELECT intent.action FROM dispatch_execution_pending_wakes pending
      JOIN dispatch_event_wake_intents intent ON intent.id=pending.intent_id WHERE pending.tenant_id=$1 AND pending.execution_id=$2`, [tenantId, executionId]);
    const selectedDominated = existing.rows[0]?.action === "COMPLETED" && selected.action === "CONTINUED";
    let selectedIntentId: string | undefined;
    const currentWorkspace = (await client.query<{ workspace: unknown }>("SELECT workspace FROM dispatch_execution_inputs WHERE execution_id=$1 AND sequence=$2", [executionId, execution.current_input_sequence])).rows[0]!.workspace;
    for (const offer of offers.rows) {
      const isSelected = offer.id === selected.id;
      const intentId = randomUUID();
      if (isSelected) selectedIntentId = intentId;
      await client.query(`INSERT INTO dispatch_event_wake_intents
        (id,tenant_id,event_id,execution_id,binding_version_id,offer_id,action,disposition,input,workspace,admitted_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11)`,
      [intentId, tenantId, offer.event_id, executionId, offer.binding_version_id, offer.id, offer.action,
        isSelected && !selectedDominated ? "PENDING" : "DOMINATED", offer.input ? JSON.stringify(offer.input) : null,
        offer.action === "CONTINUED" ? JSON.stringify(offer.workspace ?? currentWorkspace) : null, offer.admitted_at]);
    }
    if (!selectedDominated && selectedIntentId) await client.query(`INSERT INTO dispatch_execution_pending_wakes (tenant_id,execution_id,intent_id,updated_at)
      VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id,execution_id) DO UPDATE SET intent_id=EXCLUDED.intent_id, updated_at=EXCLUDED.updated_at`,
    [tenantId, executionId, selectedIntentId, now]);
  }

  async getExecution(tenantId: string, executionId: string): Promise<Execution | undefined> {
    const result = await this.pool.query<ExecutionJoinedRow>(EXECUTION_SELECT + " WHERE execution.tenant_id = $1 AND execution.id = $2", [tenantId, executionId]);
    return result.rows[0] ? executionRecordFromJoined(result.rows[0]) : undefined;
  }

  async getExecutionDetail(tenantId: string, executionId: string): Promise<ExecutionDetail | undefined> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
      const executionResult = await client.query<ExecutionJoinedRow>(
        EXECUTION_SELECT + " WHERE execution.tenant_id = $1 AND execution.id = $2",
        [tenantId, executionId],
      );
      const row = executionResult.rows[0];
      if (!row) {
        await client.query("COMMIT");
        return undefined;
      }
      const attemptsResult = await client.query<ExecutionAttemptRow>(`
        SELECT attempt, state, started_at, finished_at, lease_expires_at, opencode_session_id, workload_name, diagnostic
        FROM dispatch_execution_attempts
        WHERE tenant_id = $1 AND execution_id = $2
        ORDER BY attempt`, [tenantId, executionId]);
      const transitionsResult = await client.query<ExecutionTransitionRow>(`
        SELECT id, attempt, sequence, from_state, to_state, actor, reason, created_at, trace_context
        FROM dispatch_execution_transitions
        WHERE tenant_id = $1 AND execution_id = $2
        ORDER BY sequence`, [tenantId, executionId]);
      const waitsResult = await client.query<EventWaitRow>(`
        SELECT id, attempt, name, state, correlation, deadline_at, activated_at, ended_at
        FROM dispatch_event_waits WHERE tenant_id = $1 AND execution_id = $2
        ORDER BY activated_at, id`, [tenantId, executionId]);
      const detail = {
        ...executionRecordFromJoined(row),
        attempts: attemptsResult.rows.map((attempt) => executionAttemptFromRow(executionId, attempt)),
        transitions: transitionsResult.rows.map((transition) => executionTransitionFromRow(executionId, transition)),
        waits: waitsResult.rows.map((wait) => eventWaitFromRow(executionId, wait)),
      };
      await client.query("COMMIT");
      return detail;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async requestExecutionCancellation(
    command: RequestExecutionCancellationCommand,
  ): Promise<RequestExecutionCancellationResult | undefined> {
    const requestedAt = new Date(command.requestedAt);
    if (Number.isNaN(requestedAt.getTime())) throw new Error("Execution cancellation request time must be valid");
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const executionResult = await client.query<{ id: string; state: string }>(
        "SELECT id, state FROM dispatch_executions WHERE tenant_id = $1 AND id = $2 FOR UPDATE",
        [command.tenantId, command.executionId],
      );
      const execution = executionResult.rows[0];
      if (!execution) {
        await client.query("COMMIT");
        return undefined;
      }
      if (!isExecutionState(execution.state)) {
        throw new PersistedExecutionCorruptionError(command.executionId, "state");
      }
      if (execution.state === "CANCELLED") {
        await client.query("COMMIT");
        return { id: execution.id, outcome: "CANCELLED", state: "CANCELLED" };
      }
      if (execution.state === "CANCEL_REQUESTED") {
        await client.query("COMMIT");
        return { id: execution.id, outcome: "REQUESTED", state: "CANCEL_REQUESTED" };
      }
      if (execution.state !== "QUEUED" && execution.state !== "RETRY_WAIT" && execution.state !== "AWAITING_APPROVAL" && execution.state !== "WAITING"
        && execution.state !== "PROVISIONING" && execution.state !== "RUNNING") {
        throw new ExecutionCancellationConflictError(command.executionId);
      }

      const immediate = execution.state === "QUEUED" || execution.state === "RETRY_WAIT"
        || execution.state === "AWAITING_APPROVAL" || execution.state === "WAITING";
      const cancellationAt = (await client.query<{ now: Date }>("SELECT clock_timestamp() AS now")).rows[0]!.now;
      if (execution.state === "WAITING") {
        const wait = await client.query("SELECT id FROM dispatch_event_waits WHERE tenant_id = $1 AND execution_id = $2 AND state IN ('ACTIVE','PENDING_CONTEXT') FOR UPDATE", [command.tenantId, command.executionId]);
        if (wait.rowCount !== 1) throw new PersistedExecutionCorruptionError(command.executionId, "active wait");
        await client.query("UPDATE dispatch_event_waits SET state = 'CANCELLED', ended_at = $3 WHERE tenant_id = $1 AND execution_id = $2 AND state IN ('ACTIVE','PENDING_CONTEXT')", [command.tenantId, command.executionId, cancellationAt]);
      }
      const transitionIds = immediate ? [command.transitionId, randomUUID()] : [command.transitionId];
      await client.query({
        text: `WITH cancellation_clock AS (SELECT $3::timestamptz AS requested_at),
          updated_execution AS (
            UPDATE dispatch_executions
            SET state = $4, updated_at = cancellation_clock.requested_at,
                completed_at = CASE WHEN $5 THEN cancellation_clock.requested_at ELSE completed_at END
            FROM cancellation_clock
            WHERE id = $1 AND tenant_id = $2 AND state = $6
            RETURNING id
          )
          INSERT INTO dispatch_execution_transitions
            (id, tenant_id, execution_id, attempt, sequence, from_state, to_state, actor, reason, created_at)
          SELECT transition.id, $2, $1, NULL,
            (SELECT COALESCE(MAX(sequence), 0) FROM dispatch_execution_transitions WHERE tenant_id = $2 AND execution_id = $1)
              + transition.ordinality::integer,
            transition.from_state, transition.to_state, $7, $8, cancellation_clock.requested_at
          FROM updated_execution, cancellation_clock,
            unnest($9::text[], $10::text[], $11::text[]) WITH ORDINALITY
              AS transition(id, from_state, to_state, ordinality)`,
        values: [command.executionId, command.tenantId, cancellationAt, immediate ? "CANCELLED" : "CANCEL_REQUESTED",
          immediate, execution.state, command.actor, command.reason, transitionIds,
          immediate ? [execution.state, "CANCEL_REQUESTED"] : [execution.state],
          immediate ? ["CANCEL_REQUESTED", "CANCELLED"] : ["CANCEL_REQUESTED"]],
      });
      await client.query("COMMIT");
      return immediate
        ? { id: execution.id, outcome: "CANCELLED", state: "CANCELLED" }
        : { id: execution.id, outcome: "REQUESTED", state: "CANCEL_REQUESTED" };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async claimNextQueuedExecution(command: { leaseOwner: string; leaseDurationMs: number }): Promise<ClaimedExecution | undefined> {
    assertPositiveInteger(command.leaseDurationMs, "Execution lease duration");

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const claimed = await client.query<ClaimedExecutionRow>({
        text: `
          WITH claim_clock AS MATERIALIZED (
            SELECT clock_timestamp() AS claimed_at
          ), candidate AS (
            SELECT execution.id, execution.tenant_id
            FROM dispatch_executions AS execution, claim_clock
            WHERE execution.state = 'QUEUED'
              AND execution.available_at <= claim_clock.claimed_at
              AND execution.timeout_at > claim_clock.claimed_at
              AND NOT EXISTS (
                SELECT 1 FROM dispatch_outbox AS required_effect
                WHERE required_effect.tenant_id = execution.tenant_id
                  AND required_effect.topic = 'github.issue-reaction.requested'
                  AND required_effect.aggregate_type = 'github-issue-reaction'
                  AND required_effect.aggregate_id = execution.event_id
                  AND required_effect.published_at IS NULL
              )
            ORDER BY execution.available_at, execution.created_at, execution.id
            FOR UPDATE OF execution SKIP LOCKED
            LIMIT 1
          ), next_attempt AS (
            SELECT candidate.id, candidate.tenant_id,
                   COALESCE(MAX(attempt.attempt), 0) + 1 AS attempt
            FROM candidate
            LEFT JOIN dispatch_execution_attempts AS attempt ON attempt.execution_id = candidate.id
            GROUP BY candidate.id, candidate.tenant_id
          ), inserted_attempt AS (
            INSERT INTO dispatch_execution_attempts
              (execution_id, tenant_id, attempt, fencing_token, state, lease_owner, lease_expires_at)
            SELECT id, tenant_id, attempt, $1, 'LEASED', $2,
                   claim_clock.claimed_at + ($3::double precision * interval '1 millisecond')
            FROM next_attempt, claim_clock
            RETURNING execution_id, tenant_id, attempt, fencing_token, state, lease_owner,
                      lease_expires_at
          ), updated_execution AS (
            UPDATE dispatch_executions AS execution
            SET state = 'PROVISIONING', updated_at = claim_clock.claimed_at
            FROM candidate, claim_clock
            WHERE execution.id = candidate.id
              AND execution.tenant_id = candidate.tenant_id
              AND execution.state = 'QUEUED'
            RETURNING execution.*
          ), inserted_transition AS (
            INSERT INTO dispatch_execution_transitions
              (id, tenant_id, execution_id, attempt, sequence, from_state, to_state, actor, reason, created_at)
            SELECT $4, execution.tenant_id, execution.id, attempt.attempt,
                   COALESCE((SELECT MAX(t.sequence) + 1
                             FROM dispatch_execution_transitions AS t
                             WHERE t.tenant_id = execution.tenant_id AND t.execution_id = execution.id), 4),
                   'QUEUED', 'PROVISIONING', $2, $5, claim_clock.claimed_at
            FROM updated_execution AS execution
            JOIN inserted_attempt AS attempt ON attempt.execution_id = execution.id,
                 claim_clock
            RETURNING execution_id
          )
          SELECT execution.id, execution.tenant_id, execution.state AS execution_state,
                  execution.event_id, current_input.input, current_input.workspace, execution.resolved_policy,
                 execution.created_at, execution.timeout_at,
                  profile.id AS profile_version_id, profile.profile_id, profile.version,
                  profile.definition AS definition,
                 attempt.attempt, attempt.fencing_token, attempt.state AS attempt_state,
                 attempt.lease_owner, attempt.lease_expires_at
          FROM updated_execution AS execution
          JOIN inserted_attempt AS attempt ON attempt.execution_id = execution.id
          JOIN inserted_transition AS transition ON transition.execution_id = execution.id
          JOIN dispatch_execution_inputs AS current_input
            ON current_input.execution_id = execution.id AND current_input.sequence = execution.current_input_sequence
          JOIN dispatch_agent_profile_versions AS profile
            ON profile.id = execution.profile_version_id AND profile.tenant_id = execution.tenant_id
        `,
        values: [randomUUID(), command.leaseOwner, command.leaseDurationMs, randomUUID(), "execution claimed"],
      });
      const result = claimed.rows[0] ? claimedExecutionFromRow(claimed.rows[0]) : undefined;
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async claimExpiredRunningExecution(command: {
    leaseOwner: string;
    leaseDurationMs: number;
  }): Promise<ClaimedExecution | undefined> {
    assertPositiveInteger(command.leaseDurationMs, "Execution lease duration");

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const claimed = await client.query<ClaimedExecutionRow>({
        text: `
          WITH claim_clock AS MATERIALIZED (
            SELECT clock_timestamp() AS claimed_at
          ), candidate_execution AS MATERIALIZED (
            SELECT execution.id, execution.tenant_id
            FROM dispatch_executions AS execution, claim_clock
            WHERE execution.state = 'RUNNING'
              AND execution.timeout_at > claim_clock.claimed_at
              AND EXISTS (
                SELECT 1
                FROM dispatch_execution_attempts AS attempt
                WHERE attempt.execution_id = execution.id
                  AND attempt.tenant_id = execution.tenant_id
                  AND attempt.state = 'RUNNING'
                  AND attempt.lease_expires_at <= claim_clock.claimed_at
                  AND attempt.workload_name IS NOT NULL
                  AND attempt.opencode_session_id IS NOT NULL
              )
            ORDER BY execution.updated_at, execution.created_at, execution.id
            FOR UPDATE OF execution SKIP LOCKED
            LIMIT 1
          ), candidate_attempt AS MATERIALIZED (
            SELECT attempt.execution_id, attempt.tenant_id, attempt.attempt
            FROM dispatch_execution_attempts AS attempt
            JOIN candidate_execution AS execution
              ON execution.id = attempt.execution_id AND execution.tenant_id = attempt.tenant_id,
                 claim_clock
            WHERE attempt.state = 'RUNNING'
              AND attempt.lease_expires_at <= claim_clock.claimed_at
              AND attempt.workload_name IS NOT NULL
              AND attempt.opencode_session_id IS NOT NULL
            ORDER BY attempt.attempt DESC
            FOR UPDATE OF attempt
            LIMIT 1
          ), updated_attempt AS (
            UPDATE dispatch_execution_attempts AS attempt
            SET fencing_token = $1, lease_owner = $2,
                lease_expires_at = claim_clock.claimed_at + ($3::double precision * interval '1 millisecond')
            FROM candidate_attempt AS candidate, claim_clock
            WHERE attempt.execution_id = candidate.execution_id
              AND attempt.tenant_id = candidate.tenant_id
              AND attempt.attempt = candidate.attempt
              AND attempt.state = 'RUNNING'
              AND attempt.lease_expires_at <= claim_clock.claimed_at
              AND attempt.workload_name IS NOT NULL
              AND attempt.opencode_session_id IS NOT NULL
            RETURNING attempt.*
          ), updated_execution AS (
            UPDATE dispatch_executions AS execution
            SET updated_at = claim_clock.claimed_at
            FROM candidate_execution AS candidate, updated_attempt AS attempt, claim_clock
            WHERE execution.id = candidate.id AND execution.tenant_id = candidate.tenant_id
              AND attempt.execution_id = execution.id AND attempt.tenant_id = execution.tenant_id
              AND execution.state = 'RUNNING'
              AND execution.timeout_at > claim_clock.claimed_at
            RETURNING execution.*
          )
          SELECT execution.id, execution.tenant_id, execution.state AS execution_state,
                  execution.event_id, current_input.input, current_input.workspace, execution.resolved_policy,
                 execution.created_at, execution.timeout_at,
                 profile.id AS profile_version_id, profile.profile_id, profile.version,
                 profile.definition AS definition,
                 attempt.attempt, attempt.fencing_token, attempt.state AS attempt_state,
                 attempt.lease_owner, attempt.lease_expires_at,
                 attempt.workload_name, attempt.opencode_session_id
          FROM updated_execution AS execution
          JOIN updated_attempt AS attempt
            ON attempt.execution_id = execution.id AND attempt.tenant_id = execution.tenant_id
          JOIN dispatch_execution_inputs AS current_input
            ON current_input.execution_id = execution.id AND current_input.sequence = execution.current_input_sequence
          JOIN dispatch_agent_profile_versions AS profile
            ON profile.id = execution.profile_version_id AND profile.tenant_id = execution.tenant_id
        `,
        values: [randomUUID(), command.leaseOwner, command.leaseDurationMs],
      });
      const result = claimed.rows[0] ? claimedExecutionFromRow(claimed.rows[0]) : undefined;
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async renewExecutionLease(command: {
    executionId: string;
    tenantId: string;
    attempt: number;
    fencingToken: string;
    leaseOwner: string;
    leaseDurationMs: number;
  }): Promise<ExecutionLeaseRenewalResult> {
    assertPositiveInteger(command.leaseDurationMs, "Execution lease duration");
    const result = await this.pool.query<{ lease_expires_at: Date }>({
      text: `
        WITH lease_clock AS MATERIALIZED (SELECT clock_timestamp() AS renewed_at)
        UPDATE dispatch_execution_attempts AS attempt
        SET lease_expires_at = GREATEST(
          attempt.lease_expires_at,
          lease_clock.renewed_at + ($6::double precision * interval '1 millisecond')
        )
        FROM dispatch_executions AS execution, lease_clock
        WHERE attempt.execution_id = $1 AND attempt.tenant_id = $2
          AND attempt.attempt = $3 AND attempt.fencing_token = $4 AND attempt.lease_owner = $5
          AND attempt.state IN ('LEASED', 'RUNNING')
          AND attempt.lease_expires_at > lease_clock.renewed_at
          AND execution.id = attempt.execution_id AND execution.tenant_id = attempt.tenant_id
          AND execution.state IN ('PROVISIONING', 'RUNNING')
        RETURNING attempt.lease_expires_at
      `,
      values: [command.executionId, command.tenantId, command.attempt, command.fencingToken, command.leaseOwner, command.leaseDurationMs],
    });
    if (result.rowCount === 1) return "RENEWED";

    const cancellation = await this.pool.query<{ cancellation_requested: boolean }>({
      text: `
        SELECT EXISTS (
          SELECT 1
          FROM dispatch_execution_attempts AS attempt
          JOIN dispatch_executions AS execution
            ON execution.id = attempt.execution_id AND execution.tenant_id = attempt.tenant_id
          WHERE attempt.execution_id = $1 AND attempt.tenant_id = $2
            AND attempt.attempt = $3 AND attempt.fencing_token = $4 AND attempt.lease_owner = $5
            AND attempt.state IN ('LEASED', 'RUNNING')
            AND attempt.lease_expires_at > clock_timestamp()
            AND execution.state = 'CANCEL_REQUESTED'
        ) AS cancellation_requested
      `,
      values: [command.executionId, command.tenantId, command.attempt, command.fencingToken, command.leaseOwner],
    });
    return cancellation.rows[0]?.cancellation_requested ? "CANCEL_REQUESTED" : "LOST";
  }

  async acknowledgeLeasedExecutionCancellation(
    command: AcknowledgeLeasedExecutionCancellationCommand,
  ): Promise<AcknowledgeLeasedExecutionCancellationResult> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const executionResult = await client.query<{ state: string }>(
        "SELECT state FROM dispatch_executions WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
        [command.executionId, command.tenantId],
      );
      const execution = executionResult.rows[0];
      if (!execution) {
        await client.query("ROLLBACK");
        return { applied: false, reason: "NOT_FOUND" };
      }
      if (execution.state !== "CANCEL_REQUESTED") {
        await client.query("ROLLBACK");
        return { applied: false, reason: "STATE_MISMATCH" };
      }

      const attemptResult = await client.query<{ lease_expires_at: Date; state: string }>({
        text: `SELECT state, lease_expires_at
          FROM dispatch_execution_attempts
          WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
            AND fencing_token = $4 AND lease_owner = $5
          FOR UPDATE`,
        values: [command.executionId, command.tenantId, command.attempt, command.fencingToken, command.leaseOwner],
      });
      const attempt = attemptResult.rows[0];
      if (!attempt) {
        const exists = await client.query<{ exists: boolean }>(
          "SELECT EXISTS (SELECT 1 FROM dispatch_execution_attempts WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3) AS exists",
          [command.executionId, command.tenantId, command.attempt],
        );
        await client.query("ROLLBACK");
        return { applied: false, reason: exists.rows[0]?.exists ? "LEASE_MISMATCH" : "NOT_FOUND" };
      }
      if (attempt.state !== "LEASED" && attempt.state !== "RUNNING") {
        await client.query("ROLLBACK");
        return { applied: false, reason: "STATE_MISMATCH" };
      }
      const clock = await client.query<{ now: Date }>("SELECT clock_timestamp() AS now");
      const acknowledgedAt = clock.rows[0]!.now;
      if (attempt.lease_expires_at <= acknowledgedAt) {
        await client.query("ROLLBACK");
        return { applied: false, reason: "LEASE_EXPIRED" };
      }

      const updated = await client.query({
        text: `WITH updated_attempt AS (
            UPDATE dispatch_execution_attempts
            SET state = 'CANCELLED', finished_at = $6, lease_owner = NULL, lease_expires_at = NULL
            WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
              AND fencing_token = $4 AND lease_owner = $5 AND state = $7
            RETURNING execution_id
          ), updated_execution AS (
            UPDATE dispatch_executions
            SET state = 'CANCELLED', completed_at = $6, updated_at = $6
            WHERE id = $1 AND tenant_id = $2 AND state = 'CANCEL_REQUESTED'
              AND EXISTS (SELECT 1 FROM updated_attempt)
            RETURNING id
          ), inserted_transition AS (
            INSERT INTO dispatch_execution_transitions
              (id, tenant_id, execution_id, attempt, sequence, from_state, to_state, actor, reason, created_at)
            SELECT $8, $2, id, $3,
              (SELECT COALESCE(MAX(sequence), 0) + 1 FROM dispatch_execution_transitions WHERE tenant_id = $2 AND execution_id = $1),
              'CANCEL_REQUESTED', 'CANCELLED', $9, $10, $6
            FROM updated_execution
            RETURNING execution_id
          )
          SELECT id FROM updated_execution WHERE EXISTS (SELECT 1 FROM inserted_transition)`,
        values: [command.executionId, command.tenantId, command.attempt, command.fencingToken, command.leaseOwner,
          acknowledgedAt, attempt.state, randomUUID(), command.actor, command.reason],
      });
      if (updated.rowCount !== 1) {
        await client.query("ROLLBACK");
        return { applied: false, reason: "STATE_MISMATCH" };
      }
      await client.query("COMMIT");
      return { applied: true };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async listRequestedCancellationCleanups(command: {
    limit: number;
  }): Promise<RequestedCancellationCleanup[]> {
    assertPositiveInteger(command.limit, "Execution cancellation cleanup limit");
    const result = await this.pool.query<{
      attempt: number | null;
      execution_id: string;
      fencing_token: string | null;
      tenant_id: string;
      workload_name: string | null;
    }>({
      text: `WITH candidates AS (
          SELECT execution.id, execution.tenant_id
          FROM dispatch_executions AS execution
          LEFT JOIN LATERAL (
            SELECT candidate.attempt, candidate.lease_expires_at
            FROM dispatch_execution_attempts AS candidate
            WHERE candidate.execution_id = execution.id AND candidate.tenant_id = execution.tenant_id
              AND candidate.state IN ('LEASED', 'RUNNING')
            ORDER BY candidate.attempt DESC
            LIMIT 1
          ) AS active_attempt ON true
          WHERE execution.state = 'CANCEL_REQUESTED'
            AND (active_attempt.attempt IS NULL OR active_attempt.lease_expires_at <= clock_timestamp())
          ORDER BY execution.updated_at, execution.created_at, execution.id
          FOR UPDATE OF execution SKIP LOCKED
          LIMIT $1
        ), rotated AS (
          UPDATE dispatch_executions AS execution
          SET updated_at = clock_timestamp()
          FROM candidates
          WHERE execution.id = candidates.id AND execution.tenant_id = candidates.tenant_id
          RETURNING execution.id, execution.tenant_id
        )
        SELECT rotated.id AS execution_id, rotated.tenant_id,
          attempt.attempt, attempt.fencing_token, attempt.workload_name
        FROM rotated
        LEFT JOIN LATERAL (
          SELECT candidate.attempt, candidate.fencing_token, candidate.lease_expires_at, candidate.workload_name
          FROM dispatch_execution_attempts AS candidate
          WHERE candidate.execution_id = rotated.id AND candidate.tenant_id = rotated.tenant_id
            AND candidate.state IN ('LEASED', 'RUNNING')
          ORDER BY candidate.attempt DESC
          LIMIT 1
        ) AS attempt ON true
        ORDER BY rotated.id`,
      values: [command.limit],
    });
    return result.rows.map((row) => {
      const candidate: RequestedCancellationCleanup = {
        attempt: row.attempt,
        executionId: row.execution_id,
        tenantId: row.tenant_id,
        workloadName: row.workload_name,
      };
      Object.defineProperty(candidate, cancellationCleanupFence, { value: row.fencing_token });
      return candidate;
    });
  }

  async finalizeRequestedExecutionCancellation(
    command: FinalizeRequestedExecutionCancellationCommand,
  ): Promise<FinalizedRequestedExecutionCancellation | undefined> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const executionResult = await client.query<{ state: string }>(
        "SELECT state FROM dispatch_executions WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
        [command.executionId, command.tenantId],
      );
      if (executionResult.rows[0]?.state !== "CANCEL_REQUESTED") {
        await client.query("ROLLBACK");
        return undefined;
      }

      const fencingToken = (command as CancellationCleanupCandidate)[cancellationCleanupFence];
      const clock = await client.query<{ now: Date }>("SELECT clock_timestamp() AS now");
      const finalizedAt = clock.rows[0]!.now;
      if (command.attempt === null) {
        const activeAttempt = await client.query(`SELECT attempt
          FROM dispatch_execution_attempts
          WHERE execution_id = $1 AND tenant_id = $2 AND state IN ('LEASED', 'RUNNING')
          ORDER BY attempt DESC
          FOR UPDATE
          LIMIT 1`, [command.executionId, command.tenantId]);
        if (activeAttempt.rowCount !== 0 || fencingToken !== null) {
          await client.query("ROLLBACK");
          return undefined;
        }
      } else {
        const attemptResult = await client.query<{ fencing_token: string; lease_expires_at: Date; state: string }>({
          text: `SELECT state, fencing_token, lease_expires_at
            FROM dispatch_execution_attempts
            WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
            FOR UPDATE`,
          values: [command.executionId, command.tenantId, command.attempt],
        });
        const attempt = attemptResult.rows[0];
        if (!attempt || fencingToken === undefined || attempt.fencing_token !== fencingToken
          || (attempt.state !== "LEASED" && attempt.state !== "RUNNING")
          || attempt.lease_expires_at > finalizedAt) {
          await client.query("ROLLBACK");
          return undefined;
        }
      }

      const result = await client.query({
        text: `WITH updated_attempt AS (
            UPDATE dispatch_execution_attempts
            SET state = 'CANCELLED', finished_at = $4, lease_owner = NULL, lease_expires_at = NULL
            WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
              AND fencing_token = $5 AND state IN ('LEASED', 'RUNNING') AND lease_expires_at <= $4
            RETURNING execution_id
          ), updated_execution AS (
            UPDATE dispatch_executions
            SET state = 'CANCELLED', completed_at = $4, updated_at = $4
            WHERE id = $1 AND tenant_id = $2 AND state = 'CANCEL_REQUESTED'
              AND ($3::integer IS NULL OR EXISTS (SELECT 1 FROM updated_attempt))
            RETURNING id
          ), inserted_transition AS (
            INSERT INTO dispatch_execution_transitions
              (id, tenant_id, execution_id, attempt, sequence, from_state, to_state, actor, reason, created_at)
            SELECT $6, $2, id, $3,
              (SELECT COALESCE(MAX(sequence), 0) + 1 FROM dispatch_execution_transitions WHERE tenant_id = $2 AND execution_id = $1),
              'CANCEL_REQUESTED', 'CANCELLED', 'execution-reconciler', 'execution cancellation finalized', $4
            FROM updated_execution
            RETURNING execution_id
          )
          SELECT id FROM updated_execution WHERE EXISTS (SELECT 1 FROM inserted_transition)`,
        values: [command.executionId, command.tenantId, command.attempt, finalizedAt, fencingToken, randomUUID()],
      });
      if (result.rowCount !== 1) {
        await client.query("ROLLBACK");
        return undefined;
      }
      await client.query("COMMIT");
      return { ...command, finalizedAt };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async recoverExpiredExecutionLeases(command: {
    limit: number;
    maxAttempts: number;
    retryDelayMs: number;
  }): Promise<RecoveredExecutionLease[]> {
    assertPositiveInteger(command.limit, "Execution lease recovery limit");
    assertPositiveInteger(command.maxAttempts, "Execution maximum attempts");
    assertNonnegativeInteger(command.retryDelayMs, "Execution retry delay");

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const candidates = await client.query<RecoveryExecutionRow>({
        text: `
          WITH recovery_clock AS MATERIALIZED (SELECT clock_timestamp() AS recovered_at)
          SELECT execution.id, execution.tenant_id, execution.state, execution.timeout_at
          FROM dispatch_executions AS execution, recovery_clock
          WHERE execution.state IN ('PROVISIONING', 'RUNNING')
            AND EXISTS (
              SELECT 1
              FROM dispatch_execution_attempts AS attempt
              WHERE attempt.execution_id = execution.id
                AND attempt.tenant_id = execution.tenant_id
                AND attempt.state IN ('LEASED', 'RUNNING')
                AND attempt.lease_expires_at <= recovery_clock.recovered_at
                AND NOT (
                  attempt.state = 'RUNNING'
                  AND attempt.workload_name IS NOT NULL
                  AND attempt.opencode_session_id IS NOT NULL
                  AND execution.timeout_at > recovery_clock.recovered_at
                )
            )
          ORDER BY execution.updated_at, execution.created_at, execution.id
          FOR UPDATE OF execution SKIP LOCKED
          LIMIT $1
        `,
        values: [command.limit],
      });

      const recovered: RecoveredExecutionLease[] = [];
      for (const execution of candidates.rows) {
        // Renewal locks only the attempt. Rechecking after this lock lets an in-flight
        // renewal win without introducing the execution/attempt lock-order cycle.
        // Fully checkpointed running attempts remain adoptable rather than being failed here.
        const attemptResult = await client.query<ExpiredAttemptRow>({
          text: `
            WITH recovery_clock AS MATERIALIZED (SELECT clock_timestamp() AS recovered_at)
            SELECT attempt.attempt, attempt.state, recovery_clock.recovered_at
            FROM dispatch_execution_attempts AS attempt, recovery_clock
            WHERE attempt.execution_id = $1 AND attempt.tenant_id = $2
              AND attempt.state IN ('LEASED', 'RUNNING')
              AND attempt.lease_expires_at <= recovery_clock.recovered_at
              AND NOT (
                attempt.state = 'RUNNING'
                AND attempt.workload_name IS NOT NULL
                AND attempt.opencode_session_id IS NOT NULL
                AND $3 > recovery_clock.recovered_at
              )
            ORDER BY attempt.attempt DESC
            FOR UPDATE OF attempt
            LIMIT 1
          `,
          values: [execution.id, execution.tenant_id, execution.timeout_at],
        });
        const attempt = attemptResult.rows[0];
        if (!attempt) continue;

        const updateResult = await client.query<RecoveredExecutionRow>({
          text: `
            WITH updated_attempt AS (
              UPDATE dispatch_execution_attempts
              SET state = CASE WHEN $4 >= $9 THEN 'TIMED_OUT' ELSE 'FAILED' END, finished_at = $4,
                  lease_owner = NULL, lease_expires_at = NULL
              WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
                AND state IN ('LEASED', 'RUNNING') AND lease_expires_at <= $4
              RETURNING execution_id
            ), updated_execution AS (
              UPDATE dispatch_executions
               SET state = CASE
                     WHEN $4 >= timeout_at THEN 'TIMED_OUT'
                     WHEN $3 < $5 AND $4 + ($6::double precision * interval '1 millisecond') < timeout_at
                       THEN 'RETRY_WAIT'
                    ELSE 'FAILED'
                  END,
                  available_at = CASE
                    WHEN $3 < $5 AND $4 + ($6::double precision * interval '1 millisecond') < timeout_at
                      THEN $4 + ($6::double precision * interval '1 millisecond')
                    ELSE available_at
                  END,
                  completed_at = CASE
                    WHEN $3 < $5 AND $4 + ($6::double precision * interval '1 millisecond') < timeout_at
                      THEN NULL
                    ELSE $4
                  END,
                  updated_at = $4
              WHERE id = $1 AND tenant_id = $2 AND state = $7
                AND EXISTS (SELECT 1 FROM updated_attempt)
              RETURNING id, tenant_id, state
            ), inserted_transition AS (
              INSERT INTO dispatch_execution_transitions
                (id, tenant_id, execution_id, attempt, sequence, from_state, to_state, actor, reason, created_at)
              SELECT $8, tenant_id, id, $3,
                     (SELECT COALESCE(MAX(sequence), 0) + 1
                      FROM dispatch_execution_transitions
                      WHERE tenant_id = $2 AND execution_id = $1),
                     $7, state, 'execution-reconciler', 'execution lease expired', $4
              FROM updated_execution
              RETURNING execution_id
            )
            SELECT id, tenant_id, state, $4::timestamptz AS recovered_at
            FROM updated_execution
            WHERE EXISTS (SELECT 1 FROM inserted_transition)
          `,
          values: [
            execution.id,
            execution.tenant_id,
            attempt.attempt,
            attempt.recovered_at,
            command.maxAttempts,
            command.retryDelayMs,
             execution.state,
             randomUUID(),
             execution.timeout_at,
          ],
        });
        const row = updateResult.rows[0];
        if (!row) continue;
        recovered.push({
          attempt: attempt.attempt,
          executionId: row.id,
          executionState: row.state,
          recoveredAt: row.recovered_at,
          tenantId: row.tenant_id,
        });
      }

      await client.query("COMMIT");
      return recovered;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async promoteDueExecutionRetries(command: { limit: number }): Promise<PromotedExecutionRetry[]> {
    assertPositiveInteger(command.limit, "Execution retry promotion limit");

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<PromotedExecutionRow>({
        text: `
          WITH promotion_clock AS MATERIALIZED (SELECT clock_timestamp() AS promoted_at),
          candidates AS (
            SELECT execution.id, execution.tenant_id
            FROM dispatch_executions AS execution, promotion_clock
            WHERE execution.state = 'RETRY_WAIT'
              AND (execution.available_at <= promotion_clock.promoted_at
                OR execution.timeout_at <= promotion_clock.promoted_at)
            ORDER BY execution.available_at, execution.created_at, execution.id
            FOR UPDATE OF execution SKIP LOCKED
            LIMIT $1
          ), updated_execution AS (
            UPDATE dispatch_executions AS execution
            SET state = CASE
                  WHEN execution.timeout_at <= promotion_clock.promoted_at THEN 'TIMED_OUT'
                  ELSE 'QUEUED'
                END,
                completed_at = CASE
                  WHEN execution.timeout_at <= promotion_clock.promoted_at THEN promotion_clock.promoted_at
                  ELSE completed_at
                END,
                updated_at = promotion_clock.promoted_at
            FROM candidates, promotion_clock
            WHERE execution.id = candidates.id AND execution.tenant_id = candidates.tenant_id
              AND execution.state = 'RETRY_WAIT'
              AND (execution.available_at <= promotion_clock.promoted_at
                OR execution.timeout_at <= promotion_clock.promoted_at)
            RETURNING execution.id, execution.tenant_id, execution.state
          ), inserted_transition AS (
            INSERT INTO dispatch_execution_transitions
              (id, tenant_id, execution_id, attempt, sequence, from_state, to_state, actor, reason, created_at)
            SELECT ($2::text[])[(row_number() OVER ())::integer], execution.tenant_id, execution.id, NULL,
                   (SELECT COALESCE(MAX(sequence), 0) + 1
                    FROM dispatch_execution_transitions
                    WHERE tenant_id = execution.tenant_id AND execution_id = execution.id),
                   'RETRY_WAIT', execution.state, 'execution-reconciler',
                   CASE WHEN execution.state = 'TIMED_OUT' THEN 'execution deadline elapsed' ELSE 'execution retry became due' END,
                   promotion_clock.promoted_at
            FROM updated_execution AS execution, promotion_clock
            RETURNING execution_id
          )
          SELECT execution.id, execution.tenant_id, execution.state, promotion_clock.promoted_at
          FROM updated_execution AS execution, promotion_clock
          WHERE EXISTS (
            SELECT 1 FROM inserted_transition WHERE inserted_transition.execution_id = execution.id
          )
        `,
        values: [command.limit, Array.from({ length: command.limit }, () => randomUUID())],
      });
      await client.query("COMMIT");
      return result.rows.map((row) => ({
        executionId: row.id,
        executionState: row.state,
        promotedAt: row.promoted_at,
        tenantId: row.tenant_id,
      }));
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async transitionLeasedExecution(command: TransitionLeasedExecutionCommand): Promise<TransitionLeasedExecutionResult> {
    if (!isValidTransitionLeasedExecutionCommand(command)) {
      throw new Error("Invalid leased execution transition");
    }
    assertNonnegativeInteger(command.retryDelayMs ?? 0, "Execution retry delay");

    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const executionResult = await client.query<{ state: string }>({
        text: `SELECT state FROM dispatch_executions WHERE id = $1 AND tenant_id = $2 FOR UPDATE`,
        values: [command.executionId, command.tenantId],
      });
      if (!executionResult.rows[0]) {
        await client.query("ROLLBACK");
        return { applied: false, reason: "NOT_FOUND" };
      }
      if (executionResult.rows[0].state !== command.expectedExecutionState) {
        await client.query("ROLLBACK");
        return { applied: false, reason: "STATE_MISMATCH" };
      }

      const attemptResult = await client.query<{ state: string; lease_expires_at: Date }>({
        text: `
          SELECT state, lease_expires_at
          FROM dispatch_execution_attempts
          WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
            AND fencing_token = $4 AND lease_owner = $5
          FOR UPDATE
        `,
        values: [command.executionId, command.tenantId, command.attempt, command.fencingToken, command.leaseOwner],
      });
      const attempt = attemptResult.rows[0];
      if (!attempt) {
        const existsResult = await client.query<{ exists: boolean }>({
          text: `
            SELECT EXISTS (
              SELECT 1 FROM dispatch_execution_attempts
              WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
            ) AS exists
          `,
          values: [command.executionId, command.tenantId, command.attempt],
        });
        await client.query("ROLLBACK");
        return { applied: false, reason: existsResult.rows[0]?.exists ? "LEASE_MISMATCH" : "NOT_FOUND" };
      }
      if (attempt.state !== command.expectedAttemptState) {
        await client.query("ROLLBACK");
        return { applied: false, reason: "STATE_MISMATCH" };
      }
      const clockResult = await client.query<{ now: Date }>("SELECT clock_timestamp() AS now");
      const now = clockResult.rows[0]?.now;
      if (!now || attempt.lease_expires_at <= now) {
        await client.query("ROLLBACK");
        return { applied: false, reason: "LEASE_EXPIRED" };
      }

      const terminalAttempt = TERMINAL_ATTEMPT_STATES.has(command.targetAttemptState);
      const terminalExecution = isTerminalExecutionState(command.targetExecutionState);
      const updated = await client.query<{ updated_at: Date }>({
        text: `
          WITH updated_attempt AS (
            UPDATE dispatch_execution_attempts
            SET state = $6,
                started_at = CASE WHEN $6 = 'RUNNING' THEN COALESCE(started_at, $7) ELSE started_at END,
                finished_at = CASE WHEN $8 THEN $7 ELSE NULL END,
                lease_owner = CASE WHEN $8 THEN NULL ELSE lease_owner END,
                lease_expires_at = CASE WHEN $8 THEN NULL ELSE lease_expires_at END,
                workload_name = COALESCE($18, workload_name),
                opencode_session_id = COALESCE($19, opencode_session_id)
            WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
              AND fencing_token = $4 AND lease_owner = $5 AND state = $9
            RETURNING execution_id
          ), updated_execution AS (
            UPDATE dispatch_executions
            SET state = $10, result = $11::jsonb,
                available_at = CASE WHEN $10 = 'RETRY_WAIT'
                  THEN $7 + ($20::double precision * interval '1 millisecond') ELSE available_at END,
                completed_at = CASE WHEN $12 THEN $7 ELSE NULL END,
                updated_at = $7
            WHERE id = $1 AND tenant_id = $2 AND state = $13
              AND EXISTS (SELECT 1 FROM updated_attempt)
            RETURNING id, updated_at
          ), inserted_transition AS (
            INSERT INTO dispatch_execution_transitions
              (id, tenant_id, execution_id, attempt, sequence, from_state, to_state, actor, reason, trace_context, created_at)
            SELECT $14, $2, id, $3,
                   (SELECT COALESCE(MAX(sequence), 0) + 1 FROM dispatch_execution_transitions WHERE tenant_id = $2 AND execution_id = $1),
                   $13, $10, $15, $16, $17::jsonb, $7
            FROM updated_execution
            RETURNING execution_id
          )
          SELECT updated_at FROM updated_execution
          WHERE EXISTS (SELECT 1 FROM inserted_transition)
        `,
        values: [
          command.executionId, command.tenantId, command.attempt, command.fencingToken, command.leaseOwner,
          command.targetAttemptState, now, terminalAttempt, command.expectedAttemptState, command.targetExecutionState,
          JSON.stringify(command.result ?? null), terminalExecution, command.expectedExecutionState, randomUUID(), command.actor,
          command.reason, JSON.stringify({}), command.workloadName ?? null, command.opencodeSessionId ?? null,
          command.retryDelayMs ?? 0,
        ],
      });
      if (updated.rowCount !== 1) {
        await client.query("ROLLBACK");
        return { applied: false, reason: "STATE_MISMATCH" };
      }
      await client.query("COMMIT");
      return {
        applied: true,
        attemptState: command.targetAttemptState,
        executionState: command.targetExecutionState,
      };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async checkpointLeasedExecutionAttemptDiagnostic(command: CheckpointLeasedExecutionAttemptDiagnosticCommand): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      const now = (await client.query<{ now: Date }>("SELECT clock_timestamp() AS now")).rows[0]!.now;
      const diagnostic = { ...command.diagnostic, updatedAt: now.toISOString() };
      if (Buffer.byteLength(JSON.stringify(diagnostic), "utf8") > 8192) throw new Error("Execution attempt diagnostic exceeds 8192 bytes");
      const updated = await client.query(`UPDATE dispatch_execution_attempts
        SET diagnostic = $6::jsonb
        WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
          AND fencing_token = $4 AND lease_owner = $5
          AND lease_expires_at > clock_timestamp() AND state IN ('LEASED', 'RUNNING')`,
      [command.executionId, command.tenantId, command.attempt, command.fencingToken, command.leaseOwner, JSON.stringify(diagnostic)]);
      return updated.rowCount === 1;
    } finally {
      client.release();
    }
  }

  async completeLeasedExecutionTurn(command: CompleteLeasedExecutionTurnCommand): Promise<CompleteLeasedExecutionTurnResult> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const executionResult = await client.query<{ binding_version_id: string; event_id: string; state: string; timeout_at: Date }>(
        "SELECT binding_version_id, event_id, state, timeout_at FROM dispatch_executions WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
        [command.executionId, command.tenantId],
      );
      const execution = executionResult.rows[0];
      if (!execution) { await client.query("ROLLBACK"); return { applied: false, reason: "NOT_FOUND" }; }
      if (execution.state !== "RUNNING") { await client.query("ROLLBACK"); return { applied: false, reason: "STATE_MISMATCH" }; }
      const attemptResult = await client.query<{ state: string; lease_expires_at: Date }>(`SELECT state, lease_expires_at
        FROM dispatch_execution_attempts WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
          AND fencing_token = $4 AND lease_owner = $5 FOR UPDATE`,
      [command.executionId, command.tenantId, command.attempt, command.fencingToken, command.leaseOwner]);
      const attempt = attemptResult.rows[0];
      if (!attempt) {
        const exists = await client.query<{ exists: boolean }>("SELECT EXISTS (SELECT 1 FROM dispatch_execution_attempts WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3) AS exists", [command.executionId, command.tenantId, command.attempt]);
        await client.query("ROLLBACK");
        return { applied: false, reason: exists.rows[0]?.exists ? "LEASE_MISMATCH" : "NOT_FOUND" };
      }
      if (attempt.state !== "RUNNING") { await client.query("ROLLBACK"); return { applied: false, reason: "STATE_MISMATCH" }; }
      const now = (await client.query<{ now: Date }>("SELECT clock_timestamp() AS now")).rows[0]!.now;
      if (attempt.lease_expires_at <= now) { await client.query("ROLLBACK"); return { applied: false, reason: "LEASE_EXPIRED" }; }

      const bindingResult = await client.query<BindingRow>(BINDING_SELECT + " WHERE binding.tenant_id = $1 AND binding.id = $2", [command.tenantId, execution.binding_version_id]);
      const binding = bindingResult.rows[0] ? bindingFromRow(bindingResult.rows[0]) : undefined;
      if (!binding) throw new PersistedExecutionCorruptionError(command.executionId, "binding version");
      if ("disposition" in binding.definition) throw new PersistedExecutionCorruptionError(command.executionId, "wake binding created execution");
      const policy = binding.definition.afterTurn;
      let checkpointOutcome: { bindingId: string; result: "applied" | "superseded" } | undefined;
      let wait: { id: string; name: string; correlation: Record<string, JsonPrimitive>; deadline: Date } | undefined;
      const pendingResult = await client.query<{
        id: string; event_id: string; binding_version_id: string; action: "CONTINUED" | "COMPLETED"; input: unknown; workspace: unknown;
      }>(`SELECT intent.id, intent.event_id, intent.binding_version_id, intent.action, intent.input, intent.workspace
        FROM dispatch_execution_pending_wakes pending
        JOIN dispatch_event_wake_intents intent ON intent.id=pending.intent_id
        WHERE pending.tenant_id=$1 AND pending.execution_id=$2`, [command.tenantId, command.executionId]);
      const pending = pendingResult.rows[0];
      let executionState: "SUCCEEDED" | "WAITING" | "TIMED_OUT" | "QUEUED" | "COMPLETED" = "SUCCEEDED";
      let continuationSequence: number | null = null;
      if (execution.timeout_at <= now) executionState = "TIMED_OUT";
      else if (pending?.action === "CONTINUED") executionState = "QUEUED";
      else if (pending?.action === "COMPLETED") executionState = "COMPLETED";
      else if (policy) {
        {
          const eventData = (await client.query<{ data: JsonValue }>("SELECT data FROM dispatch_events WHERE id = $1 AND tenant_id = $2", [execution.event_id, command.tenantId])).rows[0]?.data;
          if (eventData === undefined) throw new PersistedExecutionCorruptionError(command.executionId, "event data");
          const correlation: Record<string, JsonPrimitive> = {};
          for (const item of policy.wait.correlation) {
            if (!("path" in item)) continue;
            const resolved = resolveJsonPointer(eventData, item.path);
            if (!resolved.found || (resolved.value !== null && typeof resolved.value === "object")) {
              throw new PersistedExecutionCorruptionError(command.executionId, `wait correlation ${item.name}`);
            }
            if (Buffer.byteLength(JSON.stringify(resolved.value), "utf8") > 1_024) {
              throw new PersistedExecutionCorruptionError(command.executionId, `wait correlation ${item.name}`);
            }
            correlation[item.name] = resolved.value;
          }
          const context = await client.query<{ correlation: Record<string, JsonPrimitive>; state: string }>(
            "SELECT correlation, state FROM dispatch_execution_wake_contexts WHERE tenant_id=$1 AND execution_id=$2",
            [command.tenantId, command.executionId],
          );
          if (context.rows[0]) Object.assign(correlation, context.rows[0].correlation);
          wait = {
            id: randomUUID(), name: policy.wait.name, correlation,
            deadline: new Date(now.getTime() + policy.wait.deadlineSeconds * 1_000),
          };
          if (wait.deadline <= now) executionState = "TIMED_OUT";
          else executionState = "WAITING";
        }
      }

      if (binding.definition.requireGitHubPullRequestEffect && executionState === "WAITING") {
        const effect = await client.query<{ state: string }>(`SELECT state FROM dispatch_github_pull_request_effects
          WHERE tenant_id = $1 AND execution_id = $2 FOR UPDATE`, [command.tenantId, command.executionId]);
        if (effect.rowCount !== 1) {
          await client.query(`UPDATE dispatch_execution_attempts SET state = 'FAILED', finished_at = $6,
            lease_owner = NULL, lease_expires_at = NULL WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
            AND fencing_token = $4 AND lease_owner = $5 AND state = 'RUNNING'`,
          [command.executionId, command.tenantId, command.attempt, command.fencingToken, command.leaseOwner, now]);
          await client.query(`UPDATE dispatch_executions SET state = 'FAILED', result = $3::jsonb, completed_at = $4,
            updated_at = $4 WHERE id = $1 AND tenant_id = $2 AND state = 'RUNNING'`,
          [command.executionId, command.tenantId, JSON.stringify({ error: "MISSING_REQUIRED_GITHUB_PR_EFFECT", output: command.result }), now]);
          await client.query(`INSERT INTO dispatch_execution_transitions
            (id, tenant_id, execution_id, attempt, sequence, from_state, to_state, actor, reason, created_at)
            VALUES ($1,$2,$3,$4,(SELECT COALESCE(MAX(sequence),0)+1 FROM dispatch_execution_transitions WHERE tenant_id=$2 AND execution_id=$3),
              'RUNNING','FAILED',$5,'MISSING_REQUIRED_GITHUB_PR_EFFECT',$6)`,
          [randomUUID(), command.tenantId, command.executionId, command.attempt, command.actor, now]);
          await client.query("COMMIT");
          return { applied: true, attemptState: "FAILED", executionState: "FAILED" } as CompleteLeasedExecutionTurnResult;
        }
      }

      if (executionState === "QUEUED" && pending) {
        const currentSequence = (await client.query<{ current_input_sequence: number }>(
          "SELECT current_input_sequence FROM dispatch_executions WHERE id=$1 AND tenant_id=$2",
          [command.executionId, command.tenantId],
        )).rows[0]!.current_input_sequence;
        continuationSequence = currentSequence + 1;
        await client.query(`INSERT INTO dispatch_execution_inputs
          (tenant_id, execution_id, sequence, kind, event_id, input, workspace, created_at)
          VALUES ($1,$2,$3,'WAKE',$4,$5::jsonb,$6::jsonb,$7)`,
        [command.tenantId, command.executionId, continuationSequence, pending.event_id,
          JSON.stringify(pending.input), JSON.stringify(pending.workspace), now]);
      }

      await client.query(`UPDATE dispatch_execution_attempts SET state = 'SUCCEEDED', finished_at = $6,
        lease_owner = NULL, lease_expires_at = NULL WHERE execution_id = $1 AND tenant_id = $2 AND attempt = $3
        AND fencing_token = $4 AND lease_owner = $5 AND state = 'RUNNING'`,
      [command.executionId, command.tenantId, command.attempt, command.fencingToken, command.leaseOwner, now]);
      await client.query(`UPDATE dispatch_executions SET state = $3::text, result = $4::jsonb, updated_at = $5::timestamptz,
        timeout_at = CASE
          WHEN $3::text = 'WAITING' THEN $6::timestamptz
          WHEN $3::text = 'QUEUED' THEN $5::timestamptz + (((resolved_policy->>'timeoutSeconds')::integer) * interval '1 second')
          ELSE timeout_at END,
        current_input_sequence = COALESCE($7::integer, current_input_sequence),
        available_at = CASE WHEN $3::text = 'QUEUED' THEN $5::timestamptz ELSE available_at END,
        completed_at = CASE WHEN $3::text IN ('TIMED_OUT','FAILED','CANCELLED','COMPLETED','DEAD_LETTERED') THEN $5::timestamptz ELSE NULL END
        WHERE id = $1 AND tenant_id = $2 AND state = 'RUNNING'`,
      [command.executionId, command.tenantId, executionState, JSON.stringify(command.result), now, wait?.deadline ?? execution.timeout_at, continuationSequence]);
      if (executionState === "SUCCEEDED") {
        const advance = (await client.query<{
          binding_id: string; checkpoint_name: string; checkpoint_key_hash: string; checkpoint_key_values: JsonPrimitive[];
          expected_previous_exists: boolean; expected_previous_value: JsonPrimitive | null; target_value: JsonPrimitive;
        }>(`SELECT binding_id, checkpoint_name, checkpoint_key_hash, checkpoint_key_values,
          expected_previous_exists, expected_previous_value, target_value
          FROM dispatch_execution_checkpoint_advances
          WHERE execution_id=$1 AND tenant_id=$2 AND state='PENDING' FOR UPDATE`,
        [command.executionId, command.tenantId])).rows[0];
        if (advance) {
          await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
            `execution-checkpoint:${command.tenantId}:${advance.binding_id}:${advance.checkpoint_name}:${advance.checkpoint_key_hash}`,
          ]);
          let applied: boolean;
          if (advance.expected_previous_exists) {
            const updated = await client.query(`UPDATE dispatch_execution_checkpoints SET
              value=$6::jsonb, advanced_by_execution_id=$5, advanced_at=$7, updated_at=$7
              WHERE tenant_id=$1 AND binding_id=$2 AND checkpoint_name=$3 AND checkpoint_key_hash=$4
                AND value=$8::jsonb`,
            [command.tenantId, advance.binding_id, advance.checkpoint_name, advance.checkpoint_key_hash,
              command.executionId, JSON.stringify(advance.target_value), now, JSON.stringify(advance.expected_previous_value)]);
            applied = updated.rowCount === 1;
          } else {
            const inserted = await client.query(`INSERT INTO dispatch_execution_checkpoints
              (tenant_id,binding_id,checkpoint_name,checkpoint_key_hash,checkpoint_key_values,value,
               advanced_by_execution_id,advanced_at,created_at,updated_at)
              VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$8,$8) ON CONFLICT DO NOTHING`,
            [command.tenantId, advance.binding_id, advance.checkpoint_name, advance.checkpoint_key_hash,
              JSON.stringify(advance.checkpoint_key_values), JSON.stringify(advance.target_value), command.executionId, now]);
            applied = inserted.rowCount === 1;
          }
          await client.query(`UPDATE dispatch_execution_checkpoint_advances SET state=$3, applied_at=$4
            WHERE execution_id=$1 AND tenant_id=$2`,
          [command.executionId, command.tenantId, applied ? "APPLIED" : "SUPERSEDED", now]);
          checkpointOutcome = { bindingId: advance.binding_id, result: applied ? "applied" : "superseded" };
        }
      }
      if (executionState === "WAITING" && wait) {
        await client.query(`INSERT INTO dispatch_event_waits
          (id, tenant_id, execution_id, attempt, name, state, correlation, deadline_at, activated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9)`,
        [wait.id, command.tenantId, command.executionId, command.attempt, wait.name,
          Object.keys(wait.correlation).length === policy!.wait.correlation.length ? "ACTIVE" : "PENDING_CONTEXT",
          JSON.stringify(wait.correlation), wait.deadline, now]);
      }
      if (pending && (executionState === "QUEUED" || executionState === "COMPLETED")) {
        const wakeId = randomUUID();
        await client.query(`INSERT INTO dispatch_event_wakes
          (id, tenant_id, event_id, event_wait_id, wake_intent_id, execution_id, binding_version_id, action, input_sequence, to_state, consumed_at)
          VALUES ($1,$2,$3,NULL,$4,$5,$6,$7,$8,$9,$10)`,
        [wakeId, command.tenantId, pending.event_id, pending.id, command.executionId, pending.binding_version_id,
          pending.action, continuationSequence, executionState, now]);
        await client.query("DELETE FROM dispatch_execution_pending_wakes WHERE tenant_id=$1 AND execution_id=$2", [command.tenantId, command.executionId]);
        if (executionState === "QUEUED") {
          await client.query(`INSERT INTO dispatch_outbox
            (id, tenant_id, topic, aggregate_type, aggregate_id, payload, available_at, created_at)
            VALUES ($1,$2,'execution.requested','event-wake',$3,$4::jsonb,$5,$5)`,
          [randomUUID(), command.tenantId, wakeId, JSON.stringify({ schemaVersion: 1, tenantId: command.tenantId, executionId: command.executionId, wakeId, inputSequence: continuationSequence }), now]);
        }
      }
      await client.query(`INSERT INTO dispatch_execution_transitions
        (id, tenant_id, execution_id, attempt, sequence, from_state, to_state, actor, reason, created_at)
        VALUES ($1,$2,$3,$4,(SELECT COALESCE(MAX(sequence),0)+1 FROM dispatch_execution_transitions WHERE tenant_id=$2 AND execution_id=$3),
          'RUNNING',$5,$6,$7,$8)`, [randomUUID(), command.tenantId, command.executionId, command.attempt, executionState, command.actor, command.reason, now]);
      await client.query("COMMIT");
      if (checkpointOutcome) checkpointAdvances.inc({ tenant: command.tenantId, binding_id: checkpointOutcome.bindingId, result: checkpointOutcome.result });
      return { applied: true, attemptState: "SUCCEEDED", executionState, ...(executionState === "WAITING" && wait ? { eventWaitId: wait.id } : {}) };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally { client.release(); }
  }

  async expireDueEventWaits(input: { limit: number }): Promise<ExpiredEventWait[]> {
    assertPositiveInteger(input.limit, "Event wait expiration limit");
    const client = await this.pool.connect();
    const expired: ExpiredEventWait[] = [];
    try {
      await client.query("BEGIN");
      const candidates = await client.query<{ id: string; tenant_id: string }>(`SELECT execution.id, execution.tenant_id
        FROM dispatch_executions AS execution
        JOIN dispatch_event_waits AS wait ON wait.execution_id = execution.id AND wait.tenant_id = execution.tenant_id
        WHERE execution.state = 'WAITING' AND wait.state IN ('ACTIVE','PENDING_CONTEXT') AND wait.deadline_at <= clock_timestamp()
        ORDER BY wait.deadline_at, execution.id FOR UPDATE OF execution SKIP LOCKED LIMIT $1`, [input.limit]);
      for (const candidate of candidates.rows) {
        const waitResult = await client.query<{ id: string; deadline_at: Date }>(`SELECT id, deadline_at FROM dispatch_event_waits
          WHERE tenant_id = $1 AND execution_id = $2 AND state IN ('ACTIVE','PENDING_CONTEXT') FOR UPDATE`, [candidate.tenant_id, candidate.id]);
        const wait = waitResult.rows[0];
        if (!wait) continue;
        const now = (await client.query<{ now: Date }>("SELECT clock_timestamp() AS now")).rows[0]!.now;
        if (wait.deadline_at > now) continue;
        await client.query("UPDATE dispatch_event_waits SET state = 'EXPIRED', ended_at = $3 WHERE tenant_id = $1 AND id = $2 AND state IN ('ACTIVE','PENDING_CONTEXT')", [candidate.tenant_id, wait.id, now]);
        await client.query("UPDATE dispatch_executions SET state = 'TIMED_OUT', completed_at = $3, updated_at = $3 WHERE tenant_id = $1 AND id = $2 AND state = 'WAITING'", [candidate.tenant_id, candidate.id, now]);
        await client.query(`INSERT INTO dispatch_execution_transitions
          (id, tenant_id, execution_id, attempt, sequence, from_state, to_state, actor, reason, created_at)
          VALUES ($1,$2,$3,NULL,(SELECT COALESCE(MAX(sequence),0)+1 FROM dispatch_execution_transitions WHERE tenant_id=$2 AND execution_id=$3),
            'WAITING','TIMED_OUT','execution-maintenance','event wait deadline elapsed',$4)`,
        [randomUUID(), candidate.tenant_id, candidate.id, now]);
        expired.push({ eventWaitId: wait.id, executionId: candidate.id, tenantId: candidate.tenant_id, expiredAt: now });
      }
      await client.query("COMMIT");
      return expired;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally { client.release(); }
  }

}

type AgentProfileVersionRow = InferSelectModel<typeof agentProfileVersions>;
type AgentProfileVersionSqlRow = {
  created_at: Date;
  definition: unknown;
  id: string;
  profile_id: string;
  tenant_id: string;
  version: number;
};
type ConnectionRow = {
  connection_id: string;
  created_at: Date;
  id: string;
  tenant_id: string;
  type: string;
};
type ExecutionRow = InferSelectModel<typeof executions>;
type BindingRow = {
  binding_id: string;
  created_at: Date;
  definition: unknown;
  disabled_at: Date | null;
  enabled: boolean;
  event_types: string[];
  id: string;
  profile_version_id: string;
  profile_id?: string;
  profile_version?: number;
  tenant_id: string;
  trigger_id: string;
  version: number;
};

type BindingProfileRow = BindingRow & { profile_definition: Record<string, unknown> };

function isDefaultBranchRevisionBinding(row: BindingProfileRow): boolean {
  const binding = bindingFromRow(row);
  if ("disposition" in binding.definition) return false;
  return binding.definition.workspace.type === "git"
    && binding.definition.workspace.revision.commit.path === "/repository/defaultBranchRevision/commit";
}
type EventRow = {
  admission_hash: string;
  data: unknown;
  data_content_type: string;
  data_schema: string | null;
  event_id: string;
  event_time: Date | null;
  extensions: Record<string, JsonValue>;
  id: string;
  ingested_at: Date;
  source: string;
  source_deduplication_key: string;
  subject: string | null;
  tenant_id: string;
  trigger_id: string;
  type: string;
};
type RevisionResolutionRow = {
  attempt: number;
  branch: string;
  clone_url: string;
  event_id: string;
  installation_id: string;
  lease_expires_at: Date | null;
  lease_owner: string | null;
  lease_token: string | null;
  provider: string;
  repository_full_name: string;
  repository_id: string;
  state: string;
  tenant_id: string;
};

type TriggerSqlRow = {
  config: unknown;
  created_at: Date;
  disabled_at: Date | null;
  enabled: boolean;
  id: string;
  tenant_id: string;
  type: string;
};

type ScheduleOccurrenceRow = {
  id: string;
  tenant_id: string;
  trigger_id: string;
  scheduled_at: Date;
  state: string;
  attempt: number;
  available_at: Date;
  lease_owner: string | null;
  lease_token: string | null;
  lease_expires_at: Date | null;
};

type ExecutionJoinedRow = {
  binding_id: string;
  binding_version: number;
  created_at: Date;
  event_id: string;
  id: string;
  input: unknown;
  profile_id: string;
  profile_version: number;
  result: unknown;
  state: string;
  tenant_id: string;
  updated_at: Date;
  workspace: unknown;
};

type ExecutionAttemptRow = {
  attempt: number;
  finished_at: Date | null;
  lease_expires_at: Date | null;
  opencode_session_id: string | null;
  started_at: Date | null;
  state: string;
  workload_name: string | null;
  diagnostic: import("../execution/types.js").ExecutionAttemptDiagnostic | null;
};

type ExecutionTransitionRow = {
  actor: string;
  attempt: number | null;
  created_at: Date;
  from_state: string | null;
  id: string;
  reason: string | null;
  sequence: number;
  to_state: string;
  trace_context: Record<string, string>;
};
type EventWaitRow = {
  activated_at: Date;
  attempt: number;
  correlation: Record<string, JsonPrimitive>;
  deadline_at: Date;
  ended_at: Date | null;
  id: string;
  name: string;
  state: string;
};
type EventWakeRow = {
  action: "CONTINUED" | "COMPLETED" | "CANCELLED";
  binding_id: string;
  binding_version: number;
  consumed_at: Date;
  event_wait_id: string;
  execution_id: string;
  id: string;
  input_sequence: number | null;
  to_state: "QUEUED" | "COMPLETED" | "CANCELLED";
};
type EventWakeIntentRow = {
  action: "CONTINUED" | "COMPLETED";
  admitted_at: Date;
  binding_id: string;
  binding_version: number;
  disposition: "PENDING" | "DOMINATED";
  execution_id: string;
  id: string;
};

type ClaimedExecutionRow = {
  attempt: number;
  attempt_state: string;
  created_at: Date;
  definition: Record<string, unknown>;
  event_id: string;
  execution_state: string;
  fencing_token: string;
  id: string;
  input: unknown;
  lease_expires_at: Date;
  lease_owner: string;
  opencode_session_id?: string | null;
  profile_id: string;
  profile_version_id: string;
  resolved_policy: Record<string, unknown>;
  tenant_id: string;
  timeout_at: Date;
  version: number;
  workload_name?: string | null;
  workspace: Record<string, unknown>;
};

type ClaimedOutboxRow = {
  aggregate_id: string;
  aggregate_type: string;
  available_at: Date;
  created_at: Date;
  headers: Record<string, string>;
  id: string;
  lease_expires_at: Date;
  lease_token: string;
  payload: unknown;
  publish_attempts: number;
  tenant_id: string;
  topic: string;
};

type RecoveryExecutionRow = {
  id: string;
  state: "PROVISIONING" | "RUNNING";
  tenant_id: string;
  timeout_at: Date;
};

type ExpiredAttemptRow = {
  attempt: number;
  recovered_at: Date;
  state: "LEASED" | "RUNNING";
};

type RecoveredExecutionRow = {
  id: string;
  recovered_at: Date;
  state: "RETRY_WAIT" | "FAILED" | "TIMED_OUT";
  tenant_id: string;
};

type PromotedExecutionRow = {
  id: string;
  promoted_at: Date;
  state: "QUEUED" | "TIMED_OUT";
  tenant_id: string;
};

function outboxMessageFromRow(row: ClaimedOutboxRow): ClaimedOutboxMessage {
  return {
    aggregateId: row.aggregate_id,
    aggregateType: row.aggregate_type,
    availableAt: row.available_at,
    createdAt: row.created_at,
    headers: row.headers,
    id: row.id,
    claimExpiresAt: row.lease_expires_at,
    claimToken: row.lease_token,
    payload: row.payload,
    publishAttempts: row.publish_attempts,
    tenantId: row.tenant_id,
    topic: row.topic,
  };
}

const TERMINAL_ATTEMPT_STATES: ReadonlySet<string> = new Set(["SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"]);

function claimedExecutionFromRow(row: ClaimedExecutionRow): ClaimedExecution {
  if (!isExecutionState(row.execution_state) || !isAttemptState(row.attempt_state)) {
    throw new Error(`Claimed execution ${row.id} has an invalid persisted state`);
  }
  return {
    ...(row.workload_name && row.opencode_session_id
      ? { adoption: { workloadName: row.workload_name, opencodeSessionId: row.opencode_session_id } }
      : {}),
    createdAt: row.created_at,
    eventId: row.event_id,
    executionId: row.id,
    input: row.input as ClaimedExecution["input"],
    lease: {
      attempt: row.attempt,
      fencingToken: row.fencing_token,
      leaseExpiresAt: row.lease_expires_at,
      leaseOwner: row.lease_owner,
    },
    profileVersion: {
      definition: row.definition as ClaimedExecution["profileVersion"]["definition"],
      id: row.profile_version_id,
      profileId: row.profile_id,
      version: row.version,
    },
    resolvedPolicy: row.resolved_policy as ClaimedExecution["resolvedPolicy"],
    tenantId: row.tenant_id,
    timeoutAt: row.timeout_at,
    workspace: persistedWorkspace(row.id, row.workspace),
  };
}

function profileVersionFromRow(row: AgentProfileVersionRow): AgentProfileVersion {
  return {
    createdAt: row.createdAt.toISOString(),
    definition: agentProfileDefinitionSchema.parse(row.definition),
    id: row.id,
    profile: { id: row.profileID, version: row.version },
    tenantId: row.tenantID,
  };
}

function profileVersionFromSqlRow(row: AgentProfileVersionSqlRow): AgentProfileVersion {
  return {
    createdAt: row.created_at.toISOString(),
    definition: agentProfileDefinitionSchema.parse(row.definition),
    id: row.id,
    profile: { id: row.profile_id, version: row.version },
    tenantId: row.tenant_id,
  };
}

function connectionFromRow(row: ConnectionRow): Connection {
  return parseConnection({
    connection: { id: row.connection_id, type: row.type },
    createdAt: row.created_at.toISOString(),
    id: row.id,
    tenantId: row.tenant_id,
  });
}

function triggerFromRow(row: TriggerSqlRow): Trigger {
  return triggerSchema.parse({
    config: row.config,
    createdAt: row.created_at.toISOString(),
    disabledAt: row.disabled_at?.toISOString() ?? null,
    enabled: row.enabled,
    id: row.id,
    tenantId: row.tenant_id,
    type: row.type,
  });
}

function bindingFromRow(row: BindingRow): PublishedBindingVersion {
  const persisted = row.definition as Record<string, unknown>;
  const definition = bindingDefinitionSchema.parse({
    filter: persisted.filter,
    schemaVersion: persisted.schemaVersion,
    eventTypes: row.event_types,
    ...(persisted.disposition === "wake"
      ? { disposition: persisted.disposition, wake: persisted.wake }
      : { activeSingleton: persisted.activeSingleton, afterTurn: persisted.afterTurn, checkpoint: persisted.checkpoint, prompt: persisted.prompt, requireGitHubPullRequestEffect: persisted.requireGitHubPullRequestEffect, workspace: persisted.workspace }),
  });
  return {
    bindingId: row.binding_id,
    createdAt: row.created_at.toISOString(),
    definition,
    disabledAt: row.disabled_at?.toISOString() ?? null,
    enabled: row.enabled,
    id: row.id,
    profile: {
      id: row.profile_id!,
      version: row.profile_version!,
    },
    tenantId: row.tenant_id,
    triggerId: row.trigger_id,
    version: row.version,
  };
}

const BINDING_SELECT = `SELECT binding.*, profile.profile_id, profile.version AS profile_version
  FROM dispatch_binding_versions AS binding
  JOIN dispatch_agent_profile_versions AS profile
    ON profile.id = binding.profile_version_id AND profile.tenant_id = binding.tenant_id`;

function eventSummary(row: EventRow): AdmissionResult["event"] {
  return {
    admissionHash: row.admission_hash,
    admittedAt: row.ingested_at.toISOString(),
    eventId: row.event_id,
    id: row.id,
    source: row.source,
    sourceDeduplicationKey: row.source_deduplication_key,
    tenantId: row.tenant_id,
    triggerId: row.trigger_id,
    type: row.type,
  };
}

function eventFromRow(row: EventRow): NormalizedCloudEvent {
  return normalizedCloudEventSchema.parse({
    specversion: "1.0",
    id: row.event_id,
    source: row.source,
    type: row.type,
    ...(row.subject ? { subject: row.subject } : {}),
    ...(row.event_time ? { time: row.event_time.toISOString() } : {}),
    datacontenttype: row.data_content_type,
    ...(row.data_schema ? { dataschema: row.data_schema } : {}),
    data: row.data,
    ...row.extensions,
  });
}

function resolveCorrelation(
  projections: readonly { name: string; path: string }[],
  data: JsonValue,
): Record<string, JsonPrimitive> {
  const correlation: Record<string, JsonPrimitive> = {};
  for (const item of projections) {
    const resolved = resolveJsonPointer(data, item.path);
    if (!resolved.found || (resolved.value !== null && typeof resolved.value === "object")
      || Buffer.byteLength(JSON.stringify(resolved.value), "utf8") > 1_024) {
      throw new Error(`Invalid busy-wake correlation ${item.name}`);
    }
    correlation[item.name] = resolved.value;
  }
  return correlation;
}

function wakeContextLock(tenantId: string, name: string, correlation: Record<string, JsonPrimitive>): string {
  return `wake-context:${tenantId}:${name}:${hashCanonicalJson(correlation)}`;
}

function jsonObject(value: unknown, name: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error(`${name} must be an object`);
  return value as Record<string, unknown>;
}

function validateGitHubPullRequestUrl(value: string, repositoryFullName: string, pullRequestNumber: number): void {
  let url: URL;
  try { url = new URL(value); } catch { throw new Error("GitHub pull request URL is invalid"); }
  if (url.protocol !== "https:" || url.hostname.toLowerCase() !== "github.com" || url.username || url.password || url.search || url.hash
    || url.pathname.toLowerCase() !== `/${repositoryFullName}/pull/${pullRequestNumber}`.toLowerCase()) {
    throw new Error("GitHub pull request URL does not match the registered repository and number");
  }
}

async function acquireWakeContextLocks(client: pg.PoolClient, command: AdmissionCommand, rows: BindingProfileRow[]): Promise<void> {
  const keys: string[] = [];
  for (const row of rows) {
    const binding = bindingFromRow(row);
    if (!matchesBinding(binding, command.event)) continue;
    if ("disposition" in binding.definition) {
      const correlation = projectWakeCorrelation(binding.definition, command.event);
      if (correlation) keys.push(wakeContextLock(command.tenantId, binding.definition.wake.waitName, correlation));
      continue;
    }
    const wait = binding.definition.afterTurn?.wait;
    if (wait?.admitWhileBusy) {
      const eventCorrelation = wait.correlation.filter((item): item is Extract<typeof item, { path: string }> => "path" in item);
      if (eventCorrelation.length === wait.correlation.length) {
        keys.push(wakeContextLock(command.tenantId, wait.name, resolveCorrelation(eventCorrelation, command.event.data)));
      }
    }
  }
  for (const key of [...new Set(keys)].sort()) {
    await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [key]);
  }
}

function revisionResolutionFromRow(row: RevisionResolutionRow): ClaimedRevisionResolution {
  if (row.provider !== "github" || row.state !== "LEASED" || !row.lease_owner || !row.lease_token || !row.lease_expires_at) {
    throw new Error(`Revision resolution ${row.event_id} has invalid lease state`);
  }
  const installationId = Number(row.installation_id);
  const repositoryId = Number(row.repository_id);
  if (!Number.isSafeInteger(installationId) || installationId < 1 || !Number.isSafeInteger(repositoryId) || repositoryId < 1) {
    throw new Error(`Revision resolution ${row.event_id} has invalid GitHub IDs`);
  }
  return {
    attempt: row.attempt,
    branch: row.branch,
    cloneUrl: row.clone_url,
    eventId: row.event_id,
    installationId,
    leaseOwner: row.lease_owner,
    leaseToken: row.lease_token,
    provider: "github",
    repositoryFullName: row.repository_full_name,
    repositoryId,
    tenantId: row.tenant_id,
  };
}

const EXECUTION_SELECT = `SELECT execution.*, current_input.input AS input, current_input.workspace AS workspace,
  binding.binding_id, binding.version AS binding_version,
  profile.profile_id, profile.version AS profile_version
  FROM dispatch_executions AS execution
  JOIN dispatch_execution_inputs AS current_input
    ON current_input.execution_id = execution.id AND current_input.sequence = execution.current_input_sequence
  JOIN dispatch_binding_versions AS binding ON binding.id = execution.binding_version_id AND binding.tenant_id = execution.tenant_id
  JOIN dispatch_agent_profile_versions AS profile ON profile.id = execution.profile_version_id AND profile.tenant_id = execution.tenant_id`;

async function loadEventExecutions(client: pg.PoolClient, tenantId: string, eventId: string): Promise<Execution[]> {
  const result = await client.query<ExecutionJoinedRow>(EXECUTION_SELECT + ` WHERE execution.tenant_id = $1 AND execution.event_id = $2
    ORDER BY binding.binding_id, binding.version, execution.id`, [tenantId, eventId]);
  return result.rows.map(executionRecordFromJoined);
}

async function loadEventWakes(client: pg.PoolClient, tenantId: string, eventId: string): Promise<AdmissionWakeResult[]> {
  const result = await client.query<EventWakeRow>(`SELECT wake.*, binding.binding_id, binding.version AS binding_version
    FROM dispatch_event_wakes AS wake
    JOIN dispatch_binding_versions AS binding ON binding.id = wake.binding_version_id AND binding.tenant_id = wake.tenant_id
    WHERE wake.tenant_id = $1 AND wake.event_id = $2 AND wake.wake_intent_id IS NULL AND wake.offer_id IS NULL
    ORDER BY wake.execution_id, wake.event_wait_id, wake.id`, [tenantId, eventId]);
  return result.rows.map((row) => ({
    action: row.action,
    binding: { id: row.binding_id, version: row.binding_version },
    consumedAt: row.consumed_at.toISOString(),
    eventWaitId: row.event_wait_id,
    executionId: row.execution_id,
    id: row.id,
    inputSequence: row.input_sequence,
    state: row.to_state,
  }));
}

async function loadEventWakeIntents(client: pg.PoolClient, tenantId: string, eventId: string): Promise<PendingWakeResult[]> {
  const result = await client.query<EventWakeIntentRow>(`SELECT intent.*, binding.binding_id, binding.version AS binding_version
    FROM dispatch_event_wake_intents intent
    JOIN dispatch_binding_versions binding ON binding.id=intent.binding_version_id AND binding.tenant_id=intent.tenant_id
    WHERE intent.tenant_id=$1 AND intent.event_id=$2 AND intent.offer_id IS NULL ORDER BY intent.execution_id, intent.id`, [tenantId, eventId]);
  return result.rows.map((row) => ({
    id: row.id, executionId: row.execution_id, binding: { id: row.binding_id, version: row.binding_version },
    action: row.action, disposition: row.disposition, admittedAt: row.admitted_at.toISOString(),
  }));
}

function executionRecordFromJoined(row: ExecutionJoinedRow): Execution {
  if (!isExecutionState(row.state)) throw new PersistedExecutionCorruptionError(row.id, "state");
  return {
    binding: { id: row.binding_id, version: row.binding_version },
    createdAt: row.created_at.toISOString(),
    eventId: row.event_id,
    id: row.id,
    input: row.input as Execution["input"],
    profile: { id: row.profile_id, version: row.profile_version },
    result: (row.result as Execution["result"]) ?? null,
    state: row.state,
    tenantId: row.tenant_id,
    updatedAt: row.updated_at.toISOString(),
    workspace: persistedWorkspace(row.id, row.workspace),
  };
}

function executionAttemptFromRow(executionId: string, row: ExecutionAttemptRow): ExecutionAttempt {
  if (!isAttemptState(row.state)) throw new PersistedExecutionCorruptionError(executionId, "attempt state");
  return {
    attempt: row.attempt,
    finishedAt: row.finished_at?.toISOString() ?? null,
    leaseExpiresAt: row.lease_expires_at?.toISOString() ?? null,
    opencodeSessionId: row.opencode_session_id,
    startedAt: row.started_at?.toISOString() ?? null,
    state: row.state,
    workloadName: row.workload_name,
    diagnostic: row.diagnostic,
  };
}

function executionTransitionFromRow(executionId: string, row: ExecutionTransitionRow): ExecutionStateTransition {
  if ((row.from_state !== null && !isExecutionState(row.from_state)) || !isExecutionState(row.to_state)) {
    throw new PersistedExecutionCorruptionError(executionId, "transition state");
  }
  return {
    actor: row.actor,
    attempt: row.attempt,
    createdAt: row.created_at.toISOString(),
    fromState: row.from_state,
    id: row.id,
    reason: row.reason,
    sequence: row.sequence,
    toState: row.to_state,
    traceContext: row.trace_context,
  };
}

function eventWaitFromRow(executionId: string, row: EventWaitRow): import("../execution/types.js").ExecutionEventWait {
  if (!(["PENDING_CONTEXT", "ACTIVE", "CANCELLED", "EXPIRED", "CONSUMED"] as const).includes(row.state as any)
    || row.correlation === null || typeof row.correlation !== "object" || Array.isArray(row.correlation)
    || Object.values(row.correlation).some((value) => value !== null && typeof value === "object")) {
    throw new PersistedExecutionCorruptionError(executionId, "event wait");
  }
  return {
    activatedAt: row.activated_at.toISOString(),
    attempt: row.attempt,
    correlation: row.correlation,
    deadlineAt: row.deadline_at.toISOString(),
    endedAt: row.ended_at?.toISOString() ?? null,
    id: row.id,
    name: row.name,
    state: row.state as import("../execution/types.js").EventWaitState,
  };
}

function persistedWorkspace(executionId: string, value: unknown): Execution["workspace"] {
  const parsed = resolvedWorkspaceSchema.safeParse(value);
  if (!parsed.success) {
    throw new PersistedExecutionCorruptionError(executionId, "workspace", { cause: parsed.error });
  }
  return parsed.data;
}

function eventExtensions(event: AdmissionCommand["event"]): Record<string, unknown> {
  const core = new Set(["specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"]);
  return Object.fromEntries(Object.entries(event).filter(([key]) => !core.has(key)));
}

function triggerControlLock(tenantId: string, triggerId: string): string {
  return `control-trigger:${tenantId}:${triggerId}`;
}

function profileTimeoutSeconds(definition: Record<string, unknown>): number {
  const value = definition.timeoutSeconds;
  if (!Number.isInteger(value) || (value as number) < 1 || (value as number) > 86_400) {
    throw new Error("Published profile has an invalid timeoutSeconds value");
  }
  return value as number;
}

function assertNonnegativeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${name} must be a nonnegative safe integer`);
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${name} must be a positive safe integer`);
}
