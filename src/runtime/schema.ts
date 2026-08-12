import { sql } from "drizzle-orm";
import type { Connection } from "../connection/index.js";
import type { BindingDefinition, TriggerConfig, TriggerDefinition } from "../control/types.js";
import type { JsonPrimitive } from "../json.js";
import {
  boolean,
  check,
  foreignKey,
  index,
  integer,
  jsonb,
  pgTable,
  primaryKey,
  text,
  timestamp,
  unique,
  uniqueIndex,
} from "drizzle-orm/pg-core";

export const agentProfileVersions = pgTable("dispatch_agent_profile_versions", {
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  definition: jsonb("definition").$type<import("../execution/types.js").AgentProfileDefinition>().notNull(),
  id: text("id").primaryKey(),
  profileID: text("profile_id").notNull(),
  tenantID: text("tenant_id").notNull(),
  version: integer("version").notNull(),
}, (table) => [
  check("dispatch_agent_profile_versions_version_positive", sql`${table.version} > 0`),
  uniqueIndex("dispatch_agent_profile_versions_profile_version_unique").on(table.tenantID, table.profileID, table.version),
  unique("dispatch_agent_profile_versions_id_tenant_unique").on(table.id, table.tenantID),
  index("dispatch_agent_profile_versions_tenant_profile_idx").on(table.tenantID, table.profileID),
]);

export const connections = pgTable("dispatch_connections", {
  connectionID: text("connection_id").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  id: text("id").primaryKey(),
  tenantID: text("tenant_id").notNull(),
  type: text("type").$type<Connection["connection"]["type"]>().notNull(),
}, (table) => [
  unique("dispatch_connections_id_tenant_unique").on(table.id, table.tenantID),
  uniqueIndex("dispatch_connections_tenant_connection_unique").on(table.tenantID, table.connectionID),
  index("dispatch_connections_tenant_type_idx").on(table.tenantID, table.type),
]);

export const agentProfileVersionConnections = pgTable("dispatch_agent_profile_version_connections", {
  connectionID: text("connection_id").notNull(),
  ordinal: integer("ordinal").notNull(),
  profileVersionID: text("profile_version_id").notNull(),
  sidecar: text("sidecar").notNull(),
  tenantID: text("tenant_id").notNull(),
}, (table) => [
  check("dispatch_agent_profile_version_connections_ordinal_nonnegative", sql`${table.ordinal} >= 0`),
  foreignKey({
    columns: [table.profileVersionID, table.tenantID],
    foreignColumns: [agentProfileVersions.id, agentProfileVersions.tenantID],
    name: "dispatch_agent_profile_version_connections_profile_tenant_fk",
  }),
  foreignKey({
    columns: [table.connectionID, table.tenantID],
    foreignColumns: [connections.id, connections.tenantID],
    name: "dispatch_agent_profile_version_connections_connection_tenant_fk",
  }),
  uniqueIndex("dispatch_agent_profile_version_connections_ordinal_unique")
    .on(table.tenantID, table.profileVersionID, table.ordinal),
  uniqueIndex("dispatch_agent_profile_version_connections_connection_unique")
    .on(table.tenantID, table.profileVersionID, table.connectionID),
]);

export const triggers = pgTable("dispatch_triggers", {
  config: jsonb("config").$type<TriggerConfig>().notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  disabledAt: timestamp("disabled_at", { withTimezone: true }),
  enabled: boolean("enabled").notNull().default(true),
  id: text("id").primaryKey(),
  tenantID: text("tenant_id").notNull(),
  type: text("type").$type<TriggerDefinition["type"]>().notNull(),
}, (table) => [
  check("dispatch_triggers_enabled_lifecycle_consistent", sql`${table.enabled} = (${table.disabledAt} IS NULL)`),
  unique("dispatch_triggers_id_tenant_unique").on(table.id, table.tenantID),
  index("dispatch_triggers_tenant_type_enabled_idx").on(table.tenantID, table.type, table.enabled),
]);

export const scheduleStates = pgTable("dispatch_schedule_states", {
  tenantID: text("tenant_id").notNull(),
  triggerID: text("trigger_id").notNull(),
  nextFireAt: timestamp("next_fire_at", { withTimezone: true }).notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  primaryKey({ columns: [table.tenantID, table.triggerID], name: "dispatch_schedule_states_pk" }),
  foreignKey({ columns: [table.triggerID, table.tenantID], foreignColumns: [triggers.id, triggers.tenantID], name: "dispatch_schedule_states_trigger_fk" }).onDelete("cascade"),
  index("dispatch_schedule_states_due_idx").on(table.nextFireAt),
]);

export const scheduleOccurrences = pgTable("dispatch_schedule_occurrences", {
  id: text("id").primaryKey(),
  tenantID: text("tenant_id").notNull(),
  triggerID: text("trigger_id").notNull(),
  scheduledAt: timestamp("scheduled_at", { withTimezone: true }).notNull(),
  state: text("state").notNull().default("PENDING"),
  attempt: integer("attempt").notNull().default(0),
  availableAt: timestamp("available_at", { withTimezone: true }).notNull().defaultNow(),
  leaseOwner: text("lease_owner"),
  leaseToken: text("lease_token"),
  leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true }),
  lastError: text("last_error"),
  completedAt: timestamp("completed_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  foreignKey({ columns: [table.triggerID, table.tenantID], foreignColumns: [triggers.id, triggers.tenantID], name: "dispatch_schedule_occurrences_trigger_fk" }).onDelete("cascade"),
  uniqueIndex("dispatch_schedule_occurrences_trigger_time_unique").on(table.tenantID, table.triggerID, table.scheduledAt),
  index("dispatch_schedule_occurrences_available_idx").on(table.state, table.availableAt),
  check("dispatch_schedule_occurrences_state", sql`${table.state} IN ('PENDING','LEASED','RETRY_WAIT','SUCCEEDED','DEAD_LETTERED')`),
  check("dispatch_schedule_occurrences_attempt_nonnegative", sql`${table.attempt} >= 0`),
]);

export const bindingVersions = pgTable("dispatch_binding_versions", {
  bindingID: text("binding_id").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  definition: jsonb("definition").$type<Omit<BindingDefinition, "eventTypes">>().notNull(),
  disabledAt: timestamp("disabled_at", { withTimezone: true }),
  enabled: boolean("enabled").notNull().default(true),
  eventTypes: text("event_types").array().notNull(),
  id: text("id").primaryKey(),
  profileVersionID: text("profile_version_id").notNull(),
  tenantID: text("tenant_id").notNull(),
  triggerID: text("trigger_id").notNull(),
  version: integer("version").notNull(),
}, (table) => [
  check("dispatch_binding_versions_version_positive", sql`${table.version} > 0`),
  check("dispatch_binding_versions_event_types_nonempty", sql`cardinality(${table.eventTypes}) > 0`),
  check(
    "dispatch_binding_versions_enabled_lifecycle_consistent",
    sql`${table.enabled} = (${table.disabledAt} IS NULL)`,
  ),
  foreignKey({
    columns: [table.triggerID, table.tenantID],
    foreignColumns: [triggers.id, triggers.tenantID],
    name: "dispatch_binding_versions_trigger_tenant_fk",
  }),
  foreignKey({
    columns: [table.profileVersionID, table.tenantID],
    foreignColumns: [agentProfileVersions.id, agentProfileVersions.tenantID],
    name: "dispatch_binding_versions_profile_version_tenant_fk",
  }),
  uniqueIndex("dispatch_binding_versions_binding_version_unique").on(table.tenantID, table.bindingID, table.version),
  uniqueIndex("dispatch_binding_versions_one_enabled_unique")
    .on(table.tenantID, table.bindingID)
    .where(sql`${table.enabled}`),
  unique("dispatch_binding_versions_id_tenant_unique").on(table.id, table.tenantID),
  index("dispatch_binding_versions_match_idx").on(table.tenantID, table.triggerID, table.enabled),
]);

export const events = pgTable("dispatch_events", {
  admissionHash: text("admission_hash").notNull(),
  data: jsonb("data").$type<unknown>().notNull(),
  dataContentType: text("data_content_type").notNull().default("application/json"),
  dataSchema: text("data_schema"),
  eventID: text("event_id").notNull(),
  eventTime: timestamp("event_time", { withTimezone: true }),
  extensions: jsonb("extensions").$type<Record<string, unknown>>().notNull().default({}),
  id: text("id").primaryKey(),
  ingestedAt: timestamp("ingested_at", { withTimezone: true }).notNull().defaultNow(),
  normalizationVersion: integer("normalization_version").notNull().default(1),
  rawPayloadRef: text("raw_payload_ref"),
  source: text("source").notNull(),
  sourceDeduplicationKey: text("source_deduplication_key").notNull(),
  specVersion: text("spec_version").notNull().default("1.0"),
  subject: text("subject"),
  tenantID: text("tenant_id").notNull(),
  triggerID: text("trigger_id").notNull(),
  type: text("type").notNull(),
}, (table) => [
  check("dispatch_events_normalization_version_positive", sql`${table.normalizationVersion} > 0`),
  check("dispatch_events_spec_version_1", sql`${table.specVersion} = '1.0'`),
  foreignKey({
    columns: [table.triggerID, table.tenantID],
    foreignColumns: [triggers.id, triggers.tenantID],
    name: "dispatch_events_trigger_tenant_fk",
  }),
  uniqueIndex("dispatch_events_trigger_source_event_unique").on(table.tenantID, table.triggerID, table.source, table.eventID),
  uniqueIndex("dispatch_events_trigger_source_dedup_unique").on(table.tenantID, table.triggerID, table.sourceDeduplicationKey),
  unique("dispatch_events_id_tenant_unique").on(table.id, table.tenantID),
  index("dispatch_events_tenant_trigger_ingested_idx").on(table.tenantID, table.triggerID, table.ingestedAt),
  index("dispatch_events_tenant_type_ingested_idx").on(table.tenantID, table.type, table.ingestedAt),
]);

export const eventRevisionResolutions = pgTable("dispatch_event_revision_resolutions", {
  attempt: integer("attempt").notNull().default(0),
  availableAt: timestamp("available_at", { withTimezone: true }).notNull().defaultNow(),
  branch: text("branch").notNull(),
  cloneUrl: text("clone_url").notNull(),
  commit: text("commit"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  eventID: text("event_id").notNull(),
  installationID: text("installation_id").notNull(),
  lastError: text("last_error"),
  leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true }),
  leaseOwner: text("lease_owner"),
  leaseToken: text("lease_token"),
  provider: text("provider").notNull(),
  repositoryFullName: text("repository_full_name").notNull(),
  repositoryID: text("repository_id").notNull(),
  resolvedAt: timestamp("resolved_at", { withTimezone: true }),
  state: text("state").notNull().default("PENDING"),
  tenantID: text("tenant_id").notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  primaryKey({ columns: [table.eventID, table.tenantID] }),
  check("dispatch_event_revision_resolutions_attempt_nonnegative", sql`${table.attempt} >= 0`),
  check("dispatch_event_revision_resolutions_provider_github", sql`${table.provider} = 'github'`),
  check("dispatch_event_revision_resolutions_state_valid", sql`${table.state} IN ('PENDING', 'LEASED', 'RETRY_WAIT', 'SUCCEEDED', 'DEAD_LETTERED')`),
  check("dispatch_event_revision_resolutions_commit_valid", sql`${table.commit} IS NULL OR ${table.commit} ~ '^[0-9a-f]{40}$'`),
  check("dispatch_event_revision_resolutions_lease_consistent", sql`(${table.leaseOwner} IS NULL AND ${table.leaseToken} IS NULL AND ${table.leaseExpiresAt} IS NULL) OR (${table.leaseOwner} IS NOT NULL AND ${table.leaseToken} IS NOT NULL AND ${table.leaseExpiresAt} IS NOT NULL AND ${table.state} = 'LEASED')`),
  foreignKey({
    columns: [table.eventID, table.tenantID],
    foreignColumns: [events.id, events.tenantID],
    name: "dispatch_event_revision_resolutions_event_tenant_fk",
  }),
  uniqueIndex("dispatch_event_revision_resolutions_lease_token_unique").on(table.leaseToken).where(sql`${table.leaseToken} IS NOT NULL`),
  index("dispatch_event_revision_resolutions_claim_idx")
    .on(table.availableAt, table.createdAt, table.eventID)
    .where(sql`${table.state} IN ('PENDING', 'RETRY_WAIT', 'LEASED')`),
]);

export const eventRevisionResolutionCandidates = pgTable("dispatch_event_revision_resolution_candidates", {
  bindingVersionID: text("binding_version_id").notNull(),
  eventID: text("event_id").notNull(),
  tenantID: text("tenant_id").notNull(),
}, (table) => [
  primaryKey({
    columns: [table.eventID, table.tenantID, table.bindingVersionID],
    name: "dispatch_event_revision_resolution_candidates_pk",
  }),
  foreignKey({
    columns: [table.eventID, table.tenantID],
    foreignColumns: [eventRevisionResolutions.eventID, eventRevisionResolutions.tenantID],
    name: "dispatch_event_revision_resolution_candidates_resolution_fk",
  }).onDelete("cascade"),
  foreignKey({
    columns: [table.bindingVersionID, table.tenantID],
    foreignColumns: [bindingVersions.id, bindingVersions.tenantID],
    name: "dispatch_event_revision_resolution_candidates_binding_fk",
  }),
]);

export const executions = pgTable("dispatch_executions", {
  availableAt: timestamp("available_at", { withTimezone: true }).notNull().defaultNow(),
  activeSingletonKey: text("active_singleton_key"),
  activeSingletonName: text("active_singleton_name"),
  bindingVersionID: text("binding_version_id").notNull(),
  completedAt: timestamp("completed_at", { withTimezone: true }),
  currentInputSequence: integer("current_input_sequence").notNull().default(1),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  eventID: text("event_id").notNull(),
  id: text("id").primaryKey(),
  idempotencyKey: text("idempotency_key").notNull(),
  input: jsonb("input").$type<unknown>().notNull(),
  profileVersionID: text("profile_version_id").notNull(),
  requestHash: text("request_hash").notNull(),
  resolvedPolicy: jsonb("resolved_policy").$type<Record<string, unknown>>().notNull(),
  result: jsonb("result").$type<unknown>(),
  state: text("state").notNull().default("QUEUED"),
  tenantID: text("tenant_id").notNull(),
  timeoutAt: timestamp("timeout_at", { withTimezone: true }).notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  workspace: jsonb("workspace").$type<import("../workspace/types.js").ResolvedWorkspace>().notNull().default({ type: "empty" }),
}, (table) => [
  check("dispatch_executions_state_valid", sql`${table.state} IN ('RECEIVED', 'PLANNED', 'QUEUED', 'PROVISIONING', 'RUNNING', 'WAITING', 'SUCCEEDED', 'DELIVERING', 'COMPLETED', 'RETRY_WAIT', 'AWAITING_APPROVAL', 'CANCEL_REQUESTED', 'CANCELLED', 'TIMED_OUT', 'FAILED', 'DEAD_LETTERED')`),
  check("dispatch_executions_current_input_sequence_positive", sql`${table.currentInputSequence} > 0`),
  check("dispatch_executions_active_singleton_pair", sql`(${table.activeSingletonName} IS NULL) = (${table.activeSingletonKey} IS NULL)`),
  foreignKey({
    columns: [table.bindingVersionID, table.tenantID],
    foreignColumns: [bindingVersions.id, bindingVersions.tenantID],
    name: "dispatch_executions_binding_version_tenant_fk",
  }),
  foreignKey({
    columns: [table.eventID, table.tenantID],
    foreignColumns: [events.id, events.tenantID],
    name: "dispatch_executions_event_tenant_fk",
  }),
  foreignKey({
    columns: [table.profileVersionID, table.tenantID],
    foreignColumns: [agentProfileVersions.id, agentProfileVersions.tenantID],
    name: "dispatch_executions_profile_version_tenant_fk",
  }),
  uniqueIndex("dispatch_executions_tenant_idempotency_unique").on(table.tenantID, table.idempotencyKey),
  uniqueIndex("dispatch_executions_tenant_event_binding_unique").on(table.tenantID, table.eventID, table.bindingVersionID),
  uniqueIndex("dispatch_executions_active_singleton_unique")
    .on(table.tenantID, table.activeSingletonName, table.activeSingletonKey)
    .where(sql`${table.activeSingletonKey} IS NOT NULL AND ${table.state} NOT IN ('SUCCEEDED', 'COMPLETED', 'CANCELLED', 'TIMED_OUT', 'FAILED', 'DEAD_LETTERED')`),
  unique("dispatch_executions_id_tenant_unique").on(table.id, table.tenantID),
  index("dispatch_executions_tenant_binding_created_idx").on(table.tenantID, table.bindingVersionID, table.createdAt),
  index("dispatch_executions_tenant_event_idx").on(table.tenantID, table.eventID),
  index("dispatch_executions_tenant_state_created_idx").on(table.tenantID, table.state, table.createdAt),
  index("dispatch_executions_state_timeout_idx").on(table.state, table.timeoutAt),
  index("dispatch_executions_dispatch_idx")
    .on(table.availableAt, table.createdAt, table.id)
    .where(sql`${table.state} = 'QUEUED'`),
]);

export const executionCheckpoints = pgTable("dispatch_execution_checkpoints", {
  tenantID: text("tenant_id").notNull(),
  bindingID: text("binding_id").notNull(),
  checkpointName: text("checkpoint_name").notNull(),
  checkpointKeyHash: text("checkpoint_key_hash").notNull(),
  checkpointKeyValues: jsonb("checkpoint_key_values").$type<JsonPrimitive[]>().notNull(),
  value: jsonb("value").$type<JsonPrimitive>().notNull(),
  advancedByExecutionID: text("advanced_by_execution_id").notNull(),
  advancedAt: timestamp("advanced_at", { withTimezone: true }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull(),
}, (table) => [
  primaryKey({ columns: [table.tenantID, table.bindingID, table.checkpointName, table.checkpointKeyHash], name: "dispatch_execution_checkpoints_pk" }),
  foreignKey({ columns: [table.advancedByExecutionID, table.tenantID], foreignColumns: [executions.id, executions.tenantID], name: "dispatch_execution_checkpoints_execution_fk" }),
  check("dispatch_execution_checkpoints_key_hash_valid", sql`${table.checkpointKeyHash} ~ '^[0-9a-f]{64}$'`),
]);

export const executionCheckpointAdvances = pgTable("dispatch_execution_checkpoint_advances", {
  executionID: text("execution_id").primaryKey(),
  tenantID: text("tenant_id").notNull(),
  bindingID: text("binding_id").notNull(),
  checkpointName: text("checkpoint_name").notNull(),
  checkpointKeyHash: text("checkpoint_key_hash").notNull(),
  checkpointKeyValues: jsonb("checkpoint_key_values").$type<JsonPrimitive[]>().notNull(),
  expectedPreviousExists: boolean("expected_previous_exists").notNull(),
  expectedPreviousValue: jsonb("expected_previous_value").$type<JsonPrimitive>(),
  targetValue: jsonb("target_value").$type<JsonPrimitive>().notNull(),
  state: text("state").notNull().default("PENDING"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
  appliedAt: timestamp("applied_at", { withTimezone: true }),
}, (table) => [
  foreignKey({ columns: [table.executionID, table.tenantID], foreignColumns: [executions.id, executions.tenantID], name: "dispatch_execution_checkpoint_advances_execution_fk" }).onDelete("cascade"),
  check("dispatch_execution_checkpoint_advances_state", sql`${table.state} IN ('PENDING','APPLIED','SUPERSEDED')`),
  check("dispatch_execution_checkpoint_advances_expected_consistent", sql`${table.expectedPreviousExists} = (${table.expectedPreviousValue} IS NOT NULL)`),
  check("dispatch_execution_checkpoint_advances_key_hash_valid", sql`${table.checkpointKeyHash} ~ '^[0-9a-f]{64}$'`),
  index("dispatch_execution_checkpoint_advances_identity_idx").on(table.tenantID, table.bindingID, table.checkpointName, table.checkpointKeyHash),
]);

export const githubPullRequestEffects = pgTable("dispatch_github_pull_request_effects", {
  attemptedAt: timestamp("attempted_at", { withTimezone: true }),
  confirmedAt: timestamp("confirmed_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
  executionID: text("execution_id").notNull(),
  fenceHash: text("fence_hash").notNull(),
  baseRef: text("base_ref").notNull(),
  githubPullRequestID: text("github_pull_request_id"),
  headRef: text("head_ref").notNull(),
  id: text("id").primaryKey(),
  openedEventID: text("opened_event_id"),
  pullRequestNumber: integer("pull_request_number"),
  pullRequestURL: text("pull_request_url"),
  pullRequestTitle: text("pull_request_title").notNull(),
  repositoryFullName: text("repository_full_name").notNull(),
  repositoryID: text("repository_id").notNull(),
  requestHash: text("request_hash").notNull(),
  state: text("state").notNull(),
  tenantID: text("tenant_id").notNull(),
}, (table) => [
  check("dispatch_github_pull_request_effects_state_valid", sql`${table.state} IN ('REGISTERED','REPORTED','CONFIRMED')`),
  check("dispatch_github_pull_request_effects_fence_hash_valid", sql`${table.fenceHash} ~ '^[0-9a-f]{64}$'`),
  check("dispatch_github_pull_request_effects_repository_id_valid", sql`${table.repositoryID} ~ '^[1-9][0-9]*$'`),
  check("dispatch_github_pull_request_effects_repository_name_bounded", sql`octet_length(${table.repositoryFullName}) BETWEEN 3 AND 255`),
  check("dispatch_github_pull_request_effects_request_hash_valid", sql`${table.requestHash} ~ '^[0-9a-f]{64}$'`),
  check("dispatch_github_pull_request_effects_pr_id_valid", sql`${table.githubPullRequestID} IS NULL OR ${table.githubPullRequestID} ~ '^[1-9][0-9]*$'`),
  check("dispatch_github_pull_request_effects_pr_number_valid", sql`${table.pullRequestNumber} IS NULL OR ${table.pullRequestNumber} > 0`),
  check("dispatch_github_pull_request_effects_url_bounded", sql`${table.pullRequestURL} IS NULL OR octet_length(${table.pullRequestURL}) BETWEEN 20 AND 2048`),
  check("dispatch_github_pull_request_effects_request_bounded", sql`octet_length(${table.pullRequestTitle}) BETWEEN 1 AND 4096 AND octet_length(${table.headRef}) BETWEEN 1 AND 255 AND octet_length(${table.baseRef}) BETWEEN 1 AND 255`),
  check("dispatch_github_pull_request_effects_identity_consistent", sql`(${table.state}='REGISTERED' AND ${table.githubPullRequestID} IS NULL AND ${table.pullRequestNumber} IS NULL AND ${table.pullRequestURL} IS NULL AND ${table.openedEventID} IS NULL AND ${table.confirmedAt} IS NULL) OR (${table.state}='REPORTED' AND ${table.githubPullRequestID} IS NOT NULL AND ${table.pullRequestNumber} IS NOT NULL AND ${table.pullRequestURL} IS NOT NULL AND ${table.openedEventID} IS NULL AND ${table.confirmedAt} IS NULL) OR (${table.state}='CONFIRMED' AND ${table.githubPullRequestID} IS NOT NULL AND ${table.pullRequestNumber} IS NOT NULL AND ${table.pullRequestURL} IS NOT NULL AND ${table.openedEventID} IS NOT NULL AND ${table.confirmedAt} IS NOT NULL)`),
  foreignKey({ columns: [table.executionID, table.tenantID], foreignColumns: [executions.id, executions.tenantID], name: "dispatch_github_pull_request_effects_execution_tenant_fk" }),
  foreignKey({ columns: [table.openedEventID, table.tenantID], foreignColumns: [events.id, events.tenantID], name: "dispatch_github_pull_request_effects_event_tenant_fk" }),
  uniqueIndex("dispatch_github_pull_request_effects_execution_unique").on(table.tenantID, table.executionID),
  uniqueIndex("dispatch_github_pull_request_effects_pr_id_unique").on(table.tenantID, table.repositoryID, table.githubPullRequestID).where(sql`${table.githubPullRequestID} IS NOT NULL`),
  uniqueIndex("dispatch_github_pull_request_effects_pr_number_unique").on(table.tenantID, table.repositoryID, table.pullRequestNumber).where(sql`${table.pullRequestNumber} IS NOT NULL`),
]);

export const executionInputs = pgTable("dispatch_execution_inputs", {
  createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
  eventID: text("event_id").notNull(),
  executionID: text("execution_id").notNull(),
  input: jsonb("input").$type<unknown>().notNull(),
  kind: text("kind").notNull(),
  sequence: integer("sequence").notNull(),
  tenantID: text("tenant_id").notNull(),
  workspace: jsonb("workspace").$type<import("../workspace/types.js").ResolvedWorkspace>().notNull(),
}, (table) => [
  primaryKey({ columns: [table.executionID, table.sequence] }),
  check("dispatch_execution_inputs_sequence_positive", sql`${table.sequence} > 0`),
  check("dispatch_execution_inputs_kind_valid", sql`${table.kind} IN ('INITIAL', 'WAKE')`),
  foreignKey({ columns: [table.executionID, table.tenantID], foreignColumns: [executions.id, executions.tenantID], name: "dispatch_execution_inputs_execution_tenant_fk" }),
  foreignKey({ columns: [table.eventID, table.tenantID], foreignColumns: [events.id, events.tenantID], name: "dispatch_execution_inputs_event_tenant_fk" }),
  uniqueIndex("dispatch_execution_inputs_initial_unique").on(table.executionID).where(sql`${table.kind} = 'INITIAL'`),
]);

export const executionWakeContexts = pgTable("dispatch_execution_wake_contexts", {
  correlation: jsonb("correlation").$type<Record<string, import("../json.js").JsonPrimitive>>().notNull().default({}),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
  executionID: text("execution_id").notNull(),
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  requiredNames: jsonb("required_names").$type<string[]>().notNull(),
  state: text("state").notNull(),
  tenantID: text("tenant_id").notNull(),
}, (table) => [
  check("dispatch_execution_wake_contexts_correlation_object", sql`jsonb_typeof(${table.correlation}) = 'object'`),
  check("dispatch_execution_wake_contexts_required_names_array", sql`jsonb_typeof(${table.requiredNames}) = 'array'`),
  check("dispatch_execution_wake_contexts_state_valid", sql`${table.state} IN ('BUILDING','READY')`),
  foreignKey({ columns: [table.executionID, table.tenantID], foreignColumns: [executions.id, executions.tenantID], name: "dispatch_execution_wake_contexts_execution_tenant_fk" }),
  uniqueIndex("dispatch_execution_wake_contexts_execution_unique").on(table.tenantID, table.executionID),
  uniqueIndex("dispatch_execution_wake_contexts_reference_unique").on(table.id, table.tenantID, table.executionID),
  index("dispatch_execution_wake_contexts_match_idx").on(table.tenantID, table.name),
]);

export const executionWakeContextValues = pgTable("dispatch_execution_wake_context_values", {
  authorityID: text("authority_id"),
  authorityType: text("authority_type").notNull(),
  contextID: text("context_id").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
  executionID: text("execution_id").notNull(),
  name: text("name").notNull(),
  tenantID: text("tenant_id").notNull(),
  value: jsonb("value").$type<import("../json.js").JsonPrimitive>().notNull(),
}, (table) => [
  primaryKey({ columns: [table.contextID, table.name] }),
  check("dispatch_execution_wake_context_values_authority_bounded", sql`octet_length(${table.authorityType}) BETWEEN 1 AND 255`),
  check("dispatch_execution_wake_context_values_primitive", sql`jsonb_typeof(${table.value}) IN ('null','boolean','number','string')`),
  foreignKey({ columns: [table.contextID, table.tenantID, table.executionID], foreignColumns: [executionWakeContexts.id, executionWakeContexts.tenantID, executionWakeContexts.executionID], name: "dispatch_execution_wake_context_values_context_fk" }),
  foreignKey({ columns: [table.executionID, table.tenantID], foreignColumns: [executions.id, executions.tenantID], name: "dispatch_execution_wake_context_values_execution_tenant_fk" }),
]);

export const eventWakeOffers = pgTable("dispatch_event_wake_offers", {
  action: text("action").notNull(),
  admittedAt: timestamp("admitted_at", { withTimezone: true }).notNull(),
  bindingVersionID: text("binding_version_id").notNull(),
  correlation: jsonb("correlation").$type<Record<string, import("../json.js").JsonPrimitive>>().notNull(),
  eventID: text("event_id").notNull(),
  id: text("id").primaryKey(),
  input: jsonb("input").$type<unknown>(),
  tenantID: text("tenant_id").notNull(),
  waitName: text("wait_name").notNull(),
  workspace: jsonb("workspace").$type<import("../workspace/types.js").ResolvedWorkspace>(),
}, (table) => [
  check("dispatch_event_wake_offers_action_valid", sql`${table.action} IN ('CONTINUED','COMPLETED')`),
  check("dispatch_event_wake_offers_payload_consistent", sql`(${table.action}='CONTINUED' AND ${table.input} IS NOT NULL) OR (${table.action}='COMPLETED' AND ${table.input} IS NULL AND ${table.workspace} IS NULL)`),
  foreignKey({ columns: [table.eventID, table.tenantID], foreignColumns: [events.id, events.tenantID], name: "dispatch_event_wake_offers_event_tenant_fk" }),
  foreignKey({ columns: [table.bindingVersionID, table.tenantID], foreignColumns: [bindingVersions.id, bindingVersions.tenantID], name: "dispatch_event_wake_offers_binding_tenant_fk" }),
  uniqueIndex("dispatch_event_wake_offers_event_binding_unique").on(table.tenantID, table.eventID, table.bindingVersionID),
  uniqueIndex("dispatch_event_wake_offers_reference_unique").on(table.id, table.tenantID, table.eventID, table.bindingVersionID, table.action),
  index("dispatch_event_wake_offers_match_idx").on(table.tenantID, table.waitName),
]);

export const eventWakeIntents = pgTable("dispatch_event_wake_intents", {
  action: text("action").notNull(),
  admittedAt: timestamp("admitted_at", { withTimezone: true }).notNull(),
  bindingVersionID: text("binding_version_id").notNull(),
  disposition: text("disposition").notNull(),
  eventID: text("event_id").notNull(),
  executionID: text("execution_id").notNull(),
  id: text("id").primaryKey(),
  input: jsonb("input").$type<unknown>(),
  offerID: text("offer_id"),
  tenantID: text("tenant_id").notNull(),
  workspace: jsonb("workspace").$type<import("../workspace/types.js").ResolvedWorkspace>(),
}, (table) => [
  check("dispatch_event_wake_intents_action_valid", sql`${table.action} IN ('CONTINUED', 'COMPLETED')`),
  check("dispatch_event_wake_intents_disposition_valid", sql`${table.disposition} IN ('PENDING', 'DOMINATED')`),
  check("dispatch_event_wake_intents_payload_consistent", sql`(${table.action} = 'CONTINUED' AND ${table.input} IS NOT NULL AND ${table.workspace} IS NOT NULL) OR (${table.action} = 'COMPLETED' AND ${table.input} IS NULL AND ${table.workspace} IS NULL)`),
  foreignKey({ columns: [table.eventID, table.tenantID], foreignColumns: [events.id, events.tenantID], name: "dispatch_event_wake_intents_event_tenant_fk" }),
  foreignKey({ columns: [table.executionID, table.tenantID], foreignColumns: [executions.id, executions.tenantID], name: "dispatch_event_wake_intents_execution_tenant_fk" }),
  foreignKey({ columns: [table.bindingVersionID, table.tenantID], foreignColumns: [bindingVersions.id, bindingVersions.tenantID], name: "dispatch_event_wake_intents_binding_tenant_fk" }),
  foreignKey({
    columns: [table.offerID, table.tenantID, table.eventID, table.bindingVersionID, table.action],
    foreignColumns: [eventWakeOffers.id, eventWakeOffers.tenantID, eventWakeOffers.eventID, eventWakeOffers.bindingVersionID, eventWakeOffers.action],
    name: "dispatch_event_wake_intents_offer_fk",
  }),
  uniqueIndex("dispatch_event_wake_intents_event_execution_unique").on(table.tenantID, table.eventID, table.executionID),
  uniqueIndex("dispatch_event_wake_intents_offer_execution_unique").on(table.offerID, table.executionID).where(sql`${table.offerID} IS NOT NULL`),
  uniqueIndex("dispatch_event_wake_intents_pending_reference_unique").on(table.id, table.tenantID, table.executionID),
  uniqueIndex("dispatch_event_wake_intents_applied_reference_unique").on(
    table.id, table.tenantID, table.eventID, table.executionID, table.bindingVersionID, table.action,
  ),
]);

export const executionPendingWakes = pgTable("dispatch_execution_pending_wakes", {
  executionID: text("execution_id").notNull(),
  intentID: text("intent_id").notNull(),
  tenantID: text("tenant_id").notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull(),
}, (table) => [
  primaryKey({ columns: [table.tenantID, table.executionID] }),
  foreignKey({ columns: [table.executionID, table.tenantID], foreignColumns: [executions.id, executions.tenantID], name: "dispatch_execution_pending_wakes_execution_tenant_fk" }),
  foreignKey({
    columns: [table.intentID, table.tenantID, table.executionID],
    foreignColumns: [eventWakeIntents.id, eventWakeIntents.tenantID, eventWakeIntents.executionID],
    name: "dispatch_execution_pending_wakes_intent_fk",
  }),
  uniqueIndex("dispatch_execution_pending_wakes_intent_unique").on(table.intentID),
]);

export const executionAttempts = pgTable("dispatch_execution_attempts", {
  attempt: integer("attempt").notNull(),
  diagnostic: jsonb("diagnostic").$type<import("../execution/types.js").ExecutionAttemptDiagnostic>(),
  executionID: text("execution_id").notNull(),
  fencingToken: text("fencing_token").notNull(),
  finishedAt: timestamp("finished_at", { withTimezone: true }),
  leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true }),
  leaseOwner: text("lease_owner"),
  opencodeSessionID: text("opencode_session_id"),
  startedAt: timestamp("started_at", { withTimezone: true }),
  state: text("state").notNull().default("PENDING"),
  tenantID: text("tenant_id").notNull(),
  workloadName: text("workload_name"),
}, (table) => [
  primaryKey({ columns: [table.executionID, table.attempt] }),
  check("dispatch_execution_attempts_attempt_positive", sql`${table.attempt} > 0`),
  check("dispatch_execution_attempts_diagnostic_bounded", sql`${table.diagnostic} IS NULL OR octet_length(${table.diagnostic}::text) <= 8192`),
  check("dispatch_execution_attempts_state_valid", sql`${table.state} IN ('PENDING', 'LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')`),
  check(
    "dispatch_execution_attempts_active_lease_consistent",
    sql`(${table.leaseOwner} IS NULL) = (${table.leaseExpiresAt} IS NULL) AND (${table.state} IN ('LEASED', 'RUNNING')) = (${table.leaseOwner} IS NOT NULL)`,
  ),
  check(
    "dispatch_execution_attempts_terminal_consistent",
    sql`(${table.state} IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')) = (${table.finishedAt} IS NOT NULL)`,
  ),
  foreignKey({
    columns: [table.executionID, table.tenantID],
    foreignColumns: [executions.id, executions.tenantID],
    name: "dispatch_execution_attempts_execution_tenant_fk",
  }),
  uniqueIndex("dispatch_execution_attempts_fencing_token_unique").on(table.fencingToken),
  uniqueIndex("dispatch_execution_attempts_one_active_unique")
    .on(table.executionID)
    .where(sql`${table.state} IN ('LEASED', 'RUNNING')`),
  index("dispatch_execution_attempts_expired_active_lease_idx")
    .on(table.leaseExpiresAt, table.executionID)
    .where(sql`${table.state} IN ('LEASED', 'RUNNING')`),
]);

export const eventWaits = pgTable("dispatch_event_waits", {
  activatedAt: timestamp("activated_at", { withTimezone: true }).notNull(),
  attempt: integer("attempt").notNull(),
  correlation: jsonb("correlation").$type<Record<string, import("../json.js").JsonPrimitive>>().notNull(),
  deadlineAt: timestamp("deadline_at", { withTimezone: true }).notNull(),
  endedAt: timestamp("ended_at", { withTimezone: true }),
  executionID: text("execution_id").notNull(),
  id: text("id").primaryKey(),
  name: text("name").notNull(),
  state: text("state").notNull().default("ACTIVE"),
  tenantID: text("tenant_id").notNull(),
}, (table) => [
  check("dispatch_event_waits_attempt_positive", sql`${table.attempt} > 0`),
  check("dispatch_event_waits_correlation_object", sql`jsonb_typeof(${table.correlation}) = 'object'`),
  check("dispatch_event_waits_correlation_bounded", sql`octet_length(${table.correlation}::text) <= 32768`),
  check("dispatch_event_waits_deadline_after_activation", sql`${table.deadlineAt} > ${table.activatedAt}`),
  check("dispatch_event_waits_end_after_activation", sql`${table.endedAt} IS NULL OR ${table.endedAt} >= ${table.activatedAt}`),
  check("dispatch_event_waits_state_valid", sql`${table.state} IN ('PENDING_CONTEXT', 'ACTIVE', 'CANCELLED', 'EXPIRED', 'CONSUMED')`),
  check("dispatch_event_waits_lifecycle_consistent", sql`(${table.state} IN ('PENDING_CONTEXT','ACTIVE')) = (${table.endedAt} IS NULL)`),
  foreignKey({
    columns: [table.executionID, table.tenantID],
    foreignColumns: [executions.id, executions.tenantID],
    name: "dispatch_event_waits_execution_tenant_fk",
  }),
  foreignKey({
    columns: [table.executionID, table.attempt],
    foreignColumns: [executionAttempts.executionID, executionAttempts.attempt],
    name: "dispatch_event_waits_attempt_fk",
  }),
  uniqueIndex("dispatch_event_waits_one_active_execution_unique").on(table.tenantID, table.executionID).where(sql`${table.state} IN ('PENDING_CONTEXT','ACTIVE')`),
  unique("dispatch_event_waits_id_tenant_execution_unique").on(table.id, table.tenantID, table.executionID),
  index("dispatch_event_waits_deadline_idx").on(table.deadlineAt, table.executionID).where(sql`${table.state} IN ('PENDING_CONTEXT','ACTIVE')`),
  index("dispatch_event_waits_name_idx").on(table.tenantID, table.name).where(sql`${table.state} = 'ACTIVE'`),
]);

export const eventWakes = pgTable("dispatch_event_wakes", {
  action: text("action").notNull(),
  bindingVersionID: text("binding_version_id").notNull(),
  consumedAt: timestamp("consumed_at", { withTimezone: true }).notNull(),
  eventID: text("event_id").notNull(),
  eventWaitID: text("event_wait_id"),
  wakeIntentID: text("wake_intent_id"),
  executionID: text("execution_id").notNull(),
  id: text("id").primaryKey(),
  inputSequence: integer("input_sequence"),
  offerID: text("offer_id"),
  tenantID: text("tenant_id").notNull(),
  toState: text("to_state").notNull(),
}, (table) => [
  check("dispatch_event_wakes_action_valid", sql`${table.action} IN ('CONTINUED', 'COMPLETED', 'CANCELLED')`),
  check("dispatch_event_wakes_lifecycle_consistent", sql`(${table.action} = 'CONTINUED' AND ${table.toState} = 'QUEUED' AND ${table.inputSequence} IS NOT NULL) OR (${table.action} = 'COMPLETED' AND ${table.toState} = 'COMPLETED' AND ${table.inputSequence} IS NULL) OR (${table.action} = 'CANCELLED' AND ${table.toState} = 'CANCELLED' AND ${table.inputSequence} IS NULL)`),
  check("dispatch_event_wakes_exactly_one_source", sql`(${table.eventWaitID} IS NOT NULL) <> (${table.wakeIntentID} IS NOT NULL)`),
  foreignKey({ columns: [table.bindingVersionID, table.tenantID], foreignColumns: [bindingVersions.id, bindingVersions.tenantID], name: "dispatch_event_wakes_binding_tenant_fk" }),
  foreignKey({ columns: [table.eventID, table.tenantID], foreignColumns: [events.id, events.tenantID], name: "dispatch_event_wakes_event_tenant_fk" }),
  foreignKey({ columns: [table.executionID, table.tenantID], foreignColumns: [executions.id, executions.tenantID], name: "dispatch_event_wakes_execution_tenant_fk" }),
  foreignKey({
    columns: [table.offerID, table.tenantID, table.eventID, table.bindingVersionID, table.action],
    foreignColumns: [eventWakeOffers.id, eventWakeOffers.tenantID, eventWakeOffers.eventID, eventWakeOffers.bindingVersionID, eventWakeOffers.action],
    name: "dispatch_event_wakes_offer_fk",
  }),
  foreignKey({ columns: [table.eventWaitID, table.tenantID, table.executionID], foreignColumns: [eventWaits.id, eventWaits.tenantID, eventWaits.executionID], name: "dispatch_event_wakes_wait_execution_fk" }),
  foreignKey({
    columns: [table.wakeIntentID, table.tenantID, table.eventID, table.executionID, table.bindingVersionID, table.action],
    foreignColumns: [eventWakeIntents.id, eventWakeIntents.tenantID, eventWakeIntents.eventID, eventWakeIntents.executionID, eventWakeIntents.bindingVersionID, eventWakeIntents.action],
    name: "dispatch_event_wakes_intent_fk",
  }),
  foreignKey({ columns: [table.executionID, table.inputSequence], foreignColumns: [executionInputs.executionID, executionInputs.sequence], name: "dispatch_event_wakes_input_fk" }),
  uniqueIndex("dispatch_event_wakes_wait_unique").on(table.eventWaitID).where(sql`${table.eventWaitID} IS NOT NULL`),
  uniqueIndex("dispatch_event_wakes_intent_unique").on(table.wakeIntentID).where(sql`${table.wakeIntentID} IS NOT NULL`),
  uniqueIndex("dispatch_event_wakes_offer_execution_unique").on(table.offerID, table.executionID).where(sql`${table.offerID} IS NOT NULL`),
  index("dispatch_event_wakes_event_idx").on(table.tenantID, table.eventID, table.executionID),
]);

export const executionTransitions = pgTable("dispatch_execution_transitions", {
  actor: text("actor").notNull(),
  attempt: integer("attempt"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  executionID: text("execution_id").notNull(),
  fromState: text("from_state"),
  id: text("id").primaryKey(),
  reason: text("reason"),
  sequence: integer("sequence").notNull(),
  tenantID: text("tenant_id").notNull(),
  toState: text("to_state").notNull(),
  traceContext: jsonb("trace_context").$type<Record<string, string>>().notNull().default({}),
}, (table) => [
  check("dispatch_execution_transitions_attempt_positive", sql`${table.attempt} IS NULL OR ${table.attempt} > 0`),
  check("dispatch_execution_transitions_sequence_positive", sql`${table.sequence} > 0`),
  check("dispatch_execution_transitions_to_state_valid", sql`${table.toState} IN ('RECEIVED', 'PLANNED', 'QUEUED', 'PROVISIONING', 'RUNNING', 'WAITING', 'SUCCEEDED', 'DELIVERING', 'COMPLETED', 'RETRY_WAIT', 'AWAITING_APPROVAL', 'CANCEL_REQUESTED', 'CANCELLED', 'TIMED_OUT', 'FAILED', 'DEAD_LETTERED')`),
  check("dispatch_execution_transitions_from_state_valid", sql`${table.fromState} IS NULL OR ${table.fromState} IN ('RECEIVED', 'PLANNED', 'QUEUED', 'PROVISIONING', 'RUNNING', 'WAITING', 'SUCCEEDED', 'DELIVERING', 'COMPLETED', 'RETRY_WAIT', 'AWAITING_APPROVAL', 'CANCEL_REQUESTED', 'CANCELLED', 'TIMED_OUT', 'FAILED', 'DEAD_LETTERED')`),
  foreignKey({
    columns: [table.executionID, table.tenantID],
    foreignColumns: [executions.id, executions.tenantID],
    name: "dispatch_execution_transitions_execution_tenant_fk",
  }),
  foreignKey({
    columns: [table.executionID, table.attempt],
    foreignColumns: [executionAttempts.executionID, executionAttempts.attempt],
    name: "dispatch_execution_transitions_attempt_fk",
  }),
  uniqueIndex("dispatch_execution_transitions_sequence_unique").on(table.tenantID, table.executionID, table.sequence),
  index("dispatch_execution_transitions_execution_created_idx").on(table.executionID, table.createdAt),
]);

export const outboxEntries = pgTable("dispatch_outbox", {
  aggregateID: text("aggregate_id").notNull(),
  aggregateType: text("aggregate_type").notNull(),
  availableAt: timestamp("available_at", { withTimezone: true }).notNull().defaultNow(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  headers: jsonb("headers").$type<Record<string, string>>().notNull().default({}),
  id: text("id").primaryKey(),
  lastError: text("last_error"),
  leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true }),
  leaseToken: text("lease_token"),
  payload: jsonb("payload").$type<unknown>().notNull(),
  publishAttempts: integer("publish_attempts").notNull().default(0),
  publishedAt: timestamp("published_at", { withTimezone: true }),
  tenantID: text("tenant_id").notNull(),
  topic: text("topic").notNull(),
}, (table) => [
  check("dispatch_outbox_lease_complete", sql`(${table.leaseToken} IS NULL) = (${table.leaseExpiresAt} IS NULL)`),
  check("dispatch_outbox_published_unleased", sql`${table.publishedAt} IS NULL OR ${table.leaseToken} IS NULL`),
  check("dispatch_outbox_publish_attempts_nonnegative", sql`${table.publishAttempts} >= 0`),
  uniqueIndex("dispatch_outbox_topic_aggregate_unique").on(table.topic, table.aggregateType, table.aggregateID),
  index("dispatch_outbox_claim_idx")
    .on(table.availableAt, table.leaseExpiresAt)
    .where(sql`${table.publishedAt} IS NULL`),
  index("dispatch_outbox_tenant_aggregate_idx").on(table.tenantID, table.aggregateType, table.aggregateID),
]);
