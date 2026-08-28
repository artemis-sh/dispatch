import { createRoute, z } from "@hono/zod-openapi";
import { bindingDefinitionSchema } from "./binding.js";
import { executionSchema, idempotencyKeySchema, profileRefSchema, simpleIdSchema, versionSchema } from "../execution/api-schema.js";
import { scheduleCronTriggerConfigSchema } from "./trigger.js";

const errorSchema = z.object({ error: z.string().min(1) }).strict();
const workspaceResolutionErrorSchema = z.object({
  error: z.literal("Workspace could not be resolved from event data"),
}).strict().openapi("WorkspaceResolutionError");
const cloudEventsHttpTriggerConfigSchema = z.object({ schemaVersion: z.literal(1) }).strict();
const githubAppWebhookTriggerConfigSchema = z.object({
  schemaVersion: z.literal(1),
  webhookSecretEnv: z.string().regex(/^DISPATCH_GITHUB_WEBHOOK_SECRET_[A-Z0-9_]{1,96}$/),
}).strict();
const triggerFields = {
  id: simpleIdSchema,
  tenantId: simpleIdSchema,
  enabled: z.boolean(),
  createdAt: z.string().datetime(),
  disabledAt: z.string().datetime().nullable(),
};
const triggerSchema = z.discriminatedUnion("type", [
  z.object({ ...triggerFields, type: z.literal("cloudevents.http"), config: cloudEventsHttpTriggerConfigSchema }).strict(),
  z.object({ ...triggerFields, type: z.literal("github.app.webhook"), config: githubAppWebhookTriggerConfigSchema }).strict(),
  z.object({ ...triggerFields, type: z.literal("schedule.cron"), config: scheduleCronTriggerConfigSchema }).strict(),
]).openapi("Trigger");
const createTriggerRequestSchema = z.discriminatedUnion("type", [
  z.object({ id: simpleIdSchema, type: z.literal("cloudevents.http"), config: cloudEventsHttpTriggerConfigSchema }).strict(),
  z.object({ id: simpleIdSchema, type: z.literal("github.app.webhook"), config: githubAppWebhookTriggerConfigSchema }).strict(),
  z.object({ id: simpleIdSchema, type: z.literal("schedule.cron"), config: scheduleCronTriggerConfigSchema }).strict(),
]);
const githubWebhookSecretUnavailableSchema = z.object({
  error: z.literal("GitHub webhook secret unavailable"),
}).strict().openapi("GitHubWebhookSecretUnavailableError");
const scheduleWorkerUnavailableSchema = z.object({
  error: z.literal("Schedule triggers require the schedule worker to be enabled"),
}).strict().openapi("ScheduleWorkerUnavailableError");
const bindingVersionSchema = z.object({
  id: simpleIdSchema,
  bindingId: simpleIdSchema,
  version: z.number().int().positive(),
  tenantId: simpleIdSchema,
  triggerId: simpleIdSchema,
  profile: profileRefSchema,
  definition: bindingDefinitionSchema,
  enabled: z.boolean(),
  createdAt: z.string().datetime(),
  disabledAt: z.string().datetime().nullable(),
}).strict().openapi("BindingVersion");
const admittedEventSchema = z.object({
  id: z.string(),
  tenantId: z.string(),
  triggerId: z.string(),
  source: z.string(),
  eventId: z.string(),
  type: z.string(),
  sourceDeduplicationKey: z.string(),
  admissionHash: z.string(),
  admittedAt: z.string().datetime(),
}).strict();
// Runtime normalization applies the byte, JSON data, URI-reference, and extension bounds.
// Keep this boundary schema non-transforming and permissive enough not to reject valid extensions first.
const cloudEventRequestSchema = z.object({
  specversion: z.literal("1.0"),
  id: z.string().min(1),
  source: z.string().min(1),
  type: z.string().min(1),
  subject: z.string().optional(),
  time: z.string().optional(),
  datacontenttype: z.literal("application/json").optional(),
  dataschema: z.string().optional(),
  data: z.unknown(),
}).catchall(z.union([z.string(), z.boolean(), z.number()]));
const admissionWakeResultSchema = z.object({
  id: z.string(), executionId: z.string(), eventWaitId: z.string(), binding: profileRefSchema,
  action: z.enum(["CONTINUED", "COMPLETED", "CANCELLED"]), inputSequence: z.number().int().positive().nullable(),
  state: z.enum(["QUEUED", "COMPLETED", "CANCELLED"]), consumedAt: z.string().datetime(),
}).strict().openapi("AdmissionWakeResult");
const admissionResultSchema = z.object({
  event: admittedEventSchema,
  executions: z.array(executionSchema),
  wakes: z.array(admissionWakeResultSchema),
  pendingWakes: z.array(z.object({
    id: z.string(), executionId: z.string(), binding: profileRefSchema,
    action: z.enum(["CONTINUED", "COMPLETED"]), disposition: z.enum(["PENDING", "DOMINATED"]),
    admittedAt: z.string().datetime(),
  }).strict()),
  replayed: z.boolean(),
}).strict().openapi("AdmissionResult");

const securedErrors = {
  400: jsonResponse("Invalid request.", errorSchema),
  401: jsonResponse("Missing or invalid bearer token.", errorSchema),
  413: jsonResponse("Request body too large.", errorSchema),
  500: jsonResponse("Internal server error.", errorSchema),
};
const triggerParams = z.object({ triggerID: simpleIdSchema });
const bindingParams = z.object({ bindingID: simpleIdSchema, version: versionSchema });
const connectionTypeSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/, "must contain only lowercase letters, numbers, '.', or '-' and start and end with a letter or number");
const connectionSchema = z.object({
  id: simpleIdSchema,
  tenantId: simpleIdSchema,
  type: connectionTypeSchema,
  createdAt: z.string().datetime(),
}).strict().openapi("Connection");
const connectionIdSchema = z.string().min(1).max(128).regex(/^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/);
const createConnectionRequestSchema = z.object({ id: connectionIdSchema, type: connectionTypeSchema }).strict();
const connectionParams = z.object({ connectionID: connectionIdSchema });

export const createConnectionRoute = createRoute({
  method: "post", path: "/connections", tags: ["control"], summary: "Create a connection", security: [{ bearerAuth: [] }],
  request: { body: { required: true, content: { "application/json": { schema: createConnectionRequestSchema } } } },
  responses: { 201: jsonResponse("Connection created.", connectionSchema), ...securedErrors, 409: jsonResponse("Connection already exists.", errorSchema) },
});
export const getConnectionRoute = createRoute({
  method: "get", path: "/connections/{connectionID}", tags: ["control"], summary: "Read a connection", security: [{ bearerAuth: [] }],
  request: { params: connectionParams }, responses: { 200: jsonResponse("Connection found.", connectionSchema), ...securedErrors, 404: jsonResponse("Connection not found.", errorSchema) },
});

export const createTriggerRoute = createRoute({
  method: "post", path: "/triggers", tags: ["control"], summary: "Create a trigger", security: [{ bearerAuth: [] }],
  request: { body: { required: true, content: { "application/json": { schema: createTriggerRequestSchema } } } },
  responses: { 201: jsonResponse("Trigger created.", triggerSchema), ...securedErrors, 409: jsonResponse("Trigger already exists.", errorSchema), 422: jsonResponse("Schedule triggers or GitHub webhook triggers cannot be created in the current configuration.", z.union([githubWebhookSecretUnavailableSchema, scheduleWorkerUnavailableSchema])) },
});
export const getTriggerRoute = createRoute({
  method: "get", path: "/triggers/{triggerID}", tags: ["control"], summary: "Read a trigger", security: [{ bearerAuth: [] }],
  request: { params: triggerParams }, responses: { 200: jsonResponse("Trigger found.", triggerSchema), ...securedErrors, 404: jsonResponse("Trigger not found.", errorSchema) },
});
export const disableTriggerRoute = createRoute({
  method: "post", path: "/triggers/{triggerID}/disable", tags: ["control"], summary: "Disable a trigger", security: [{ bearerAuth: [] }],
  request: { params: triggerParams }, responses: { 200: jsonResponse("Trigger disabled.", triggerSchema), ...securedErrors, 404: jsonResponse("Trigger not found.", errorSchema) },
});
export const publishBindingVersionRoute = createRoute({
  method: "post", path: "/bindings/{bindingID}/versions", tags: ["control"], summary: "Publish an immutable binding version", security: [{ bearerAuth: [] }],
  request: { params: z.object({ bindingID: simpleIdSchema }), body: { required: true, content: { "application/json": { schema: z.object({ version: z.number().int().positive(), triggerId: simpleIdSchema, profile: profileRefSchema, definition: bindingDefinitionSchema }).strict() } } } },
  responses: { 201: jsonResponse("Binding version published.", bindingVersionSchema), ...securedErrors, 404: jsonResponse("Dependency not found.", errorSchema), 409: jsonResponse("Binding version already exists.", errorSchema) },
});
export const getBindingVersionRoute = createRoute({
  method: "get", path: "/bindings/{bindingID}/versions/{version}", tags: ["control"], summary: "Read an exact binding version", security: [{ bearerAuth: [] }],
  request: { params: bindingParams }, responses: { 200: jsonResponse("Binding version found.", bindingVersionSchema), ...securedErrors, 404: jsonResponse("Binding version not found.", errorSchema) },
});
export const disableBindingVersionRoute = createRoute({
  method: "post", path: "/bindings/{bindingID}/versions/{version}/disable", tags: ["control"], summary: "Disable a binding version", security: [{ bearerAuth: [] }],
  request: { params: bindingParams }, responses: { 200: jsonResponse("Binding version disabled.", bindingVersionSchema), ...securedErrors, 404: jsonResponse("Binding version not found.", errorSchema) },
});
export const admitEventRoute = createRoute({
  method: "post", path: "/triggers/{triggerID}/events", tags: ["events"], summary: "Admit a structured CloudEvent", security: [{ bearerAuth: [] }],
  request: { params: triggerParams, headers: z.object({ "Idempotency-Key": idempotencyKeySchema }), body: { required: true, content: { "application/cloudevents+json": { schema: cloudEventRequestSchema }, "application/json": { schema: cloudEventRequestSchema } } } },
  responses: {
    202: jsonResponse("Event admitted.", admissionResultSchema),
    ...securedErrors,
    404: jsonResponse("Enabled trigger not found.", errorSchema),
    409: jsonResponse("Idempotency key conflict.", errorSchema),
    422: jsonResponse("A matching binding's workspace could not be resolved. The event and all of its executions are rejected atomically.", workspaceResolutionErrorSchema),
  },
});

function jsonResponse(description: string, schema: z.ZodType) {
  return { description, content: { "application/json": { schema } } };
}
