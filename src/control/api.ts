import { randomUUID, timingSafeEqual } from "node:crypto";
import { OpenAPIHono, type Hook } from "@hono/zod-openapi";
import type { Context } from "hono";
import { bodyLimit } from "hono/body-limit";
import type { Config } from "../config.js";
import { ConnectionAlreadyExistsError, ConnectionNotFoundError, type Connection, type ConnectionStore } from "../connection/index.js";
import { registerExecutionApi } from "../execution/api.js";
import { normalizedCloudEventSchema } from "../execution/events.js";
import { sourceDeliveryIdempotencyKey } from "../execution/idempotency.js";
import type { EventAdmissionStore, ExecutionStore } from "../execution/store.js";
import { IdempotencyConflictError, ProfileVersionNotFoundError } from "../execution/types.js";
import { hashCanonicalJson, type JsonValue } from "../json.js";
import { WorkspaceResolutionError } from "../workspace/resolver.js";
import { BindingVersionAlreadyExistsError, BindingVersionNotFoundError, type BindingStore } from "./binding.js";
import { TriggerAlreadyExistsError, TriggerNotFoundError, type TriggerStore } from "./trigger.js";
import {
  admitEventRoute,
  createConnectionRoute,
  createTriggerRoute,
  disableBindingVersionRoute,
  disableTriggerRoute,
  getBindingVersionRoute,
  getConnectionRoute,
  getTriggerRoute,
  publishBindingVersionRoute,
} from "./api-schema.js";

const TENANT_ID = "default";
const MAX_BODY_BYTES = 128 * 1024;
const WORKSPACE_RESOLUTION_MESSAGE = "Workspace could not be resolved from event data";
const GITHUB_WEBHOOK_SECRET_UNAVAILABLE_MESSAGE = "GitHub webhook secret unavailable";
const SCHEDULE_WORKER_UNAVAILABLE_MESSAGE = "Schedule triggers require the schedule worker to be enabled";
export type ControlApiStore = ExecutionStore & TriggerStore & BindingStore & EventAdmissionStore & ConnectionStore;

export function mountControlApi(
  app: OpenAPIHono<any>,
  config: Config,
  store: ControlApiStore,
  readEnvironmentVariable: (name: string) => string | undefined = (name) => process.env[name],
): void {
  if (!config.adminToken) return;
  const token = config.adminToken;
  const api = new OpenAPIHono({ defaultHook: validationHook });

  api.use("*", bodyLimit({ maxSize: MAX_BODY_BYTES, onError: (context) => context.json({ error: "Request body too large" }, 413) }));
  api.use("*", async (context, next) => {
    if (!isAuthorized(context, token)) return context.json({ error: "Unauthorized" }, 401);
    await next();
  });

  registerExecutionApi(api, store);

  api.openapi(createConnectionRoute, async (context) => handle(context, async () => {
    const body = context.req.valid("json");
    const connection = await store.createConnection({
      id: randomUUID(),
      tenantId: TENANT_ID,
      connection: body,
      createdAt: new Date().toISOString(),
    });
    return context.json(publicConnection(connection), 201);
  }) as never);

  api.openapi(getConnectionRoute, async (context) => handle(context, async () => {
    const { connectionID } = context.req.valid("param");
    const connection = await store.getConnection(TENANT_ID, connectionID);
    if (!connection) throw new ConnectionNotFoundError(connectionID);
    return context.json(publicConnection(connection), 200);
  }) as never);

  api.openapi(createTriggerRoute, async (context) => handle(context, async () => {
    const body = context.req.valid("json");
    if (body.type === "schedule.cron" && !config.scheduleWorkerEnabled) {
      return context.json({ error: SCHEDULE_WORKER_UNAVAILABLE_MESSAGE }, 422);
    }
    if (body.type === "github.app.webhook") {
      const secret = readEnvironmentVariable(body.config.webhookSecretEnv);
      const secretBytes = secret === undefined ? 0 : Buffer.byteLength(secret, "utf8");
      if (secret === undefined || secret.includes("\0") || secretBytes < 32 || secretBytes > 1024) {
        return context.json({ error: GITHUB_WEBHOOK_SECRET_UNAVAILABLE_MESSAGE }, 422);
      }
    }
    const createdAt = new Date().toISOString();
    return context.json(await store.createTrigger({ ...body, tenantId: TENANT_ID, enabled: true, createdAt, disabledAt: null }), 201);
  }) as never);

  api.openapi(getTriggerRoute, async (context) => handle(context, async () => {
    const { triggerID } = context.req.valid("param");
    const trigger = await store.getTrigger(TENANT_ID, triggerID);
    if (!trigger) throw new TriggerNotFoundError(triggerID);
    return context.json(trigger, 200);
  }) as never);

  api.openapi(disableTriggerRoute, async (context) => handle(context, async () => {
    const { triggerID } = context.req.valid("param");
    const trigger = await store.disableTrigger(TENANT_ID, triggerID, new Date().toISOString());
    if (!trigger) throw new TriggerNotFoundError(triggerID);
    return context.json(trigger, 200);
  }) as never);

  api.openapi(publishBindingVersionRoute, async (context) => handle(context, async () => {
    const { bindingID } = context.req.valid("param");
    const body = context.req.valid("json");
    if (!await store.getTrigger(TENANT_ID, body.triggerId)) throw new TriggerNotFoundError(body.triggerId);
    const createdAt = new Date().toISOString();
    const binding = await store.publishBindingVersion({ id: randomUUID(), bindingId: bindingID, tenantId: TENANT_ID, ...body, enabled: true, createdAt, disabledAt: null });
    return context.json(binding, 201);
  }) as never);

  api.openapi(getBindingVersionRoute, async (context) => handle(context, async () => {
    const { bindingID, version } = context.req.valid("param");
    const binding = await store.getBindingVersion(TENANT_ID, bindingID, version);
    if (!binding) throw new BindingVersionNotFoundError(bindingID, version);
    return context.json(binding, 200);
  }) as never);

  api.openapi(disableBindingVersionRoute, async (context) => handle(context, async () => {
    const { bindingID, version } = context.req.valid("param");
    const binding = await store.disableBindingVersion(TENANT_ID, bindingID, version, new Date().toISOString());
    if (!binding) throw new BindingVersionNotFoundError(bindingID, version);
    return context.json(binding, 200);
  }) as never);

  api.openapi(admitEventRoute, async (context) => handle(context, async () => {
    const { triggerID } = context.req.valid("param");
    const trigger = await store.getTrigger(TENANT_ID, triggerID);
    if (!trigger) throw new TriggerNotFoundError(triggerID);
    if (trigger.type !== "cloudevents.http") throw new TriggerNotFoundError(triggerID);
    const normalized = normalizedCloudEventSchema.safeParse(context.req.valid("json"));
    if (!normalized.success) return context.json({ error: "Invalid request" }, 400);
    const event = normalized.data;
    const result = await store.admitEvent({
      tenantId: TENANT_ID,
      triggerId: triggerID,
      internalEventId: randomUUID(),
      event,
      sourceDeduplicationKey: sourceDeliveryIdempotencyKey(triggerID, context.req.header("idempotency-key")!),
      admissionHash: hashCanonicalJson({ schemaVersion: 1, triggerId: triggerID, event } as JsonValue),
      admittedAt: new Date().toISOString(),
    });
    return context.json(result, 202);
  }) as never);

  app.route("/v1", api);
}

async function handle(context: Context, run: () => Promise<Response>): Promise<Response> {
  try {
    return await run();
  } catch (error) {
    if (error instanceof TriggerNotFoundError || error instanceof BindingVersionNotFoundError || error instanceof ProfileVersionNotFoundError || error instanceof ConnectionNotFoundError) return context.json({ error: error.message }, 404);
    if (error instanceof TriggerAlreadyExistsError || error instanceof BindingVersionAlreadyExistsError || error instanceof IdempotencyConflictError || error instanceof ConnectionAlreadyExistsError) return context.json({ error: error.message }, 409);
    if (error instanceof WorkspaceResolutionError) return context.json({ error: WORKSPACE_RESOLUTION_MESSAGE }, 422);
    return context.json({ error: "Internal server error" }, 500);
  }
}

function publicConnection(value: Connection) {
  return { id: value.connection.id, tenantId: value.tenantId, type: value.connection.type, createdAt: value.createdAt };
}

const validationHook: Hook<unknown, any, any, any> = (result, context) => {
  if (!result.success) return context.json({ error: "Invalid request" }, 400);
};

function isAuthorized(context: Context, token: string): boolean {
  const header = context.req.header("authorization");
  if (!header?.startsWith("Bearer ")) return false;
  const expected = Buffer.from(token);
  const supplied = Buffer.from(header.slice("Bearer ".length));
  return expected.length === supplied.length && timingSafeEqual(expected, supplied);
}
