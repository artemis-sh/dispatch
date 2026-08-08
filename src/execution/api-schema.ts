import { createRoute, z } from "@hono/zod-openapi";
import { EXECUTION_STATES } from "./states.js";
import { ATTEMPT_STATES } from "../dispatch/states.js";
import type { AgentProfileDefinition, ExecutionInput, JsonObject } from "./types.js";
import { resolvedWorkspaceSchema } from "../workspace/schema.js";

export const simpleIdSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$/, "must contain only letters, numbers, '.', '_', or '-' and start and end with a letter or number");
export const versionSchema = z.coerce.number().int().positive();
const MAX_JSON_BYTES = 128 * 1024;
const boundedJsonObjectSchema = z
  .record(z.string(), z.unknown())
  .refine((value) => Buffer.byteLength(JSON.stringify(value), "utf8") <= MAX_JSON_BYTES, `must be at most ${MAX_JSON_BYTES} bytes`) as z.ZodType<JsonObject>;
const dnsLabelSchema = z
  .string()
  .min(1)
  .max(63)
  .regex(/^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/, "must be a valid DNS label");
const reservedConnectionSidecars = new Set(["opencode", "workspace-materializer", "dispatch-gateway-proxy"]);
const connectionIdSchema = z.string().min(1).max(128).regex(/^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/);
const githubTools = new Set([
  "actions_get", "actions_list", "add_comment_to_pending_review", "add_issue_comment",
  "add_reply_to_pull_request_comment", "create_branch", "create_pull_request", "get_commit",
  "get_file_contents", "get_job_logs", "get_label", "issue_read", "issue_write", "list_commits",
  "list_label", "merge_pull_request", "pull_request_read", "pull_request_review_write", "push_files", "search_code", "search_issues",
]);
const profileConnectionSchema = z
  .object({
    id: connectionIdSchema,
    sidecar: dnsLabelSchema.refine((value) => !reservedConnectionSidecars.has(value), "must not use a reserved sidecar name"),
  })
  .strict();

export const agentProfileDefinitionSchema: z.ZodType<AgentProfileDefinition> = z
  .object({
    schemaVersion: z.literal(1),
    runtime: z
      .object({
        type: z.literal("opencode"),
        agent: z.string().min(1).max(128),
        opencodeConfig: boundedJsonObjectSchema,
      })
      .strict(),
    sandbox: z
      .object({
        templateName: dnsLabelSchema,
        warmPool: dnsLabelSchema,
      })
      .strict(),
    connections: z.array(profileConnectionSchema).max(32).default([]),
    permissions: z.object({ onRequest: z.literal("fail") }).strict(),
    timeoutSeconds: z.number().int().min(1).max(86_400),
    retention: z
      .object({ sandboxSecondsAfterFinished: z.number().int().min(0).max(86_400).default(0) })
      .strict()
      .optional(),
  })
  .strict()
  .superRefine((definition, context) => {
    const agents = definition.runtime.opencodeConfig.agent;
    if (!agents || typeof agents !== "object" || Array.isArray(agents) || !(definition.runtime.agent in agents)) {
      context.addIssue({
        code: "custom",
        message: `opencodeConfig.agent must define selected agent ${definition.runtime.agent}`,
        path: ["runtime", "opencodeConfig", "agent"],
      });
    }
    const connectionIds = new Set<string>();
    definition.connections.forEach((connection, index) => {
      if (connectionIds.has(connection.id)) {
        context.addIssue({ code: "custom", message: "connection IDs must be unique", path: ["connections", index, "id"] });
      }
      connectionIds.add(connection.id);
    });
    if (definition.connections.some(({ sidecar }) => sidecar === "github-token-broker")) {
      const config = definition.runtime.opencodeConfig;
      const mcp = config.mcp;
      const github = isObject(mcp) ? mcp.github : undefined;
      const permission = config.permission;
      const globalGitHubEntries = isObject(permission)
        ? Object.entries(permission).filter(([name]) => name.startsWith("github_") && name !== "github_*")
        : [];
      const agents = config.agent;
      const selected = isObject(agents) ? agents[definition.runtime.agent] : undefined;
      const selectedPermission = isObject(selected) ? selected.permission : undefined;
      const selectedEntries = isObject(selectedPermission) ? Object.entries(selectedPermission) : [];
      const githubEntries = selectedEntries.filter(([name]) => name.startsWith("github_"));
      const hasAllowedTool = githubEntries.some(([name, action]) => githubTools.has(name.slice("github_".length)) && action === "allow");
      const hasInvalidGitHubRule = githubEntries.some(([name, action]) => !githubTools.has(name.slice("github_".length)) || action !== "allow");
      if (
        !isObject(github)
        || github.type !== "remote"
        || github.url !== "http://127.0.0.1:8083/"
        || github.oauth !== false
        || github.enabled !== true
        || !isObject(permission)
        || permission["github_*"] !== "deny"
        || globalGitHubEntries.length > 0
        || !hasAllowedTool
        || hasInvalidGitHubRule
      ) {
        context.addIssue({
          code: "custom",
          message: "github-token-broker requires the fixed localhost GitHub MCP endpoint and deny-by-default per-agent tool permissions",
          path: ["runtime", "opencodeConfig"],
        });
      }
    }
  });

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
export const definitionSchema = agentProfileDefinitionSchema;
export const profileRefSchema = z.object({ id: simpleIdSchema, version: z.number().int().positive() }).strict();
export const executionInputSchema = z
  .object({
    text: z.string().min(1).max(65_536),
    context: boundedJsonObjectSchema.optional(),
  })
  .strict() as z.ZodType<ExecutionInput>;
export const workspaceSchema = resolvedWorkspaceSchema;

export const publishProfileVersionBodySchema = z
  .object({
    version: z.number().int().positive(),
    definition: definitionSchema,
  })
  .strict();

export const idempotencyKeySchema = z
  .string()
  .min(1)
  .max(255)
  .refine((value) => !/[\u0000-\u001f\u007f]/.test(value), "must not contain control characters");

export const profileVersionSchema = z
  .object({
    id: z.string(),
    tenantId: z.string(),
    profile: profileRefSchema,
    definition: definitionSchema,
    createdAt: z.string().datetime(),
  })
  .strict()
  .openapi("AgentProfileVersion");

export const executionSchema = z
  .object({
    id: z.string(),
    binding: profileRefSchema,
    tenantId: z.string(),
    state: z.enum(EXECUTION_STATES),
    profile: profileRefSchema,
    input: executionInputSchema,
    workspace: workspaceSchema,
    eventId: z.string(),
    createdAt: z.string().datetime(),
    updatedAt: z.string().datetime(),
    result: z.unknown().nullable(),
  })
  .strict()
  .openapi("Execution");

export const executionAttemptSchema = z
  .object({
    attempt: z.number().int().positive(),
    state: z.enum(ATTEMPT_STATES),
    startedAt: z.string().datetime().nullable(),
    finishedAt: z.string().datetime().nullable(),
    leaseExpiresAt: z.string().datetime().nullable(),
    opencodeSessionId: z.string().nullable(),
    workloadName: z.string().nullable(),
    diagnostic: z.object({
      schemaVersion: z.literal(1),
      phase: z.enum(["session_created", "subscribed", "prompt_submitted", "idle", "permission_requested", "session_error", "aborted"]),
      updatedAt: z.string().datetime(),
      lastEventAt: z.string().datetime().optional(),
      lastEventType: z.string().max(128).optional(),
      toolCallCount: z.number().int().nonnegative().optional(),
      lastToolName: z.string().max(128).optional(),
      eventCount: z.number().int().nonnegative(),
      termination: z.enum(["deadline", "cancellation", "lease_lost", "error"]).optional(),
    }).strict().nullable(),
  })
  .strict()
  .openapi("ExecutionAttempt");

export const executionTransitionSchema = z
  .object({
    id: z.string(),
    attempt: z.number().int().positive().nullable(),
    sequence: z.number().int().positive(),
    fromState: z.enum(EXECUTION_STATES).nullable(),
    toState: z.enum(EXECUTION_STATES),
    actor: z.string(),
    reason: z.string().nullable(),
    createdAt: z.string().datetime(),
    traceContext: z.record(z.string(), z.string()),
  })
  .strict()
  .openapi("ExecutionTransition");

export const eventWaitSchema = z.object({
  id: z.string(),
  attempt: z.number().int().positive(),
  name: z.string(),
  state: z.enum(["PENDING_CONTEXT", "ACTIVE", "CANCELLED", "EXPIRED", "CONSUMED"]),
  correlation: z.record(z.string(), z.union([z.null(), z.boolean(), z.number(), z.string()])),
  deadlineAt: z.string().datetime(),
  activatedAt: z.string().datetime(),
  endedAt: z.string().datetime().nullable(),
}).strict().openapi("EventWait");

export const executionDetailSchema = executionSchema
  .extend({
    attempts: z.array(executionAttemptSchema),
    transitions: z.array(executionTransitionSchema),
    waits: z.array(eventWaitSchema),
  })
  .strict()
  .openapi("ExecutionDetail");

export const cancelExecutionBodySchema = z
  .object({ reason: z.string().trim().min(1).max(1024).optional() })
  .strict();

export const cancellationResponseSchema = z
  .object({ id: z.string(), state: z.enum(["CANCEL_REQUESTED", "CANCELLED"]) })
  .strict()
  .openapi("ExecutionCancellation");

export const executionErrorSchema = z.object({ error: z.string().min(1) }).strict().openapi("ExecutionError");

const profileParams = z.object({ profileID: simpleIdSchema, version: versionSchema });
const executionParams = z.object({ id: z.string().min(1).max(255).regex(/^[^/]+$/) });
const securedErrors = {
  400: jsonResponse("Invalid request.", executionErrorSchema),
  401: jsonResponse("Missing or invalid bearer token.", executionErrorSchema),
  413: jsonResponse("Request body too large.", executionErrorSchema),
  500: jsonResponse("Internal server error.", executionErrorSchema),
};

export const publishProfileVersionRoute = createRoute({
  method: "post",
  path: "/agent-profiles/{profileID}/versions",
  tags: ["executions"],
  summary: "Publish an immutable agent profile version",
  security: [{ bearerAuth: [] }],
  request: {
    params: z.object({ profileID: simpleIdSchema }),
    body: { required: true, content: { "application/json": { schema: publishProfileVersionBodySchema } } },
  },
  responses: {
    201: jsonResponse("Profile version published.", profileVersionSchema),
    ...securedErrors,
    404: jsonResponse("A referenced connection was not found.", executionErrorSchema),
    409: jsonResponse("Profile version already exists.", executionErrorSchema),
  },
});

export const getProfileVersionRoute = createRoute({
  method: "get",
  path: "/agent-profiles/{profileID}/versions/{version}",
  tags: ["executions"],
  summary: "Read an exact agent profile version",
  security: [{ bearerAuth: [] }],
  request: { params: profileParams },
  responses: {
    200: jsonResponse("Profile version found.", profileVersionSchema),
    ...securedErrors,
    404: jsonResponse("Profile version not found.", executionErrorSchema),
  },
});

export const getExecutionRoute = createRoute({
  method: "get",
  path: "/executions/{id}",
  tags: ["executions"],
  summary: "Read an execution",
  security: [{ bearerAuth: [] }],
  request: { params: executionParams },
  responses: {
    200: jsonResponse("Execution found.", executionDetailSchema),
    ...securedErrors,
    404: jsonResponse("Execution not found.", executionErrorSchema),
  },
});

export const cancelExecutionRoute = createRoute({
  method: "post",
  path: "/executions/{id}/cancel",
  tags: ["executions"],
  summary: "Request execution cancellation",
  security: [{ bearerAuth: [] }],
  request: {
    params: executionParams,
    body: { required: true, content: { "application/json": { schema: cancelExecutionBodySchema } } },
  },
  responses: {
    200: jsonResponse("Execution cancelled.", cancellationResponseSchema),
    202: jsonResponse("Execution cancellation requested.", cancellationResponseSchema),
    ...securedErrors,
    404: jsonResponse("Execution not found.", executionErrorSchema),
    409: jsonResponse("Execution cannot be cancelled in its current state.", executionErrorSchema),
  },
});

function jsonResponse(description: string, schema: z.ZodType) {
  return { description, content: { "application/json": { schema } } };
}
