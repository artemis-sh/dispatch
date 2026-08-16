import { z } from "zod";
import { jsonPointerSchema, type JsonPrimitive } from "../json.js";
import { bindingWorkspaceSchema } from "../workspace/schema.js";

const MAX_PROMPT_BYTES = 16 * 1024;
const simpleIdSchema = z.string().min(1).max(128).regex(/^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$/);
const cancellationReasonSchema = z.string().min(1).max(128).regex(/^[A-Z][A-Z0-9_]*$/);
export const versionedRefSchema = z.object({ id: simpleIdSchema, version: z.number().int().positive() }).strict();
const primitiveSchema: z.ZodType<JsonPrimitive> = z.union([z.null(), z.boolean(), z.number().finite(), z.string()]);

export const filterClauseSchema = z.discriminatedUnion("op", [
  z.object({ path: jsonPointerSchema, op: z.literal("eq"), value: primitiveSchema }).strict(),
  z.object({ path: jsonPointerSchema, op: z.literal("in"), values: z.array(primitiveSchema).min(1).max(32) }).strict(),
  z.object({ path: jsonPointerSchema, op: z.literal("contains"), value: primitiveSchema }).strict(),
  z.object({ path: jsonPointerSchema, op: z.literal("containsAny"), values: z.array(primitiveSchema).min(1).max(32) }).strict(),
  z.object({ path: jsonPointerSchema, op: z.literal("notStartsWith"), value: z.string().min(1).max(255) }).strict(),
  z.object({ path: jsonPointerSchema, op: z.literal("exists"), value: z.boolean() }).strict(),
]);

const waitCorrelationSchema = z.union([
  z.object({ name: simpleIdSchema, source: z.literal("event").optional(), path: jsonPointerSchema }).strict(),
  z.object({ name: simpleIdSchema, source: z.literal("supplied"), slot: simpleIdSchema }).strict(),
]);

export const afterTurnSchema = z.object({
  disposition: z.literal("wait"),
  wait: z.object({
    name: simpleIdSchema,
    correlation: z.array(waitCorrelationSchema).min(1).max(16)
      .refine((items) => new Set(items.map((item) => item.name)).size === items.length, "correlation names must be unique")
      .refine((items) => new Set(items.filter((item) => item.source === "supplied").map((item) => item.slot)).size === items.filter((item) => item.source === "supplied").length, "supplied slots must be unique"),
    deadlineSeconds: z.number().int().min(1).max(30 * 24 * 60 * 60),
    admitWhileBusy: z.boolean().optional(),
  }).strict(),
}).strict().superRefine((value, context) => {
  const supplied = value.wait.correlation.filter((item) => "slot" in item);
  if (supplied.length > 1) context.addIssue({ code: "custom", message: "at most one supplied correlation slot is supported", path: ["wait", "correlation"] });
  if (supplied.length > 0 && value.wait.admitWhileBusy !== true) {
    context.addIssue({ code: "custom", message: "supplied correlation requires admitWhileBusy", path: ["wait", "admitWhileBusy"] });
  }
});

const eventMatchSchema = {
  schemaVersion: z.literal(1),
  eventTypes: z.array(z.string().min(1).max(255)).min(1).max(32),
  filter: z.object({ all: z.array(filterClauseSchema).max(16) }).strict(),
};

const activeSingletonSchema = z.object({
  name: simpleIdSchema,
  key: z.array(jsonPointerSchema).min(1).max(16),
}).strict();

const checkpointSchema = z.object({
  name: simpleIdSchema,
  key: z.array(jsonPointerSchema).min(1).max(16),
  value: z.object({ path: jsonPointerSchema }).strict(),
  advanceOn: z.literal("succeeded"),
  unchanged: z.literal("skip"),
}).strict();

export const promptSchema = z
  .object({
    literal: z.string().refine((value) => Buffer.byteLength(value, "utf8") <= MAX_PROMPT_BYTES, `must be at most ${MAX_PROMPT_BYTES} bytes`),
    includeEvent: z.enum(["none", "data", "envelope"]),
  })
  .strict();

export const createBindingDefinitionSchema = z
  .object({
    ...eventMatchSchema,
    prompt: promptSchema,
    workspace: bindingWorkspaceSchema,
    afterTurn: afterTurnSchema.optional(),
    requireGitHubPullRequestEffect: z.boolean().optional(),
    activeSingleton: activeSingletonSchema.optional(),
    checkpoint: checkpointSchema.optional(),
  })
  .strict()
  .superRefine((value, context) => {
    if (value.checkpoint && value.afterTurn) {
      context.addIssue({ code: "custom", message: "checkpoint bindings must be one-shot", path: ["checkpoint"] });
    }
    if (value.requireGitHubPullRequestEffect) {
      const wait = value.afterTurn?.wait;
      if (!wait) {
        context.addIssue({ code: "custom", message: "GitHub pull request effect requirement needs an afterTurn wait", path: ["requireGitHubPullRequestEffect"] });
      } else {
        if (wait.admitWhileBusy !== true) {
          context.addIssue({ code: "custom", message: "GitHub pull request effect requirement needs admitWhileBusy", path: ["afterTurn", "wait", "admitWhileBusy"] });
        }
        if (!wait.correlation.some((item) => item.name === "pullRequestNumber" && item.source === "supplied" && item.slot === "primaryPullRequestNumber")) {
          context.addIssue({ code: "custom", message: "GitHub pull request effect requirement needs the primaryPullRequestNumber supplied correlation", path: ["afterTurn", "wait", "correlation"] });
        }
      }
    }
  });

export const wakeBindingDefinitionSchema = z
  .object({
    ...eventMatchSchema,
    disposition: z.literal("wake"),
    wake: z.object({
      waitName: simpleIdSchema,
      delivery: z.enum(["active-only", "active-or-coalesced"]).optional(),
      correlation: z.array(z.object({ name: simpleIdSchema, path: jsonPointerSchema }).strict()).min(1).max(16)
        .refine((items) => new Set(items.map((item) => item.name)).size === items.length, "correlation names must be unique"),
      action: z.discriminatedUnion("type", [
        z.object({ type: z.literal("continue"), prompt: promptSchema, workspace: bindingWorkspaceSchema.optional() }).strict(),
        z.object({ type: z.literal("complete") }).strict(),
        z.object({ type: z.literal("cancel"), reason: cancellationReasonSchema }).strict(),
      ]),
    }).strict(),
  })
  .strict()
  .superRefine((value, context) => {
    if (value.wake.action.type === "cancel" && value.wake.delivery === "active-or-coalesced") {
      context.addIssue({ code: "custom", message: "cancel wake actions support active-only delivery", path: ["wake", "delivery"] });
    }
  });

export const bindingDefinitionSchema = z.union([
  createBindingDefinitionSchema,
  wakeBindingDefinitionSchema,
]);

export const publishedBindingVersionSchema = z
  .object({
    id: simpleIdSchema,
    bindingId: simpleIdSchema,
    version: z.number().int().positive(),
    tenantId: simpleIdSchema,
    triggerId: simpleIdSchema,
    profile: versionedRefSchema,
    definition: bindingDefinitionSchema,
    enabled: z.boolean(),
    createdAt: z.iso.datetime({ offset: true }),
    disabledAt: z.iso.datetime({ offset: true }).nullable(),
  })
  .strict();

export type VersionedRef = z.infer<typeof versionedRefSchema>;
export type FilterClause = z.infer<typeof filterClauseSchema>;
export type AfterTurnPolicy = z.infer<typeof afterTurnSchema>;
export type BindingDefinition = z.infer<typeof bindingDefinitionSchema>;
export type CreateBindingDefinition = z.infer<typeof createBindingDefinitionSchema>;
export type WakeBindingDefinition = z.infer<typeof wakeBindingDefinitionSchema>;
export type PublishedBindingVersion = z.infer<typeof publishedBindingVersionSchema>;

export interface BindingStore {
  publishBindingVersion(binding: PublishedBindingVersion): Promise<PublishedBindingVersion>;
  getBindingVersion(tenantId: string, bindingId: string, version: number): Promise<PublishedBindingVersion | undefined>;
  disableBindingVersion(tenantId: string, bindingId: string, version: number, disabledAt: string): Promise<PublishedBindingVersion | undefined>;
  listBindingCandidates(tenantId: string, triggerId: string, eventType: string): Promise<readonly PublishedBindingVersion[]>;
}

export function isWakeBinding(binding: PublishedBindingVersion): binding is PublishedBindingVersion & { definition: WakeBindingDefinition } {
  return "disposition" in binding.definition && binding.definition.disposition === "wake";
}

export class BindingVersionAlreadyExistsError extends Error {
  readonly code = "BINDING_VERSION_ALREADY_EXISTS";

  constructor(bindingId: string, version: number) {
    super(`Binding ${bindingId} version ${version} already exists`);
    this.name = "BindingVersionAlreadyExistsError";
  }
}

export class BindingVersionNotFoundError extends Error {
  readonly code = "BINDING_VERSION_NOT_FOUND";

  constructor(bindingId: string, version: number) {
    super(`Binding ${bindingId} version ${version} was not found`);
    this.name = "BindingVersionNotFoundError";
  }
}
