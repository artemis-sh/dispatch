import { z } from "zod";
import { canonicalJson, jsonValueSchema, type JsonValue } from "../json.js";

export type CloudEventAttributeValue = boolean | number | string;
export const TENANT_ID_EXTENSION = "tenantid";
export const DISPATCH_EXTENSION = "dispatch";

const MAX_DATA_BYTES = 128 * 1024;
const MAX_CANONICAL_EVENT_BYTES = 40 * 1024;
const MAX_EXTENSIONS = 32;
const byteLengthAtMost = (limit: number) => (value: string) => Buffer.byteLength(value, "utf8") <= limit;
const boundedString = (limit: number) => z.string().min(1).refine(byteLengthAtMost(limit), `must be at most ${limit} bytes`);
const uriReferenceSchema = boundedString(2_048).refine((value) => {
  if (/\s|[\u0000-\u001f\u007f]/.test(value)) return false;
  try {
    new URL(value, "https://dispatch.invalid");
    return true;
  } catch {
    return false;
  }
}, "must be a URI-reference");
const absoluteUriSchema = boundedString(2_048).url();
const extensionValueSchema = z.union([boundedString(1_024), z.boolean(), z.number().int().safe()]);
const coreAttributes = new Set(["specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"]);
const reservedExtensions = new Set([TENANT_ID_EXTENSION, DISPATCH_EXTENSION]);
const extensionNamePattern = /^[a-z0-9]{1,20}$/;

const cloudEventSchema = z
  .object({
    specversion: z.literal("1.0"),
    id: boundedString(1_024),
    source: uriReferenceSchema,
    type: boundedString(255),
    subject: z.string().refine(byteLengthAtMost(1_024), "must be at most 1024 bytes").optional(),
    time: z.iso.datetime({ offset: true }).overwrite((value) => new Date(value).toISOString()).optional(),
    datacontenttype: z.literal("application/json").default("application/json"),
    dataschema: absoluteUriSchema.optional(),
    data: jsonValueSchema.refine(
      (value) => Buffer.byteLength(JSON.stringify(value), "utf8") <= MAX_DATA_BYTES,
      `must be at most ${MAX_DATA_BYTES} bytes`,
    ),
  })
  .catchall(z.unknown());

function refineCloudEvent(event: z.output<typeof cloudEventSchema>, context: z.RefinementCtx, enforceCanonicalSize: boolean): void {
    if (Object.hasOwn(event, "data_base64")) {
      context.addIssue({ code: "custom", path: ["data_base64"], message: "data_base64 is not supported" });
    }
    const extensions = Object.entries(event).filter(([name]) => !coreAttributes.has(name));
    if (extensions.length > MAX_EXTENSIONS) {
      context.addIssue({ code: "custom", message: `must have at most ${MAX_EXTENSIONS} extensions` });
    }
    for (const [name, value] of extensions) {
      if (reservedExtensions.has(name)) {
        context.addIssue({ code: "custom", path: [name], message: `${name} is a reserved extension` });
      } else if (!extensionNamePattern.test(name)) {
        context.addIssue({ code: "custom", path: [name], message: "extension names must be 1-20 lowercase alphanumeric characters" });
      } else if (!extensionValueSchema.safeParse(value).success) {
        context.addIssue({ code: "custom", path: [name], message: "extension values must be bounded strings, booleans, or safe integers" });
      }
    }
    if (enforceCanonicalSize && Buffer.byteLength(canonicalJson(event as JsonValue), "utf8") > MAX_CANONICAL_EVENT_BYTES) {
      context.addIssue({ code: "custom", message: `canonical event must be at most ${MAX_CANONICAL_EVENT_BYTES} bytes` });
    }
}

/** Validates an externally supplied event, including the inbound size limit. */
export const normalizedCloudEventSchema = cloudEventSchema
  .superRefine((event, context) => refineCloudEvent(event, context, true)) as z.ZodType<NormalizedCloudEvent>;

/**
 * Validates an event derived from an accepted event. Control-plane metadata added
 * after admission is not subject to the inbound canonical-event size limit.
 */
export const derivedCloudEventSchema = cloudEventSchema
  .superRefine((event, context) => refineCloudEvent(event, context, false)) as z.ZodType<NormalizedCloudEvent>;

export type NormalizedCloudEvent = {
  specversion: "1.0";
  id: string;
  source: string;
  type: string;
  subject?: string;
  time?: string;
  datacontenttype: string;
  dataschema?: string;
  data: JsonValue;
  [attribute: string]: JsonValue | undefined;
};

export type CloudEvent<TData extends JsonValue = JsonValue> = Omit<NormalizedCloudEvent, "data"> & { data: TData };

export type CloudEventValidationIssue = {
  attribute: string;
  message: string;
};

export type CloudEventValidationResult<TData extends JsonValue = JsonValue> =
  | { valid: true; event: CloudEvent<TData> }
  | { valid: false; issues: CloudEventValidationIssue[] };
export function validateCloudEvent<TData extends JsonValue = JsonValue>(value: unknown): CloudEventValidationResult<TData> {
  const result = normalizedCloudEventSchema.safeParse(value);
  if (result.success) return { valid: true, event: result.data as unknown as CloudEvent<TData> };
  return {
    valid: false,
    issues: result.error.issues.map((issue) => ({ attribute: issue.path.join(".") || "$", message: issue.message })),
  };
}

export function normalizeCloudEvent<TData extends JsonValue = JsonValue>(value: unknown): CloudEvent<TData> {
  return normalizedCloudEventSchema.parse(value) as unknown as CloudEvent<TData>;
}

export function isCloudEvent(value: unknown): value is NormalizedCloudEvent {
  return validateCloudEvent(value).valid;
}
