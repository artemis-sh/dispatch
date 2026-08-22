import { Buffer } from "node:buffer";
import type { Event, OpencodeClient } from "@opencode-ai/sdk/client";
import { createAgentClient, type OpenCodeEndpoint, waitForOpencodeReady } from "./client.js";
import type { ExecutionAttemptDiagnostic } from "../execution/types.js";
import { logger } from "../logger.js";

export const DEFAULT_MAX_OUTPUT_BYTES = 1_048_576;
const MAX_DIAGNOSTIC_TOOL_NAME_BYTES = 128;

export type ExecutionAttemptInput = {
  abortSessionOnSignal?: (reason: unknown) => boolean;
  agent: string;
  endpoint: OpenCodeEndpoint;
  maxOutputBytes?: number;
  prompt: string;
  signal?: AbortSignal;
  title: string;
  onSession?: (sessionId: string) => Promise<void>;
  onDiagnostic?: (diagnostic: Omit<ExecutionAttemptDiagnostic, "schemaVersion" | "updatedAt" | "termination">) => Promise<void>;
};

export type ExecutionAttemptResult = {
  output: string;
  sessionId: string;
};

export type ObserveExecutionAttemptInput = {
  abortSessionOnSignal?: (reason: unknown) => boolean;
  endpoint: OpenCodeEndpoint;
  sessionId: string;
  maxOutputBytes?: number;
  signal?: AbortSignal;
};

export async function observeExecutionAttempt(
  input: ObserveExecutionAttemptInput,
  client?: OpencodeClient,
): Promise<ExecutionAttemptResult> {
  const maxOutputBytes = input.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES;
  if (!Number.isSafeInteger(maxOutputBytes) || maxOutputBytes < 0) {
    throw new Error("maxOutputBytes must be a nonnegative safe integer");
  }

  input.signal?.throwIfAborted();
  if (!client) {
    await waitForOpencodeReady(input.endpoint, input.endpoint, input.signal);
    client = createAgentClient(input.endpoint, input.endpoint);
  }

  const { sessionId } = input;
  const log = logger.child({ sessionId });
  let abortPromise: Promise<never> | undefined;
  let removeAbortListener: (() => void) | undefined;
  let events: Awaited<ReturnType<OpencodeClient["event"]["subscribe"]>> | undefined;

  if (input.signal) {
    abortPromise = new Promise<never>((_resolve, reject) => {
      const abort = () => {
        if (input.abortSessionOnSignal?.(input.signal?.reason) !== false) {
          void client.session.abort({ path: { id: sessionId } }).catch((error: unknown) => {
            log.warn("failed to abort opencode session", { error: String(error) });
          });
        }
        reject(input.signal?.reason ?? new DOMException("The operation was aborted", "AbortError"));
      };
      removeAbortListener = () => input.signal?.removeEventListener("abort", abort);
      if (input.signal?.aborted) abort();
      else input.signal?.addEventListener("abort", abort, { once: true });
    });
  }

  const raceAbort = <T>(promise: Promise<T>): Promise<T> => abortPromise
    ? Promise.race([promise, abortPromise])
    : promise;

  try {
    await raceAbort(client.session.get({
      path: { id: sessionId },
      signal: input.signal,
      throwOnError: true,
    }));

    events = await raceAbort(client.event.subscribe({ signal: input.signal }));
    const { data: statuses } = await raceAbort(client.session.status({
      signal: input.signal,
      throwOnError: true,
    }));
    let status = parseSessionStatus(statuses, sessionId);

    if (status === "busy" || status === "retry") {
      const iterator = events.stream[Symbol.asyncIterator]();
      // The session may become idle after the initial status read but before
      // consumption begins. Reconcile after installing the iterator so an
      // adopted session does not wait forever for an idle event already past
      // the live subscription.
      const { data: updatedStatuses } = await raceAbort(client.session.status({
        signal: input.signal,
        throwOnError: true,
      }));
      status = parseSessionStatus(updatedStatuses, sessionId);

      while (true) {
        if (status === "idle") break;
        const next = await raceAbort(iterator.next());
        if (next.done) throw new Error(`opencode event stream ended before session ${sessionId} became idle`);

        const event = next.value;
        if (!isSessionEvent(event, sessionId)) continue;

        if (event.type === "permission.updated") {
          await raceAbort(client.postSessionIdPermissionsPermissionId({
            path: { id: sessionId, permissionID: event.properties.id },
            body: { response: "reject" },
            throwOnError: true,
          }));
          throw new Error(`opencode session ${sessionId} requested permission ${event.properties.id}`);
        }
        if (event.type === "session.error") {
          throw new Error(`opencode session ${sessionId} error: ${formatOpencodeError(event.properties.error)}`);
        }
        if (event.type === "session.idle"
          || (event.type === "session.status" && event.properties.status.type === "idle")) break;
      }
    }

    const { data: messages } = await raceAbort(client.session.messages({
      path: { id: sessionId },
      signal: input.signal,
      throwOnError: true,
    }));
    return { output: assistantText(messages, sessionId, maxOutputBytes), sessionId };
  } finally {
    removeAbortListener?.();
    if (events) void events.stream.return(undefined).catch(() => undefined);
  }
}

export async function runExecutionAttempt(
  input: ExecutionAttemptInput,
  client?: OpencodeClient,
): Promise<ExecutionAttemptResult> {
  const maxOutputBytes = input.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES;
  if (!Number.isSafeInteger(maxOutputBytes) || maxOutputBytes < 0) {
    throw new Error("maxOutputBytes must be a nonnegative safe integer");
  }

  input.signal?.throwIfAborted();
  if (!client) {
    await waitForOpencodeReady(input.endpoint, input.endpoint, input.signal);
    client = createAgentClient(input.endpoint, input.endpoint);
  }

  const { data: session } = await client.session.create({
    body: { title: input.title },
    signal: input.signal,
    throwOnError: true,
  });
  const sessionId = session.id;
  const log = logger.child({ sessionId, agentName: input.agent });
  logger.info("opencode session created", { sessionId, title: input.title });
  await input.onSession?.(sessionId);
  let eventCount = 0;
  let toolCallCount = 0;
  let lastDiagnosticEvent: string | undefined;
  let lastToolName: string | undefined;
  const checkpoint = (phase: "session_created" | "subscribed" | "prompt_submitted" | "idle" | "permission_requested" | "session_error", lastEventType?: string) =>
    input.onDiagnostic?.({
      phase,
      eventCount,
      ...(lastEventType ? { lastEventType, lastEventAt: new Date().toISOString() } : {}),
      ...(toolCallCount > 0 ? { toolCallCount } : {}),
      ...(lastToolName ? { lastToolName } : {}),
    });
  await checkpoint("session_created");

  let abortPromise: Promise<never> | undefined;
  let removeAbortListener: (() => void) | undefined;

  if (input.signal) {
    abortPromise = new Promise<never>((_resolve, reject) => {
      const abort = () => {
        if (input.abortSessionOnSignal?.(input.signal?.reason) !== false) {
          void client.session.abort({ path: { id: sessionId } }).catch((error: unknown) => {
            log.warn("failed to abort opencode session", { error: String(error) });
          });
        }
        reject(input.signal?.reason ?? new DOMException("The operation was aborted", "AbortError"));
      };
      removeAbortListener = () => input.signal?.removeEventListener("abort", abort);
      if (input.signal?.aborted) abort();
      else input.signal?.addEventListener("abort", abort, { once: true });
    });
  }

  let output = "";
  let outputBytes = 0;
  let events: Awaited<ReturnType<OpencodeClient["event"]["subscribe"]>> | undefined;

  try {
    const subscription = client.event.subscribe({ signal: input.signal });
    events = abortPromise ? await Promise.race([subscription, abortPromise]) : await subscription;
    await checkpoint("subscribed");
    await client.session.promptAsync({
      path: { id: sessionId },
      body: {
        agent: input.agent,
        parts: [{ type: "text", text: input.prompt }],
      },
      signal: input.signal,
      throwOnError: true,
    });
    log.info("prompt submitted");
    await checkpoint("prompt_submitted");

    const iterator = events.stream[Symbol.asyncIterator]();
    while (true) {
      const next = abortPromise
        ? await Promise.race([iterator.next(), abortPromise])
        : await iterator.next();
      if (next.done) throw new Error(`opencode event stream ended before session ${sessionId} became idle`);

      const event = next.value;
      if (!isSessionEvent(event, sessionId)) continue;
      eventCount += 1;
      const eventType = diagnosticEventType(event, sessionId);
      if (eventType?.toolName) {
        toolCallCount += 1;
        lastToolName = appendWithinByteLimit(eventType.toolName, MAX_DIAGNOSTIC_TOOL_NAME_BYTES);
      }
      if (eventType?.type && eventType.type !== lastDiagnosticEvent) {
        lastDiagnosticEvent = eventType.type;
        await checkpoint("prompt_submitted", eventType.type);
      }

      const delta = textDelta(event, sessionId);
      if (delta && outputBytes < maxOutputBytes) {
        const appended = appendWithinByteLimit(delta, maxOutputBytes - outputBytes);
        output += appended;
        outputBytes += Buffer.byteLength(appended);
      }

      if (event.type === "permission.updated") {
        await checkpoint("permission_requested", event.type);
        await client.postSessionIdPermissionsPermissionId({
          path: { id: sessionId, permissionID: event.properties.id },
          body: { response: "reject" },
          throwOnError: true,
        });
        throw new Error(`opencode session ${sessionId} requested permission ${event.properties.id}`);
      }

      if (event.type === "session.error") {
        await checkpoint("session_error", event.type);
        const message = formatOpencodeError(event.properties.error);
        throw new Error(`opencode session ${sessionId} error: ${message}`);
      }

      if (isIdleEvent(event)) {
        await checkpoint("idle", event.type);
        log.info("opencode session completed");
        return { output, sessionId };
      }
    }
  } finally {
    removeAbortListener?.();
    if (events) void events.stream.return(undefined).catch(() => undefined);
  }
}

export async function createSession(client: OpencodeClient, title: string): Promise<string> {
  const { data } = await client.session.create({ body: { title }, throwOnError: true });
  return data.id;
}

export async function* runPrompt(input: {
  agentName: string;
  client: OpencodeClient;
  sessionID: string;
  text: string;
}): AsyncIterable<string> {
  const events = await input.client.event.subscribe({});
  await input.client.session.promptAsync({
    path: { id: input.sessionID },
    body: { agent: input.agentName, parts: [{ type: "text", text: input.text }] },
    throwOnError: true,
  });

  for await (const event of events.stream) {
    if (!isSessionEvent(event, input.sessionID)) continue;
    const delta = textDelta(event, input.sessionID);
    if (delta) yield delta;

    if (event.type === "permission.updated") {
      await input.client.postSessionIdPermissionsPermissionId({
        path: { id: input.sessionID, permissionID: event.properties.id },
        body: { response: "reject" },
        throwOnError: true,
      });
      throw new Error(`opencode session ${input.sessionID} requested permission ${event.properties.id}`);
    }
    if (event.type === "session.error") {
      throw new Error(`opencode session ${input.sessionID} error: ${formatOpencodeError(event.properties.error)}`);
    }
    if (isIdleEvent(event)) return;
  }

  throw new Error(`opencode event stream ended before session ${input.sessionID} became idle`);
}

function appendWithinByteLimit(value: string, remainingBytes: number): string {
  if (Buffer.byteLength(value) <= remainingBytes) return value;

  let result = "";
  let bytes = 0;
  for (const character of value) {
    const characterBytes = Buffer.byteLength(character);
    if (bytes + characterBytes > remainingBytes) break;
    result += character;
    bytes += characterBytes;
  }
  return result;
}

function parseSessionStatus(value: unknown, sessionId: string): "idle" | "busy" | "retry" | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("opencode session status response was malformed");
  }

  const entry = (value as Record<string, unknown>)[sessionId];
  if (entry === undefined) return undefined;
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
    throw new Error(`opencode status for session ${sessionId} was malformed`);
  }

  const type = (entry as { type?: unknown }).type;
  if (type === "idle" || type === "busy" || type === "retry") return type;
  throw new Error(`opencode status for session ${sessionId} was malformed`);
}

function assistantText(value: unknown, sessionId: string, maxOutputBytes: number): string {
  if (!Array.isArray(value)) throw new Error("opencode session messages response was malformed");

  let output = "";
  let outputBytes = 0;
  let userMessages = 0;
  let assistantMessages = 0;
  for (const message of value) {
    if (!message || typeof message !== "object" || Array.isArray(message)) {
      throw new Error("opencode session messages response was malformed");
    }
    const { info, parts } = message as { info?: unknown; parts?: unknown };
    if (!info || typeof info !== "object" || Array.isArray(info) || !Array.isArray(parts)) {
      throw new Error("opencode session messages response was malformed");
    }

    const messageInfo = info as { error?: unknown; role?: unknown; sessionID?: unknown; time?: { completed?: unknown } };
    if ((messageInfo.role !== "assistant" && messageInfo.role !== "user")
      || typeof messageInfo.sessionID !== "string") {
      throw new Error("opencode session messages response was malformed");
    }
    if (messageInfo.sessionID !== sessionId) continue;
    if (messageInfo.role === "user") {
      userMessages += 1;
      continue;
    }
    if (messageInfo.error !== undefined || typeof messageInfo.time?.completed !== "number") {
      throw new Error(`opencode session ${sessionId} does not contain a completed prompt exchange`);
    }
    assistantMessages += 1;

    for (const part of parts) {
      if (!part || typeof part !== "object" || Array.isArray(part) || typeof (part as { type?: unknown }).type !== "string") {
        throw new Error("opencode session messages response was malformed");
      }
      const textPart = part as { sessionID?: unknown; text?: unknown; type: string };
      if (textPart.type !== "text") continue;
      if (textPart.sessionID !== sessionId || typeof textPart.text !== "string") {
        throw new Error("opencode session messages response was malformed");
      }
      if (outputBytes >= maxOutputBytes) continue;
      const appended = appendWithinByteLimit(textPart.text, maxOutputBytes - outputBytes);
      output += appended;
      outputBytes += Buffer.byteLength(appended);
    }
  }
  if (userMessages === 0 || assistantMessages === 0) {
    throw new Error(`opencode session ${sessionId} does not contain a completed prompt exchange`);
  }
  return output;
}

function textDelta(event: Event, sessionId: string): string | undefined {
  const maybe = event as {
    type: string;
    properties: {
      delta?: string;
      field?: string;
      sessionID?: string;
      part?: { sessionID?: string; type?: string };
    };
  };

  if (maybe.type === "message.part.delta") {
    return maybe.properties.sessionID === sessionId && maybe.properties.field === "text"
      ? maybe.properties.delta
      : undefined;
  }

  if (maybe.type === "message.part.updated") {
    return maybe.properties.part?.sessionID === sessionId && maybe.properties.part.type === "text"
      ? maybe.properties.delta
      : undefined;
  }
}

function diagnosticEventType(event: Event, sessionId: string): { type: string; toolName?: string } | undefined {
  const maybe = event as unknown as { type: string; properties?: { sessionID?: string; status?: { type?: string }; part?: { sessionID?: string; type?: string; tool?: string } } };
  if (maybe.type === "message.part.delta") return { type: "message.text" };
  if (maybe.type === "message.part.updated") {
    const part = maybe.properties?.part;
    if (!part || part.sessionID !== sessionId || typeof part.type !== "string") return undefined;
    return part.type === "tool" ? { type: "message.tool", ...(typeof part.tool === "string" ? { toolName: part.tool } : {}) } : { type: `message.${part.type}` };
  }
  if (maybe.type === "session.status") return { type: `session.${maybe.properties?.status?.type ?? "unknown"}` };
  return { type: maybe.type };
}

function isSessionEvent(event: Event, sessionId: string): boolean {
  switch (event.type) {
    case "message.part.updated":
      return event.properties.part.sessionID === sessionId;
    case "session.idle":
    case "session.status":
    case "session.error":
    case "session.compacted":
    case "permission.updated":
      return event.properties.sessionID === sessionId;
    default: {
      const maybe = event as { type: string; properties?: { sessionID?: string } };
      return maybe.type === "message.part.delta" && maybe.properties?.sessionID === sessionId;
    }
  }
}

function isIdleEvent(event: Event): boolean {
  return event.type === "session.idle"
    || (event.type === "session.status" && event.properties.status.type === "idle");
}

function formatOpencodeError(error: unknown): string {
  if (!error || typeof error !== "object") return "unknown error";

  const maybe = error as { data?: { message?: unknown }; name?: unknown };
  const name = typeof maybe.name === "string" ? maybe.name : "Error";
  const message = typeof maybe.data?.message === "string" ? maybe.data.message : JSON.stringify(error);
  return `${name}: ${message}`;
}
