import type { ExecutionState } from "./states.js";
import type { AttemptState } from "../dispatch/states.js";
import type { ResolvedWorkspace } from "../workspace/types.js";

export type JsonPrimitive = boolean | null | number | string;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export type AgentProfileDefinition = {
  readonly schemaVersion: 1;
  readonly runtime: {
    readonly type: "opencode";
    readonly agent: string;
    readonly opencodeConfig: JsonObject;
  };
  readonly sandbox: {
    readonly templateName: string;
    readonly warmPool: string;
  };
  readonly connections: readonly {
    readonly id: string;
    readonly sidecar: string;
  }[];
  readonly permissions: {
    readonly onRequest: "fail";
  };
  readonly timeoutSeconds: number;
  readonly retention?: {
    readonly sandboxSecondsAfterFinished: number;
  };
};

export type AgentProfileRef = {
  id: string;
  version: number;
};

export type BindingRef = {
  id: string;
  version: number;
};

export type AgentProfileVersion = {
  id: string;
  tenantId: string;
  profile: AgentProfileRef;
  definition: AgentProfileDefinition;
  createdAt: string;
};

export type ExecutionInput = {
  text: string;
  context?: Record<string, JsonValue>;
};

export type Execution = {
  binding: BindingRef;
  id: string;
  tenantId: string;
  state: ExecutionState;
  profile: AgentProfileRef;
  input: ExecutionInput;
  workspace: ResolvedWorkspace;
  eventId: string;
  createdAt: string;
  updatedAt: string;
  result: JsonValue | null;
};

export type ExecutionAttempt = {
  attempt: number;
  state: AttemptState;
  startedAt: string | null;
  finishedAt: string | null;
  leaseExpiresAt: string | null;
  opencodeSessionId: string | null;
  workloadName: string | null;
  diagnostic: ExecutionAttemptDiagnostic | null;
};

export type ExecutionAttemptDiagnostic = {
  schemaVersion: 1;
  phase: "session_created" | "subscribed" | "prompt_submitted" | "idle" | "permission_requested" | "session_error" | "aborted";
  updatedAt: string;
  lastEventAt?: string;
  lastEventType?: string;
  toolCallCount?: number;
  lastToolName?: string;
  eventCount: number;
  termination?: "deadline" | "cancellation" | "lease_lost" | "error";
};

export type ExecutionStateTransition = {
  id: string;
  attempt: number | null;
  sequence: number;
  fromState: ExecutionState | null;
  toState: ExecutionState;
  actor: string;
  reason: string | null;
  createdAt: string;
  traceContext: Record<string, string>;
};

export type EventWaitState = "PENDING_CONTEXT" | "ACTIVE" | "CANCELLED" | "EXPIRED" | "CONSUMED";

export type ExecutionEventWait = {
  id: string;
  attempt: number;
  name: string;
  state: EventWaitState;
  correlation: Record<string, JsonPrimitive>;
  deadlineAt: string;
  activatedAt: string;
  endedAt: string | null;
};

export type ExecutionDetail = Execution & {
  attempts: ExecutionAttempt[];
  transitions: ExecutionStateTransition[];
  waits: ExecutionEventWait[];
};

export type RequestExecutionCancellationCommand = {
  tenantId: string;
  executionId: string;
  transitionId: string;
  actor: string;
  reason: string;
  requestedAt: string;
};

export type RequestExecutionCancellationResult =
  | { outcome: "CANCELLED"; id: string; state: "CANCELLED" }
  | { outcome: "REQUESTED"; id: string; state: "CANCEL_REQUESTED" };

export class ProfileVersionAlreadyExistsError extends Error {
  readonly code = "PROFILE_VERSION_ALREADY_EXISTS";

  constructor(profileId: string, version: number) {
    super(`Agent profile ${profileId} version ${version} already exists`);
    this.name = "ProfileVersionAlreadyExistsError";
  }
}

export class ProfileVersionNotFoundError extends Error {
  readonly code = "PROFILE_VERSION_NOT_FOUND";

  constructor(profileId: string, version: number) {
    super(`Agent profile ${profileId} version ${version} was not found`);
    this.name = "ProfileVersionNotFoundError";
  }
}

export class ExecutionNotFoundError extends Error {
  readonly code = "EXECUTION_NOT_FOUND";

  constructor(executionId: string) {
    super(`Execution ${executionId} was not found`);
    this.name = "ExecutionNotFoundError";
  }
}

export class ExecutionCancellationConflictError extends Error {
  readonly code = "EXECUTION_CANCELLATION_CONFLICT";

  constructor(executionId: string) {
    super(`Execution ${executionId} cannot be cancelled in its current state`);
    this.name = "ExecutionCancellationConflictError";
  }
}

export class IdempotencyConflictError extends Error {
  readonly code = "IDEMPOTENCY_CONFLICT";

  constructor() {
    super("Idempotency-Key was already used for a different request");
    this.name = "IdempotencyConflictError";
  }
}
