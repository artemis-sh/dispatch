import type { ExecutionState } from "../execution/states.js";
import type { ExecutionAttemptDiagnostic, ExecutionInput, JsonObject, JsonValue } from "../execution/types.js";
import type { ResolvedWorkspace } from "../workspace/types.js";
import type { AttemptState } from "./states.js";

export type ExecutionLease = {
  attempt: number;
  fencingToken: string;
  leaseOwner: string;
  leaseExpiresAt: Date;
};

export type ExecutionLeaseRenewalResult =
  | { status: "RENEWED"; leaseExpiresAt: Date }
  | "CANCEL_REQUESTED"
  | "LOST";

export type AcknowledgeLeasedExecutionCancellationCommand = {
  executionId: string;
  tenantId: string;
  attempt: number;
  fencingToken: string;
  leaseOwner: string;
  actor: string;
  reason: string;
};

export type AcknowledgeLeasedExecutionCancellationResult =
  | { applied: true }
  | {
      applied: false;
      reason: "NOT_FOUND" | "STATE_MISMATCH" | "LEASE_MISMATCH" | "LEASE_EXPIRED";
    };

/** Immutable inputs needed to provision and run one execution. */
export type ClaimedExecution = {
  adoption?: {
    workloadName: string;
    opencodeSessionId: string;
  };
  executionId: string;
  tenantId: string;
  eventId: string;
  profileVersion: {
    id: string;
    profileId: string;
    version: number;
    definition: JsonObject;
  };
  input: ExecutionInput;
  workspace: ResolvedWorkspace;
  resolvedPolicy: JsonObject;
  createdAt: Date;
  timeoutAt: Date;
  lease: ExecutionLease;
};

export type TransitionLeasedExecutionCommand = {
  executionId: string;
  tenantId: string;
  attempt: number;
  fencingToken: string;
  leaseOwner: string;
  expectedExecutionState: ExecutionState;
  expectedAttemptState: AttemptState;
  targetExecutionState: ExecutionState;
  targetAttemptState: AttemptState;
  actor: string;
  reason: string;
  result?: JsonValue;
  workloadName?: string;
  opencodeSessionId?: string;
  retryDelayMs?: number;
};

export type TransitionLeasedExecutionResult =
  | {
      applied: true;
      executionState: ExecutionState;
      attemptState: AttemptState;
    }
  | {
      applied: false;
      reason: "NOT_FOUND" | "STATE_MISMATCH" | "LEASE_MISMATCH" | "LEASE_EXPIRED" | "DEADLINE_NOT_ELAPSED";
    };

export type CheckpointLeasedExecutionAttemptDiagnosticCommand = {
  executionId: string;
  tenantId: string;
  attempt: number;
  fencingToken: string;
  leaseOwner: string;
  diagnostic: Omit<ExecutionAttemptDiagnostic, "updatedAt">;
};

export type CompleteLeasedExecutionTurnCommand = {
  executionId: string;
  tenantId: string;
  attempt: number;
  fencingToken: string;
  leaseOwner: string;
  actor: string;
  reason: string;
  result: JsonValue;
};

export type CompleteLeasedExecutionTurnResult =
  | { applied: true; attemptState: "SUCCEEDED" | "TIMED_OUT" | "FAILED"; executionState: "SUCCEEDED" | "WAITING" | "QUEUED" | "COMPLETED" | "TIMED_OUT" | "FAILED"; eventWaitId?: string }
  | { applied: false; reason: "NOT_FOUND" | "STATE_MISMATCH" | "LEASE_MISMATCH" | "LEASE_EXPIRED" };

export type ExpiredEventWait = {
  eventWaitId: string;
  executionId: string;
  tenantId: string;
  expiredAt: Date;
};

export type RecoveredExecutionLease = {
  executionId: string;
  tenantId: string;
  attempt: number;
  executionState: "RETRY_WAIT" | "FAILED" | "TIMED_OUT";
  recoveredAt: Date;
};

export type PromotedExecutionRetry = {
  executionId: string;
  executionState: "QUEUED" | "TIMED_OUT";
  tenantId: string;
  promotedAt: Date;
};

export type RequestedCancellationCleanup = {
  executionId: string;
  tenantId: string;
  attempt: number | null;
  workloadName: string | null;
};

export type FinalizeRequestedExecutionCancellationCommand = RequestedCancellationCleanup;

export type FinalizedRequestedExecutionCancellation = {
  executionId: string;
  tenantId: string;
  attempt: number | null;
  finalizedAt: Date;
};

export interface ExecutionCancellationCleaner {
  releaseCancelledExecution(candidate: RequestedCancellationCleanup, signal: AbortSignal): Promise<void>;
}
