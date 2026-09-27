import { logger, toErrCtx, type Logger } from "../logger.js";
import type { ExecutionAttemptProvisioner } from "../sandbox/types.js";
import { parseExecutionAttemptProfile, type ExecutionAttemptProfile } from "./profile.js";
import type { DispatcherExecutionStore } from "./store.js";
import type { ClaimedExecution, TransitionLeasedExecutionCommand, TransitionLeasedExecutionResult } from "./types.js";
import {
  ExecutionCancellationRequestedError,
  ExecutionLeaseLostError,
  startExecutionLeaseHeartbeat,
} from "./heartbeat.js";
import type { ExecutionAttemptDiagnostic, JsonValue } from "../execution/types.js";
import { observeExecutionAttempt, runExecutionAttempt } from "../agent/runner.js";
import type { OpenCodeConnectionOptions } from "../agent/client.js";
import { SandboxClaimCleanupError, SandboxClaimRejectedError } from "../sandbox/provisioner.js";
import { executionDuration, executionOutcomes, sandboxProvisionDuration } from "../observability/metrics.js";

type AttemptProfile = ExecutionAttemptProfile;
type ProvisionedAttempt = Awaited<ReturnType<ExecutionAttemptProvisioner["provision"]>>;

export type ExecutionAttemptRunnerResult = {
  result: JsonValue;
  sessionId?: string;
};

export interface ExecutionAttemptRunner {
  run(input: {
    execution: ClaimedExecution;
    profile: AttemptProfile;
    provisioned: ProvisionedAttempt;
    signal: AbortSignal;
    onSession(sessionId: string): Promise<void>;
    onDiagnostic?(diagnostic: Omit<ExecutionAttemptDiagnostic, "schemaVersion" | "updatedAt" | "termination">): Promise<void>;
    sessionId?: string;
  }): Promise<ExecutionAttemptRunnerResult>;
}

export class OpenCodeExecutionAttemptRunner implements ExecutionAttemptRunner {
  constructor(private readonly connection: OpenCodeConnectionOptions) {}

  async run(input: Parameters<ExecutionAttemptRunner["run"]>[0]): Promise<ExecutionAttemptRunnerResult> {
    const endpoint = {
      ...this.connection,
      host: input.provisioned.host,
      password: input.provisioned.password,
    };
    const result = input.sessionId
      ? await observeExecutionAttempt({
          abortSessionOnSignal: (reason) => reason instanceof ExecutionCancellationRequestedError,
          endpoint,
          sessionId: input.sessionId,
          signal: input.signal,
        })
      : await runExecutionAttempt({
      agent: input.profile.resolvedPolicy.runtime.agent,
      abortSessionOnSignal: (reason) => reason instanceof ExecutionCancellationRequestedError,
      endpoint,
          onSession: input.onSession,
          onDiagnostic: input.onDiagnostic,
      prompt: input.execution.input.text,
      signal: input.signal,
      title: `dispatch execution ${input.execution.executionId} attempt ${input.execution.lease.attempt}`,
      });
    return { result: { output: result.output }, sessionId: result.sessionId };
  }
}

export type DispatcherWorkerOptions = {
  controlPlaneUrl?: string;
  idlePollMs: number;
  leaseDurationMs: number;
  maxAttempts: number;
  maxErrorLength?: number;
  maxResultBytes?: number;
  provisioner: ExecutionAttemptProvisioner;
  renewIntervalMs: number;
  retryDelayMs: number;
  runner: ExecutionAttemptRunner;
  store: DispatcherExecutionStore;
  workerId: string;
  log?: Logger;
};

export class DispatcherWorker {
  readonly #options: Required<Pick<DispatcherWorkerOptions, "maxErrorLength" | "maxResultBytes">>
    & Omit<DispatcherWorkerOptions, "maxErrorLength" | "maxResultBytes">;
  readonly #log: Logger;
  #active = false;
  #preferAdoption = true;

  constructor(options: DispatcherWorkerOptions) {
    requireNonempty("workerId", options.workerId);
    requirePositiveInteger("leaseDurationMs", options.leaseDurationMs);
    requirePositiveInteger("renewIntervalMs", options.renewIntervalMs);
    requirePositiveInteger("idlePollMs", options.idlePollMs);
    requirePositiveInteger("maxAttempts", options.maxAttempts);
    if (options.renewIntervalMs >= options.leaseDurationMs) {
      throw new RangeError("renewIntervalMs must be less than leaseDurationMs");
    }

    this.#options = {
      ...options,
      maxErrorLength: options.maxErrorLength ?? 2_048,
      maxResultBytes: options.maxResultBytes ?? 128 * 1_024,
    };
    requirePositiveInteger("maxErrorLength", this.#options.maxErrorLength);
    if (!Number.isSafeInteger(this.#options.maxResultBytes) || this.#options.maxResultBytes < 64) {
      throw new RangeError("maxResultBytes must be an integer of at least 64");
    }
    this.#log = options.log ?? logger.child({ component: "dispatcher-worker", workerId: options.workerId });
  }

  async runOne(signal?: AbortSignal): Promise<boolean> {
    if (this.#active) return false;
    this.#active = true;
    try {
      signal?.throwIfAborted();
      const lease = { leaseDurationMs: this.#options.leaseDurationMs, leaseOwner: this.#options.workerId };
      const execution = this.#preferAdoption
        ? await this.#options.store.claimExpiredRunningExecution(lease)
          ?? await this.#options.store.claimNextQueuedExecution(lease)
        : await this.#options.store.claimNextQueuedExecution(lease)
          ?? await this.#options.store.claimExpiredRunningExecution(lease);
      if (!execution) return false;
      this.#preferAdoption = !Boolean(execution.adoption);
      await this.#runClaimed(execution, signal);
      return true;
    } finally {
      this.#active = false;
    }
  }

  async run(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      let claimed = false;
      try {
        claimed = await this.runOne(signal);
      } catch (error) {
        if (signal.aborted) break;
        this.#log.error("dispatcher worker iteration failed", { err: toErrCtx(error) });
      }
      if (!claimed && !signal.aborted) await abortableDelay(this.#options.idlePollMs, signal);
    }
  }

  async #runClaimed(execution: ClaimedExecution, parentSignal?: AbortSignal): Promise<void> {
    const log = this.#log.child({
      attempt: execution.lease.attempt,
      executionId: execution.executionId,
      tenantId: execution.tenantId,
    });
    const deadline = deadlineSignal(execution.timeoutAt, parentSignal);
    const heartbeat = startExecutionLeaseHeartbeat({
      execution,
      leaseDurationMs: this.#options.leaseDurationMs,
      renewIntervalMs: this.#options.renewIntervalMs,
      signal: deadline.signal,
      store: this.#options.store,
    });
    let provisioned: ProvisionedAttempt | undefined;
    let provisioningStarted = false;
    let running = Boolean(execution.adoption);
    let terminal = false;
    let cleanupAttempted = false;
    let leaseLost = false;
    const provisioningStartedAt = process.hrtime.bigint();
    let provisioningObserved = false;
    const observeProvisioning = (result: string) => {
      if (provisioningObserved) return;
      provisioningObserved = true;
      sandboxProvisionDuration.observe(
        { tenant: execution.tenantId, profile_id: execution.profileVersion.profileId, result },
        Number(process.hrtime.bigint() - provisioningStartedAt) / 1_000_000_000,
      );
    };
    const observeOutcome = (result: string) => {
      executionOutcomes.inc({ tenant: execution.tenantId, profile_id: execution.profileVersion.profileId, result });
      executionDuration.observe(
        { tenant: execution.tenantId, profile_id: execution.profileVersion.profileId, result },
        Math.max(0, (Date.now() - execution.createdAt.getTime()) / 1_000),
      );
    };

    const releaseProvisioned = async (): Promise<boolean> => {
      cleanupAttempted = true;
      if (!provisioned) return true;
      try {
        await this.#options.provisioner.release(
          provisioned.release,
          AbortSignal.timeout(this.#options.renewIntervalMs),
        );
        return true;
      } catch (error) {
        log.error("execution workload cleanup failed", { err: toErrCtx(error) });
        return false;
      }
    };
    const checkpointDiagnostic = async (diagnostic: Omit<ExecutionAttemptDiagnostic, "schemaVersion" | "updatedAt">) => {
      try {
        await this.#options.store.checkpointLeasedExecutionAttemptDiagnostic?.({
          attempt: execution.lease.attempt,
          diagnostic: { schemaVersion: 1, ...diagnostic },
          executionId: execution.executionId,
          fencingToken: execution.lease.fencingToken,
          leaseOwner: execution.lease.leaseOwner,
          tenantId: execution.tenantId,
        });
      } catch {
        // Diagnostics must not affect execution progress or timeout handling.
      }
    };

    try {
      const profile = parseExecutionAttemptProfile(execution);
      heartbeat.assertOwned();
      provisioningStarted = true;
      const provisioningInput = {
        attempt: execution.lease.attempt,
        connections: profile.resolvedPolicy.connections,
        controlPlaneUrl: this.#options.controlPlaneUrl,
        executionId: execution.executionId,
        fencingToken: execution.lease.fencingToken,
        githubMergeCapability: profile.profileVersion.profileId === "merger" ? githubMergeCapability(execution.input) : undefined,
        opencodeConfig: profile.resolvedPolicy.runtime.opencodeConfig,
        profileVersion: {
          id: profile.profileVersion.id,
          profileId: profile.profileVersion.profileId,
          version: profile.profileVersion.version,
        },
        sandboxTemplate: profile.resolvedPolicy.sandbox.templateName,
        tenantId: execution.tenantId,
        timeoutAt: execution.timeoutAt,
        ttlSecondsAfterFinished: profile.resolvedPolicy.retention?.sandboxSecondsAfterFinished ?? 0,
        warmPool: profile.resolvedPolicy.sandbox.warmPool,
        workspace: execution.workspace,
      };
      provisioned = execution.adoption
        ? await this.#options.provisioner.adopt(provisioningInput, execution.adoption.workloadName, heartbeat.signal)
        : await this.#options.provisioner.provision(provisioningInput, heartbeat.signal);
      observeProvisioning(execution.adoption ? "adopted" : "ready");
      heartbeat.assertOwned();

      const markRunning = async (sessionId?: string): Promise<void> => {
        if (running) {
          if (sessionId) throw new Error("Runner reported an OpenCode session after execution was already running");
          return;
        }
        heartbeat.assertOwned();
        const transition = await this.#transition(execution, {
          expectedAttemptState: "LEASED",
          expectedExecutionState: "PROVISIONING",
          opencodeSessionId: sessionId,
          reason: "execution workload ready",
          targetAttemptState: "RUNNING",
          targetExecutionState: "RUNNING",
          workloadName: provisioned?.workloadName,
        });
        if (!transition.applied) await this.#handleRejectedTransition(execution, transition);
        running = true;
      };

      const runnerResult = await this.#options.runner.run({
        execution,
        onSession: markRunning,
        onDiagnostic: checkpointDiagnostic,
        profile,
        provisioned,
        sessionId: execution.adoption?.opencodeSessionId,
        signal: heartbeat.signal,
      });
      heartbeat.assertOwned();
      if (!running) await markRunning(runnerResult.sessionId);
      heartbeat.assertOwned();
      const completion = await this.#options.store.completeLeasedExecutionTurn({
        actor: this.#options.workerId,
        attempt: execution.lease.attempt,
        executionId: execution.executionId,
        fencingToken: execution.lease.fencingToken,
        leaseOwner: execution.lease.leaseOwner,
        reason: "execution turn completed",
        result: boundedResult(runnerResult.result, this.#options.maxResultBytes),
        tenantId: execution.tenantId,
      });
      if (!completion.applied) {
        await this.#handleRejectedTransition(execution, completion);
      } else if (["SUCCEEDED", "COMPLETED", "TIMED_OUT"].includes(completion.executionState)) {
        observeOutcome(completion.executionState.toLowerCase());
      }
      terminal = true;
    } catch (error) {
      if (heartbeat.cancellationRequested || error instanceof ExecutionCancellationRequestedError) {
        if (!(error instanceof SandboxClaimCleanupError) && await releaseProvisioned()) {
          if (await this.#acknowledgeCancellation(execution)) observeOutcome("cancelled");
        }
        log.info("execution processing stopped after cancellation request");
        return;
      }
      if (heartbeat.fenceLost || error instanceof ExecutionLeaseLostError || parentSignal?.aborted) {
        leaseLost = true;
        log.warn("execution processing stopped after lease loss or shutdown");
        return;
      }

      const message = sanitizeError(error, this.#options.maxErrorLength);
      const termination = heartbeat.cancellationRequested ? "cancellation" : heartbeat.fenceLost ? "lease_lost" : Date.now() >= execution.timeoutAt.getTime() ? "deadline" : "error";
      await checkpointDiagnostic({ eventCount: 0, phase: "aborted", termination });
      if (provisioningStarted && !provisioned) observeProvisioning("failed");
      log.error("execution attempt failed", { error: message });
      try {
        const timedOut = Date.now() >= execution.timeoutAt.getTime();
        if (heartbeat.fenceLost) throw new ExecutionLeaseLostError();
        if (!timedOut) heartbeat.assertOwned();
        const retry = !(error instanceof SandboxClaimRejectedError) && !timedOut
          && execution.lease.attempt < this.#options.maxAttempts
          && Date.now() + this.#options.retryDelayMs < execution.timeoutAt.getTime();
        await this.#transitionOrLose(execution, running
          ? {
              expectedAttemptState: "RUNNING",
              expectedExecutionState: "RUNNING",
              reason: "execution runner failed",
              result: { error: message },
              retryDelayMs: retry ? this.#options.retryDelayMs : undefined,
              targetAttemptState: timedOut ? "TIMED_OUT" : "FAILED",
              targetExecutionState: timedOut ? "TIMED_OUT" : retry ? "RETRY_WAIT" : "FAILED",
            }
          : {
              expectedAttemptState: "LEASED",
              expectedExecutionState: "PROVISIONING",
              reason: "execution provisioning failed",
              result: { error: message },
              retryDelayMs: retry ? this.#options.retryDelayMs : undefined,
              targetAttemptState: timedOut ? "TIMED_OUT" : "FAILED",
              targetExecutionState: timedOut ? "TIMED_OUT" : retry ? "RETRY_WAIT" : "FAILED",
            });
        if (!retry) observeOutcome(timedOut ? "timed_out" : "failed");
        terminal = true;
      } catch (transitionError) {
        if (!(transitionError instanceof ExecutionLeaseLostError)) throw transitionError;
        leaseLost = true;
      }
    } finally {
      await heartbeat.stop();
      deadline.stop();
      if (!cleanupAttempted && provisioned && (
        !running
        || (!heartbeat.fenceLost && !leaseLost && (
          !terminal
          || provisioned.release.ttlSecondsAfterFinished === 0
          || Date.now() >= execution.timeoutAt.getTime()
        ))
      )) {
        await releaseProvisioned();
      }
    }
  }

  async #transitionOrLose(
    execution: ClaimedExecution,
    command: Omit<TransitionLeasedExecutionCommand, "actor" | "attempt" | "executionId" | "fencingToken" | "leaseOwner" | "tenantId">,
  ): Promise<void> {
    const result = await this.#transition(execution, command);
    if (!result.applied) await this.#handleRejectedTransition(execution, result);
  }

  async #transition(
    execution: ClaimedExecution,
    command: Omit<TransitionLeasedExecutionCommand, "actor" | "attempt" | "executionId" | "fencingToken" | "leaseOwner" | "tenantId">,
  ): Promise<TransitionLeasedExecutionResult> {
    const { lease } = execution;
    const result = await this.#options.store.transitionLeasedExecution({
      ...command,
      actor: this.#options.workerId,
      attempt: lease.attempt,
      executionId: execution.executionId,
      fencingToken: lease.fencingToken,
      leaseOwner: lease.leaseOwner,
      tenantId: execution.tenantId,
    });
    return result;
  }

  async #handleRejectedTransition(
    _execution: ClaimedExecution,
    result: Extract<TransitionLeasedExecutionResult, { applied: false }>,
  ): Promise<never> {
    if (result.reason === "STATE_MISMATCH") throw new ExecutionCancellationRequestedError();
    throw new ExecutionLeaseLostError();
  }

  async #acknowledgeCancellation(execution: ClaimedExecution): Promise<boolean> {
    const { lease } = execution;
    try {
      const result = await this.#options.store.acknowledgeLeasedExecutionCancellation({
        actor: "dispatcher-worker",
        attempt: lease.attempt,
        executionId: execution.executionId,
        fencingToken: lease.fencingToken,
        leaseOwner: lease.leaseOwner,
        reason: "execution cancellation acknowledged by worker",
        tenantId: execution.tenantId,
      });
      return result.applied;
    } catch {
      return false;
    }
  }
}

function githubMergeCapability(input: import("../execution/types.js").ExecutionInput): import("../sandbox/types.js").ExecutionAttemptProvisioningInput["githubMergeCapability"] {
  const event = input.context?.event;
  if (!event || typeof event !== "object" || Array.isArray(event)) return undefined;
  const repository = event.repository;
  const pullRequest = event.pullRequest;
  const review = event.review;
  if (!repository || typeof repository !== "object" || Array.isArray(repository)
    || !pullRequest || typeof pullRequest !== "object" || Array.isArray(pullRequest)
    || !review || typeof review !== "object" || Array.isArray(review)) return undefined;
  const user = review.user;
  if (!user || typeof user !== "object" || Array.isArray(user)
    || review.state !== "approved" || typeof review.commitSha !== "string" || !/^[0-9a-f]{40}$/.test(review.commitSha)
    || typeof repository.id !== "number" || !Number.isSafeInteger(repository.id) || repository.id < 1
    || typeof repository.fullName !== "string" || !/^[^/\s]+\/[^/\s]+$/.test(repository.fullName)
    || typeof pullRequest.number !== "number" || !Number.isSafeInteger(pullRequest.number) || pullRequest.number < 1
    || typeof user.id !== "number" || !Number.isSafeInteger(user.id) || user.id < 1) return undefined;
  return { commitSha: review.commitSha, pullRequestNumber: pullRequest.number, repositoryFullName: repository.fullName,
    repositoryId: repository.id, reviewerId: user.id };
}

function deadlineSignal(timeoutAt: Date, parent?: AbortSignal): { signal: AbortSignal; stop(): void } {
  const controller = new AbortController();
  const abortParent = (): void => controller.abort(parent?.reason);
  if (parent?.aborted) abortParent();
  else parent?.addEventListener("abort", abortParent, { once: true });
  const timer = setTimeout(() => controller.abort(new DOMException("Execution deadline exceeded", "TimeoutError")), Math.max(0, timeoutAt.getTime() - Date.now()));
  return {
    signal: controller.signal,
    stop() {
      clearTimeout(timer);
      parent?.removeEventListener("abort", abortParent);
    },
  };
}

function boundedResult(result: JsonValue, maxBytes: number): JsonValue {
  const serialized = JSON.stringify(result);
  if (Buffer.byteLength(serialized, "utf8") <= maxBytes) return result;
  let low = 0;
  let high = serialized.length;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    const candidate = { preview: serialized.slice(0, middle), truncated: true };
    if (Buffer.byteLength(JSON.stringify(candidate), "utf8") <= maxBytes) low = middle;
    else high = middle - 1;
  }
  return { preview: serialized.slice(0, low), truncated: true };
}

function sanitizeError(error: unknown, maxLength: number): string {
  const raw = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
  return (raw.replace(/[\u0000-\u001f\u007f-\u009f]+/g, " ").trim() || "Unknown execution error").slice(0, maxLength);
}

function abortableDelay(delayMs: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timeout = setTimeout(finish, delayMs);
    signal.addEventListener("abort", finish, { once: true });
    function finish(): void {
      clearTimeout(timeout);
      signal.removeEventListener("abort", finish);
      resolve();
    }
  });
}

function requirePositiveInteger(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive integer`);
}

function requireNonempty(name: string, value: string): void {
  if (!value.trim()) throw new RangeError(`${name} must not be empty`);
}
