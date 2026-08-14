import { afterEach, describe, expect, it, vi } from "vitest";
import type { DispatcherExecutionStore } from "../../src/dispatch/store.js";
import type {
  ClaimedExecution,
  ExecutionLeaseRenewalResult,
  TransitionLeasedExecutionCommand,
  TransitionLeasedExecutionResult,
} from "../../src/dispatch/types.js";
import { DispatcherWorker, type ExecutionAttemptRunner } from "../../src/dispatch/worker.js";
import { SandboxClaimCleanupError, SandboxClaimRejectedError } from "../../src/sandbox/provisioner.js";
import type { ExecutionAttemptEndpoint, ExecutionAttemptProvisioner } from "../../src/sandbox/types.js";

afterEach(() => vi.useRealTimers());

describe("DispatcherWorker", () => {
  it("provisions, records the session before running, succeeds, and releases", async () => {
    const fixture = workerFixture();
    fixture.runner.run = vi.fn(async ({ onSession }) => {
      await onSession("session-1");
      return { result: { text: "complete" } };
    });

    await expect(fixture.worker.runOne()).resolves.toBe(true);

    expect(fixture.store.transitions).toMatchObject([
      {
        expectedExecutionState: "PROVISIONING",
        opencodeSessionId: "session-1",
        targetExecutionState: "RUNNING",
        workloadName: "workload-1",
      },
    ]);
    expect(fixture.store.completeLeasedExecutionTurn).toHaveBeenCalledWith(expect.objectContaining({ result: { text: "complete" } }));
    expect(fixture.provisioner.release).toHaveBeenCalledWith(
      expect.objectContaining({ attempt: 1, executionId: "execution-1" }),
      expect.any(AbortSignal),
    );
    expect(fixture.provisioner.provision).toHaveBeenCalledWith(
      expect.objectContaining({ workspace: fixture.execution.workspace }),
      expect.any(AbortSignal),
    );
  });

  it("adopts a checkpointed running attempt without replaying its prompt", async () => {
    const fixture = workerFixture();
    fixture.execution.adoption = { workloadName: "workload-1", opencodeSessionId: "session-existing" };
    fixture.store.claimExpiredRunningExecution = vi.fn().mockResolvedValue(fixture.execution);
    fixture.store.claimNextQueuedExecution = vi.fn();

    await expect(fixture.worker.runOne()).resolves.toBe(true);

    expect(fixture.store.claimNextQueuedExecution).not.toHaveBeenCalled();
    expect(fixture.provisioner.provision).not.toHaveBeenCalled();
    expect(fixture.provisioner.adopt).toHaveBeenCalledWith(
      expect.objectContaining({ fencingToken: "fence-1" }),
      "workload-1",
      expect.any(AbortSignal),
    );
    expect(fixture.runner.run).toHaveBeenCalledWith(expect.objectContaining({ sessionId: "session-existing" }));
    expect(fixture.store.transitions).toEqual([]);
    expect(fixture.store.completeLeasedExecutionTurn).toHaveBeenCalledOnce();
  });

  it("retries an ordinary provisioning failure", async () => {
    const fixture = workerFixture();
    fixture.provisioner.provision = vi.fn().mockRejectedValue(new Error("bad\u0000 secret-ish failure"));

    await fixture.worker.runOne();

    expect(fixture.store.transitions).toHaveLength(1);
    expect(fixture.store.transitions[0]).toMatchObject({
      expectedAttemptState: "LEASED",
      expectedExecutionState: "PROVISIONING",
      result: { error: "Error: bad  secret-ish failure" },
      retryDelayMs: 10,
      targetAttemptState: "FAILED",
      targetExecutionState: "RETRY_WAIT",
    });
    expect(fixture.provisioner.release).not.toHaveBeenCalled();
  });

  it("fails directly when the provisioner rejects the SandboxClaim", async () => {
    const fixture = workerFixture();
    fixture.provisioner.provision = vi.fn().mockRejectedValue(
      new SandboxClaimRejectedError("execution-1-1", "ReconcilerError: required sidecar is missing"),
    );

    await fixture.worker.runOne();

    expect(fixture.store.transitions).toHaveLength(1);
    expect(fixture.store.transitions[0]).toMatchObject({
      expectedAttemptState: "LEASED",
      expectedExecutionState: "PROVISIONING",
      result: {
        error: "SandboxClaimRejectedError: SandboxClaim execution-1-1 was rejected: ReconcilerError: required sidecar is missing",
      },
      targetAttemptState: "FAILED",
      targetExecutionState: "FAILED",
    });
    expect(fixture.store.transitions[0]?.retryDelayMs).toBeUndefined();
    expect(fixture.store.transitions[0]?.targetExecutionState).not.toBe("RETRY_WAIT");
    expect(fixture.provisioner.release).not.toHaveBeenCalled();
  });

  it("passes persisted workspace and exact connection grants through on a retry", async () => {
    const fixture = workerFixture();
    fixture.execution.lease.attempt = 2;
    fixture.execution.workspace = {
      repository: { url: "https://example.com/repo.git" },
      revision: { commit: "0123456789abcdef", type: "commit" },
      type: "git",
    };
    const sandbox = fixture.execution.resolvedPolicy.sandbox as { templateName: string; warmPool: string };
    sandbox.warmPool = "none";
    fixture.execution.resolvedPolicy.connections = [
      { id: "github", sidecar: "mcp-proxy" },
      { id: "linear", sidecar: "mcp-proxy" },
    ];

    await fixture.worker.runOne();

    const provisioned = vi.mocked(fixture.provisioner.provision).mock.calls[0]![0];
    expect(provisioned.workspace).toBe(fixture.execution.workspace);
    expect(provisioned.attempt).toBe(2);
    expect(provisioned.connections).toEqual([
      { id: "github", sidecar: "mcp-proxy" },
      { id: "linear", sidecar: "mcp-proxy" },
    ]);
    expect(JSON.stringify(provisioned.connections)).not.toMatch(/credential|secret|token/);
  });

  it("fails the running pair when the runner fails", async () => {
    const fixture = workerFixture();
    fixture.runner.run = vi.fn(async ({ onSession }) => {
      await onSession("session-1");
      throw new Error("prompt failed");
    });

    await fixture.worker.runOne();

    expect(fixture.store.transitions).toMatchObject([
      { targetExecutionState: "RUNNING" },
      {
        expectedExecutionState: "RUNNING",
        result: { error: "Error: prompt failed" },
        targetAttemptState: "FAILED",
        targetExecutionState: "RETRY_WAIT",
      },
    ]);
    expect(fixture.provisioner.release).toHaveBeenCalledOnce();
  });

  it("aborts the runner and performs no late transition after heartbeat fence loss", async () => {
    vi.useFakeTimers();
    const fixture = workerFixture({ renew: "LOST" });
    fixture.runner.run = vi.fn(({ onSession, signal }) => new Promise<never>(async (_resolve, reject) => {
      await onSession("session-1");
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    }));

    const running = fixture.worker.runOne();
    await vi.waitFor(() => expect(fixture.store.transitions).toHaveLength(1));
    await vi.advanceTimersByTimeAsync(10);
    await running;

    expect(fixture.store.renewExecutionLease).toHaveBeenCalledOnce();
    expect(fixture.store.transitions).toHaveLength(1);
    expect(fixture.provisioner.release).not.toHaveBeenCalled();
  });

  it("aborts provisioning and acknowledges a heartbeat cancellation with the exact lease", async () => {
    vi.useFakeTimers();
    const fixture = workerFixture({ renew: "CANCEL_REQUESTED" });
    fixture.provisioner.provision = vi.fn((_input, signal) => new Promise<never>((_resolve, reject) => {
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    }));

    const running = fixture.worker.runOne();
    await vi.waitFor(() => expect(fixture.provisioner.provision).toHaveBeenCalledOnce());
    await vi.advanceTimersByTimeAsync(10);
    await running;

    expect(fixture.store.acknowledgeLeasedExecutionCancellation).toHaveBeenCalledWith({
      actor: "dispatcher-worker",
      attempt: 1,
      executionId: "execution-1",
      fencingToken: "fence-1",
      leaseOwner: "worker-1",
      reason: "execution cancellation acknowledged by worker",
      tenantId: "tenant-1",
    });
    expect(fixture.provisioner.release).not.toHaveBeenCalled();
    expect(fixture.store.transitions).toHaveLength(0);
  });

  it("does not acknowledge or release again when cancelled provisioning rejects with SandboxClaimCleanupError", async () => {
    vi.useFakeTimers();
    const fixture = workerFixture({ renew: "CANCEL_REQUESTED" });
    fixture.provisioner.provision = vi.fn((_input, signal) => new Promise<never>((_resolve, reject) => {
      signal.addEventListener("abort", () => reject(new SandboxClaimCleanupError({
        cause: new Error("claim deletion failed"),
      })), { once: true });
    }));

    const running = fixture.worker.runOne();
    await vi.waitFor(() => expect(fixture.provisioner.provision).toHaveBeenCalledOnce());
    await vi.advanceTimersByTimeAsync(10);
    await running;

    expect(fixture.store.acknowledgeLeasedExecutionCancellation).not.toHaveBeenCalled();
    expect(fixture.provisioner.release).not.toHaveBeenCalled();
    expect(fixture.store.transitions).toHaveLength(0);
  });

  it("aborts a running attempt on heartbeat cancellation without failing or retrying it", async () => {
    vi.useFakeTimers();
    const fixture = workerFixture({ renew: "CANCEL_REQUESTED" });
    fixture.runner.run = vi.fn(({ onSession, signal }) => new Promise<never>(async (_resolve, reject) => {
      await onSession("session-1");
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    }));

    const running = fixture.worker.runOne();
    await vi.waitFor(() => expect(fixture.store.transitions).toHaveLength(1));
    await vi.advanceTimersByTimeAsync(10);
    await running;

    expect(fixture.store.transitions).toHaveLength(1);
    expect(fixture.store.acknowledgeLeasedExecutionCancellation).toHaveBeenCalledOnce();
    expect(fixture.provisioner.release).toHaveBeenCalledOnce();
    expect(vi.mocked(fixture.provisioner.release).mock.invocationCallOrder[0]).toBeLessThan(
      vi.mocked(fixture.store.acknowledgeLeasedExecutionCancellation).mock.invocationCallOrder[0]!,
    );
  });

  it("does not acknowledge or retry cleanup when cancellation release fails", async () => {
    vi.useFakeTimers();
    const fixture = workerFixture({ renew: "CANCEL_REQUESTED" });
    fixture.provisioner.release = vi.fn().mockRejectedValue(new Error("release failed"));
    fixture.runner.run = vi.fn(({ onSession, signal }) => new Promise<never>(async (_resolve, reject) => {
      await onSession("session-1");
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    }));

    const running = fixture.worker.runOne();
    await vi.waitFor(() => expect(fixture.store.transitions).toHaveLength(1));
    await vi.advanceTimersByTimeAsync(10);
    await running;

    expect(fixture.provisioner.release).toHaveBeenCalledOnce();
    expect(fixture.store.acknowledgeLeasedExecutionCancellation).not.toHaveBeenCalled();
    expect(fixture.store.transitions).toHaveLength(1);
  });

  it("acknowledges cancellation when the transition to running loses a state race", async () => {
    const fixture = workerFixture();
    vi.mocked(fixture.store.transitionLeasedExecution).mockResolvedValueOnce({ applied: false, reason: "STATE_MISMATCH" });

    await fixture.worker.runOne();

    expect(fixture.store.acknowledgeLeasedExecutionCancellation).toHaveBeenCalledOnce();
    expect(fixture.store.transitionLeasedExecution).toHaveBeenCalledOnce();
    expect(fixture.provisioner.release).toHaveBeenCalledOnce();
    expect(vi.mocked(fixture.provisioner.release).mock.invocationCallOrder[0]).toBeLessThan(
      vi.mocked(fixture.store.acknowledgeLeasedExecutionCancellation).mock.invocationCallOrder[0]!,
    );
  });

  it("acknowledges cancellation when the success transition loses a state race", async () => {
    const fixture = workerFixture();
    vi.mocked(fixture.store.completeLeasedExecutionTurn).mockResolvedValueOnce({ applied: false, reason: "STATE_MISMATCH" });

    await fixture.worker.runOne();

    expect(fixture.store.acknowledgeLeasedExecutionCancellation).toHaveBeenCalledOnce();
    expect(fixture.store.transitionLeasedExecution).toHaveBeenCalledTimes(1);
    expect(fixture.provisioner.release).toHaveBeenCalledOnce();
    expect(vi.mocked(fixture.provisioner.release).mock.invocationCallOrder[0]).toBeLessThan(
      vi.mocked(fixture.store.acknowledgeLeasedExecutionCancellation).mock.invocationCallOrder[0]!,
    );
  });

  it("rejects invalid heartbeat timing and prevents overlapping runOne calls", async () => {
    const fixture = workerFixture();
    expect(() => fixture.createWorker({ renewIntervalMs: 100, leaseDurationMs: 100 })).toThrow(/less than/);

    let finish!: () => void;
    fixture.provisioner.provision = vi.fn(() => new Promise<ExecutionAttemptEndpoint>((resolve) => {
      finish = () => resolve({ host: "sandbox", password: "password", workloadName: "workload-1", release: provisioningInput() });
    }));
    const first = fixture.worker.runOne();
    await vi.waitFor(() => expect(fixture.provisioner.provision).toHaveBeenCalledOnce());
    await expect(fixture.worker.runOne()).resolves.toBe(false);
    finish();
    await first;
    expect(fixture.store.claimExpiredRunningExecution).toHaveBeenCalledOnce();
    expect(fixture.store.claimNextQueuedExecution).toHaveBeenCalledOnce();
  });
});

function workerFixture(overrides: { renew?: ExecutionLeaseRenewalResult } = {}) {
  const execution = claimedExecution();
  const transitions: TransitionLeasedExecutionCommand[] = [];
  const store = {
    claimExpiredRunningExecution: vi.fn().mockResolvedValue(undefined),
    claimNextQueuedExecution: vi.fn().mockResolvedValue(execution),
    acknowledgeLeasedExecutionCancellation: vi.fn().mockResolvedValue({ applied: true as const }),
    listRequestedCancellationCleanups: vi.fn().mockResolvedValue([]),
    finalizeRequestedExecutionCancellation: vi.fn().mockResolvedValue(undefined),
    renewExecutionLease: vi.fn().mockResolvedValue(overrides.renew ?? {
      status: "RENEWED",
      leaseExpiresAt: new Date(Date.now() + 100),
    }),
    completeLeasedExecutionTurn: vi.fn().mockResolvedValue({ applied: true as const, attemptState: "SUCCEEDED" as const, executionState: "SUCCEEDED" as const }),
    expireDueEventWaits: vi.fn().mockResolvedValue([]),
    transitionLeasedExecution: vi.fn(async (command: TransitionLeasedExecutionCommand): Promise<TransitionLeasedExecutionResult> => {
      transitions.push(command);
      return {
        applied: true as const,
        attemptState: command.targetAttemptState,
        executionState: command.targetExecutionState,
      };
    }),
  } as unknown as DispatcherExecutionStore & { transitions: TransitionLeasedExecutionCommand[] };
  store.transitions = transitions;

  const provisioner: ExecutionAttemptProvisioner = {
    adopt: vi.fn().mockResolvedValue({ host: "sandbox", password: "password", workloadName: "workload-1", release: provisioningInput() }),
    provision: vi.fn().mockResolvedValue({ host: "sandbox", password: "password", workloadName: "workload-1", release: provisioningInput() }),
    release: vi.fn().mockResolvedValue(undefined),
  };
  const runner: ExecutionAttemptRunner = {
    run: vi.fn().mockResolvedValue({ result: null, sessionId: "session-1" }),
  };
  const createWorker = (options: Partial<ConstructorParameters<typeof DispatcherWorker>[0]> = {}) => new DispatcherWorker({
    idlePollMs: 20,
    leaseDurationMs: 100,
    maxAttempts: 3,
    provisioner,
    renewIntervalMs: 10,
    retryDelayMs: 10,
    runner,
    store,
    workerId: "worker-1",
    ...options,
  });

  return { createWorker, execution, provisioner, runner, store, worker: createWorker() };
}

function provisioningInput() {
  return {
    attempt: 1,
    connections: [],
    fencingToken: "fence-1",
    executionId: "execution-1",
    opencodeConfig: { agent: { coder: {} } },
    profileVersion: { id: "profile-version-1", profileId: "coder", version: 1 },
    sandboxTemplate: "opencode",
    tenantId: "tenant-1",
    timeoutAt: new Date(Date.now() + 60_000),
    ttlSecondsAfterFinished: 0,
    warmPool: "none",
    workspace: { type: "empty" as const },
  };
}

function claimedExecution(): ClaimedExecution {
  return {
    createdAt: new Date("2026-01-01T00:00:00Z"),
    eventId: "event-1",
    executionId: "execution-1",
    input: { text: "do work" },
    lease: {
      attempt: 1,
      fencingToken: "fence-1",
      leaseExpiresAt: new Date(Date.now() + 100),
      leaseOwner: "worker-1",
    },
    profileVersion: {
      definition: {
        schemaVersion: 1,
        runtime: {
          agent: "coder",
          opencodeConfig: { agent: { coder: { prompt: "Test" } } },
          type: "opencode",
        },
        sandbox: { templateName: "opencode", warmPool: "opencode" },
        permissions: { onRequest: "fail" },
        timeoutSeconds: 60,
      },
      id: "profile-version-1",
      profileId: "coder",
      version: 1,
    },
    resolvedPolicy: {
      connections: [],
      schemaVersion: 1,
      runtime: { agent: "coder", opencodeConfig: { agent: { coder: { prompt: "Test" } } }, type: "opencode" },
      sandbox: { templateName: "opencode", warmPool: "opencode" },
      permissions: { onRequest: "fail" },
      timeoutSeconds: 60,
    },
    tenantId: "tenant-1",
    timeoutAt: new Date(Date.now() + 60_000),
    workspace: { type: "empty" },
  };
}
