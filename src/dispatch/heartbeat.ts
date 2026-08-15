import type { DispatcherExecutionStore } from "./store.js";
import type { ClaimedExecution } from "./types.js";

export class ExecutionLeaseLostError extends Error {
  constructor() {
    super("Execution lease was lost");
    this.name = "ExecutionLeaseLostError";
  }
}

export class ExecutionCancellationRequestedError extends Error {
  constructor() {
    super("Execution cancellation was requested");
    this.name = "ExecutionCancellationRequestedError";
  }
}

export type ExecutionLeaseHeartbeat = {
  readonly signal: AbortSignal;
  readonly cancellationRequested: boolean;
  readonly fenceLost: boolean;
  assertOwned(): void;
  stop(): Promise<void>;
};

export function startExecutionLeaseHeartbeat(input: {
  execution: ClaimedExecution;
  leaseDurationMs: number;
  renewIntervalMs: number;
  signal?: AbortSignal;
  store: Pick<DispatcherExecutionStore, "renewExecutionLease">;
}): ExecutionLeaseHeartbeat {
  requirePositiveInteger("leaseDurationMs", input.leaseDurationMs);
  requirePositiveInteger("renewIntervalMs", input.renewIntervalMs);
  if (input.renewIntervalMs >= input.leaseDurationMs) {
    throw new RangeError("renewIntervalMs must be less than leaseDurationMs");
  }

  const controller = new AbortController();
  let cancellationRequested = false;
  let fenceLost = false;
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let expiryTimer: ReturnType<typeof setTimeout> | undefined;
  let renewal: Promise<void> | undefined;
  let confirmedExpiry = input.execution.lease.leaseExpiresAt.getTime();

  const abortFromParent = (): void => controller.abort(input.signal?.reason);
  if (input.signal?.aborted) abortFromParent();
  else input.signal?.addEventListener("abort", abortFromParent, { once: true });

  const loseFence = (): void => {
    if (stopped || fenceLost) return;
    fenceLost = true;
    controller.abort(new ExecutionLeaseLostError());
  };

  const requestCancellation = (): void => {
    if (stopped || cancellationRequested) return;
    cancellationRequested = true;
    controller.abort(new ExecutionCancellationRequestedError());
  };

  const watchExpiry = (): void => {
    clearTimeout(expiryTimer);
    if (stopped || controller.signal.aborted) return;
    expiryTimer = setTimeout(() => {
      expiryTimer = undefined;
      // A pending renewal cannot extend the last confirmed lease expiry. The
      // response is required before this worker can continue past that fence.
      loseFence();
    }, Math.max(0, confirmedExpiry - Date.now()));
  };

  const schedule = (): void => {
    if (stopped || controller.signal.aborted) return;
    timer = setTimeout(() => {
      renewal = renew().finally(() => {
        renewal = undefined;
        schedule();
      });
    }, input.renewIntervalMs);
  };

  const renew = async (): Promise<void> => {
    try {
      const execution = input.execution;
      const { lease } = execution;
      const result = await input.store.renewExecutionLease({
        attempt: lease.attempt,
        executionId: execution.executionId,
        fencingToken: lease.fencingToken,
        leaseDurationMs: input.leaseDurationMs,
        leaseOwner: lease.leaseOwner,
        tenantId: execution.tenantId,
      });
      if (result === "CANCEL_REQUESTED") requestCancellation();
      else if (result === "LOST") loseFence();
      else {
        confirmedExpiry = Math.max(
          confirmedExpiry,
          result.leaseExpiresAt.getTime(),
        );
        watchExpiry();
      }
    } catch {
      loseFence();
    }
  };

  schedule();
  watchExpiry();

  return {
    get signal() {
      return controller.signal;
    },
    get cancellationRequested() {
      return cancellationRequested;
    },
    get fenceLost() {
      return fenceLost;
    },
    assertOwned() {
      if (cancellationRequested) throw new ExecutionCancellationRequestedError();
      if (fenceLost) throw new ExecutionLeaseLostError();
      input.signal?.throwIfAborted();
    },
    async stop() {
      stopped = true;
      clearTimeout(timer);
      clearTimeout(expiryTimer);
      input.signal?.removeEventListener("abort", abortFromParent);
      if (renewal === undefined) return;
      let fallback: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          renewal,
          new Promise<void>((resolve) => {
            fallback = setTimeout(resolve, input.renewIntervalMs);
          }),
        ]);
      } finally {
        clearTimeout(fallback);
      }
    },
  };
}

function requirePositiveInteger(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive integer`);
}
