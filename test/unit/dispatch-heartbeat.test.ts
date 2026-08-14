import { afterEach, describe, expect, it, vi } from "vitest";
import { startExecutionLeaseHeartbeat } from "../../src/dispatch/heartbeat.js";

afterEach(() => vi.useRealTimers());

describe("startExecutionLeaseHeartbeat()", () => {
  it("does not lose the fence while a renewal accepted before expiry is awaiting its response", async () => {
    vi.useFakeTimers();
    let resolveRenewal!: (result: { status: "RENEWED"; leaseExpiresAt: Date }) => void;
    const renewal = new Promise<{ status: "RENEWED"; leaseExpiresAt: Date }>((resolve) => {
      resolveRenewal = resolve;
    });
    const heartbeat = startExecutionLeaseHeartbeat({
      execution: {
        executionId: "exec",
        tenantId: "tenant",
        lease: {
          attempt: 1,
          fencingToken: "fence",
          leaseOwner: "worker",
          leaseExpiresAt: new Date(Date.now() + 100),
        },
      } as never,
      leaseDurationMs: 100,
      renewIntervalMs: 20,
      store: { renewExecutionLease: vi.fn(() => renewal) } as never,
    });

    await vi.advanceTimersByTimeAsync(20);
    await vi.advanceTimersByTimeAsync(80);
    expect(heartbeat.fenceLost).toBe(false);

    resolveRenewal({ status: "RENEWED", leaseExpiresAt: new Date(Date.now() + 100) });
    await vi.advanceTimersByTimeAsync(0);
    expect(heartbeat.fenceLost).toBe(false);
    await heartbeat.stop();
  });
});

describe("startExecutionLeaseHeartbeat().stop()", () => {
  it("does not leave a live timer on the event loop when renewal never started", async () => {
    const before = process.getActiveResourcesInfo().filter((k) => k === "Timeout").length;
    const heartbeat = startExecutionLeaseHeartbeat({
      execution: {
        executionId: "exec",
        tenantId: "t",
        lease: {
          attempt: 1,
          fencingToken: "f",
          leaseOwner: "w",
          leaseExpiresAt: new Date(Date.now() + 60_000),
        },
      } as never,
      leaseDurationMs: 60_000,
      renewIntervalMs: 30_000, // ensures schedule()'s renewal timer never fires during the test
      store: {
        renewExecutionLease: () => {
          throw new Error("renewal must not run in this test");
        },
      } as never,
    });
    await heartbeat.stop();
    const after = process.getActiveResourcesInfo().filter((k) => k === "Timeout").length;
    expect(after).toBe(before);
  });
});
