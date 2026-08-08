import { afterEach, describe, expect, it, vi } from "vitest";
import { waitForOpencodeReady } from "../../src/agent/client.js";

describe("waitForOpencodeReady", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("passes authentication and the abort signal to fetch", async () => {
    const controller = new AbortController();
    const fetch = vi.fn(async () => new Response("ok"));
    vi.stubGlobal("fetch", fetch);

    await waitForOpencodeReady(
      { password: "secret", host: "::1" },
      { directory: "/workspace", port: 4096, readyTimeoutMs: 100 },
      controller.signal,
    );

    expect(fetch).toHaveBeenCalledWith("http://[::1]:4096/global/health", expect.objectContaining({
      headers: { Authorization: `Basic ${Buffer.from("opencode:secret").toString("base64")}` },
      signal: expect.any(AbortSignal),
    }));
  });

  it("stops an in-flight readiness request when aborted", async () => {
    const controller = new AbortController();
    vi.stubGlobal("fetch", vi.fn((_url: string, init: RequestInit) => new Promise((_resolve, reject) => {
      init.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
    })));

    const readiness = waitForOpencodeReady(
      { password: "secret", host: "agent.test" },
      { directory: "/workspace", port: 4096, readyTimeoutMs: 10_000 },
      controller.signal,
    );
    controller.abort(new Error("cancelled"));

    await expect(readiness).rejects.toThrow("cancelled");
  });
});
