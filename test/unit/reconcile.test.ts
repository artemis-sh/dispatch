import { describe, expect, it, vi } from "vitest";

vi.mock("../../src/sandbox/client.js", () => ({
  createKubeConfig: vi.fn(),
  createCustomObjectsApi: vi.fn().mockReturnValue({
    listNamespacedCustomObject: vi.fn().mockResolvedValue({ items: [] }),
    deleteNamespacedCustomObject: vi.fn().mockResolvedValue({}),
  }),
}));

import { isNotFound, readApiVersion, reconcileOnce } from "../../src/reconcile.js";
import type { SandboxClaim } from "../../src/sandbox/types.js";

const NAMESPACE = "test-ns";
const API_VERSION = "v1alpha1" as const;
const BASE_OPTS = { namespace: NAMESPACE, apiVersion: API_VERSION, graceMinutes: 30, now: Date.now() };

function claim(name: string, shutdownTime?: string): SandboxClaim {
  return {
    apiVersion: "extensions.agents.x-k8s.io/v1alpha1",
    kind: "SandboxClaim",
    metadata: { name, namespace: NAMESPACE },
    spec: { sandboxTemplateRef: { name: "tmpl" }, ...(shutdownTime ? { lifecycle: { shutdownTime } } : {}) },
  };
}
function msAgo(ms: number): string { return new Date(BASE_OPTS.now - ms).toISOString(); }
function msFromNow(ms: number): string { return new Date(BASE_OPTS.now + ms).toISOString(); }
function fakeApi(items: SandboxClaim[], deleteError?: unknown) {
  const list = vi.fn().mockResolvedValue({ items });
  const del = deleteError ? vi.fn().mockRejectedValue(deleteError) : vi.fn().mockResolvedValue({});
  return { listNamespacedCustomObject: list, deleteNamespacedCustomObject: del };
}

describe("readApiVersion", () => {
  it("defaults to v1beta1 when undefined or empty", () => {
    expect(readApiVersion(undefined)).toBe("v1beta1");
    expect(readApiVersion("")).toBe("v1beta1");
  });
  it("accepts supported versions", () => {
    expect(readApiVersion("v1alpha1")).toBe("v1alpha1");
    expect(readApiVersion("v1beta1")).toBe("v1beta1");
  });
  it("throws on unsupported version strings", () => {
    expect(() => readApiVersion("v2")).toThrow(/Expected DISPATCH_SANDBOX_CLAIM_API_VERSION/);
    expect(() => readApiVersion("latest")).toThrow(/Expected DISPATCH_SANDBOX_CLAIM_API_VERSION/);
  });
});

describe("isNotFound", () => {
  it("identifies 404-shaped errors only", () => {
    expect(isNotFound(null)).toBe(false);
    expect(isNotFound("404")).toBe(false);
    expect(isNotFound(404)).toBe(false);
    expect(isNotFound({ code: 404 })).toBe(true);
    expect(isNotFound({ response: { statusCode: 404 } })).toBe(true);
    expect(isNotFound({ response: { status: 404 } })).toBe(true);
    expect(isNotFound({ code: 500 })).toBe(false);
    expect(isNotFound({ response: { statusCode: 403 } })).toBe(false);
  });
});

describe("reconcileOnce", () => {
  it("rejects invalid grace periods before listing or deleting claims", async () => {
    const api = fakeApi([claim("still-running", msFromNow(30 * 60_000))]);
    await expect(reconcileOnce(api, { ...BASE_OPTS, graceMinutes: -30 })).rejects.toThrow(
      /Expected graceMinutes to be a nonnegative safe integer/,
    );
    expect(api.listNamespacedCustomObject).not.toHaveBeenCalled();
    expect(api.deleteNamespacedCustomObject).not.toHaveBeenCalled();
  });

  it.each([1.5, Number.POSITIVE_INFINITY, Number.NaN, Number.MAX_SAFE_INTEGER + 1])(
    "rejects invalid graceMinutes %p before listing claims",
    async (graceMinutes) => {
      const api = fakeApi([]);
      await expect(reconcileOnce(api, { ...BASE_OPTS, graceMinutes })).rejects.toThrow(
        /Expected graceMinutes to be a nonnegative safe integer/,
      );
      expect(api.listNamespacedCustomObject).not.toHaveBeenCalled();
    },
  );

  it("handles no claims or missing items", async () => {
    const api = fakeApi([]);
    await expect(reconcileOnce(api, BASE_OPTS)).resolves.toEqual({ deleted: 0, errors: 0, total: 0 });
    expect(api.deleteNamespacedCustomObject).not.toHaveBeenCalled();
    const list = vi.fn().mockResolvedValue({});
    await expect(reconcileOnce({ listNamespacedCustomObject: list, deleteNamespacedCustomObject: vi.fn() }, BASE_OPTS)).resolves.toEqual({ deleted: 0, errors: 0, total: 0 });
  });

  it("skips claims without, after, or within grace-period shutdown times", async () => {
    for (const item of [claim("none"), claim("active", msAgo(60_000)), claim("future", msFromNow(60_000)), claim("bad", "invalid")]) {
      const api = fakeApi([item]);
      await expect(reconcileOnce(api, BASE_OPTS)).resolves.toMatchObject({ deleted: 0 });
      expect(api.deleteNamespacedCustomObject).not.toHaveBeenCalled();
    }
  });

  it("deletes claims past their grace period with foreground propagation", async () => {
    const api = fakeApi([claim("expired", msAgo(31 * 60_000))]);
    await expect(reconcileOnce(api, BASE_OPTS)).resolves.toEqual({ deleted: 1, errors: 0, total: 1 });
    expect(api.deleteNamespacedCustomObject).toHaveBeenCalledWith({
      group: "extensions.agents.x-k8s.io", version: API_VERSION, namespace: NAMESPACE,
      plural: "sandboxclaims", name: "expired", propagationPolicy: "Foreground",
    });
  });

  it("records delete errors except not-found responses", async () => {
    const gone = fakeApi([claim("gone", msAgo(31 * 60_000))], { code: 404 });
    await expect(reconcileOnce(gone, BASE_OPTS)).resolves.toMatchObject({ deleted: 0, errors: 0 });
    const failed = fakeApi([claim("bad", msAgo(31 * 60_000))], new Error("server error"));
    await expect(reconcileOnce(failed, BASE_OPTS)).resolves.toMatchObject({ deleted: 0, errors: 1 });
  });

  it("respects configured grace minutes", async () => {
    const shutdown = msAgo(5 * 60_000);
    await expect(reconcileOnce(fakeApi([claim("marginal", shutdown)]), { ...BASE_OPTS, graceMinutes: 3 })).resolves.toMatchObject({ deleted: 1 });
    await expect(reconcileOnce(fakeApi([claim("marginal", shutdown)]), { ...BASE_OPTS, graceMinutes: 10 })).resolves.toMatchObject({ deleted: 0 });
  });

  it("uses the configured API version and namespace when listing", async () => {
    const api = fakeApi([]);
    await reconcileOnce(api, { ...BASE_OPTS, apiVersion: "v1beta1", namespace: "custom-ns" });
    expect(api.listNamespacedCustomObject).toHaveBeenCalledWith({
      group: "extensions.agents.x-k8s.io", version: "v1beta1", namespace: "custom-ns",
      plural: "sandboxclaims", labelSelector: "app.kubernetes.io/managed-by=dispatch",
    });
  });

  it("propagates listing errors", async () => {
    await expect(reconcileOnce({ listNamespacedCustomObject: vi.fn().mockRejectedValue(new Error("kube unreachable")), deleteNamespacedCustomObject: vi.fn() }, BASE_OPTS)).rejects.toThrow("kube unreachable");
  });
});
