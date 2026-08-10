import { describe, expect, it, vi } from "vitest";

vi.mock("../../src/sandbox/client.js", () => ({
  createKubeConfig: vi.fn(),
  createCustomObjectsApi: vi.fn().mockReturnValue({
    listNamespacedCustomObject: vi.fn().mockResolvedValue({ items: [] }),
    deleteNamespacedCustomObject: vi.fn().mockResolvedValue({}),
  }),
}));

import { reconcileOnce } from "../../src/reconcile.js";
import { readNonnegativeSafeInteger } from "../../src/util.js";

describe("readNonnegativeSafeInteger", () => {
  it("uses the fallback for an unset value", () => {
    expect(readNonnegativeSafeInteger(undefined, 30)).toBe(30);
    expect(readNonnegativeSafeInteger("", 30)).toBe(30);
  });

  it("accepts nonnegative safe integers", () => {
    expect(readNonnegativeSafeInteger("0", 30)).toBe(0);
    expect(readNonnegativeSafeInteger("30", 0)).toBe(30);
  });

  it("rejects negative, fractional, and unsafe values", () => {
    for (const value of ["-30", "1.5", "9007199254740992"]) {
      expect(() => readNonnegativeSafeInteger(value, 30)).toThrow(
        /Expected nonnegative integer env value/,
      );
    }
  });
});

describe("reconcileOnce grace validation", () => {
  it("rejects an invalid grace period before listing or deleting claims", async () => {
    const list = vi.fn().mockResolvedValue({ items: [] });
    const remove = vi.fn().mockResolvedValue({});
    const api = {
      listNamespacedCustomObject: list,
      deleteNamespacedCustomObject: remove,
    };

    await expect(
      reconcileOnce(api as never, {
        namespace: "agents",
        apiVersion: "v1beta1",
        graceMinutes: -30,
        now: Date.parse("2026-08-02T12:00:00.000Z"),
      }),
    ).rejects.toThrow(/Expected graceMinutes to be a nonnegative safe integer/);

    expect(list).not.toHaveBeenCalled();
    expect(remove).not.toHaveBeenCalled();
  });
});
