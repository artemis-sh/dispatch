/**
 * Unit tests for src/util.ts — readBoolean, readNumber.
 */

import { describe, expect, it } from "vitest";
import { readBoolean, readNonnegativeInteger, readNumber } from "../../src/util.js";

// ---------------------------------------------------------------------------
// readBoolean
// ---------------------------------------------------------------------------

describe("readBoolean", () => {
  it("returns the fallback when value is undefined", () => {
    expect(readBoolean(undefined, true)).toBe(true);
    expect(readBoolean(undefined, false)).toBe(false);
  });

  it("returns the fallback when value is an empty string", () => {
    expect(readBoolean("", true)).toBe(true);
    expect(readBoolean("", false)).toBe(false);
  });

  it("returns true for the canonical truthy strings", () => {
    expect(readBoolean("1", false)).toBe(true);
    expect(readBoolean("true", false)).toBe(true);
    expect(readBoolean("yes", false)).toBe(true);
    expect(readBoolean("on", false)).toBe(true);
  });

  it("is case-insensitive for truthy strings", () => {
    expect(readBoolean("TRUE", false)).toBe(true);
    expect(readBoolean("True", false)).toBe(true);
    expect(readBoolean("YES", false)).toBe(true);
    expect(readBoolean("ON", false)).toBe(true);
  });

  it("returns false for any other non-empty string", () => {
    expect(readBoolean("false", true)).toBe(false);
    expect(readBoolean("0", true)).toBe(false);
    expect(readBoolean("no", true)).toBe(false);
    expect(readBoolean("off", true)).toBe(false);
    expect(readBoolean("disabled", true)).toBe(false);
    expect(readBoolean("2", true)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// readNumber
// ---------------------------------------------------------------------------

describe("readNumber", () => {
  it("returns the fallback when value is undefined", () => {
    expect(readNumber(undefined, 42)).toBe(42);
    expect(readNumber(undefined, 0)).toBe(0);
  });

  it("returns the fallback when value is an empty string", () => {
    expect(readNumber("", 10)).toBe(10);
  });

  it("parses a valid integer", () => {
    expect(readNumber("30", 0)).toBe(30);
    expect(readNumber("0", 99)).toBe(0);
    expect(readNumber("-5", 0)).toBe(-5);
  });

  it("parses a valid floating-point number", () => {
    expect(readNumber("2.5", 0)).toBe(2.5);
    expect(readNumber("0.001", 0)).toBe(0.001);
  });

  it("throws when the value is not parseable as a number", () => {
    expect(() => readNumber("abc", 0)).toThrow(/Expected numeric env value/);
    expect(() => readNumber("1e999abc", 0)).toThrow(/Expected numeric env value/);
  });

  it("throws for the string 'Infinity'", () => {
    expect(() => readNumber("Infinity", 0)).toThrow(/Expected numeric env value/);
  });

  it("throws for the string 'NaN'", () => {
    expect(() => readNumber("NaN", 0)).toThrow(/Expected numeric env value/);
  });
});

// ---------------------------------------------------------------------------
// readNonnegativeInteger
// ---------------------------------------------------------------------------

describe("readNonnegativeInteger", () => {
  it("returns the fallback when value is unset", () => {
    expect(readNonnegativeInteger(undefined, 30)).toBe(30);
    expect(readNonnegativeInteger("", 30)).toBe(30);
  });

  it("accepts zero and positive safe integers", () => {
    expect(readNonnegativeInteger("0", 30)).toBe(0);
    expect(readNonnegativeInteger("30", 0)).toBe(30);
  });

  it("rejects negative, fractional, non-finite, and unsafe values", () => {
    for (const value of ["-1", "1.5", "Infinity", "NaN", "9007199254740992"]) {
      expect(() => readNonnegativeInteger(value, 30)).toThrow(/Expected nonnegative integer env value/);
    }
  });
});
