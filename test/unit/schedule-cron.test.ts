import { describe, expect, it } from "vitest";
import { cronExpressionSchema, nextCronOccurrence } from "../../src/schedule/cron.js";

describe("schedule cron", () => {
  it("calculates UTC occurrences for standard five-field expressions", () => {
    expect(nextCronOccurrence("17 * * * *", new Date("2026-07-20T17:17:00Z")).toISOString()).toBe("2026-07-20T18:17:00.000Z");
    expect(nextCronOccurrence("*/15 9-10 * * 1-5", new Date("2026-07-17T10:59:00Z")).toISOString()).toBe("2026-07-20T09:00:00.000Z");
    expect(nextCronOccurrence("0 0 1 1 *", new Date("2026-01-01T00:00:00Z")).toISOString()).toBe("2027-01-01T00:00:00.000Z");
    expect(nextCronOccurrence("0 0 1 * 1", new Date("2026-07-20T00:00:00Z")).toISOString()).toBe("2026-07-27T00:00:00.000Z");
  });

  it("accepts 7 as the standard Sunday alias", () => {
    expect(nextCronOccurrence("0 9 * * 7", new Date("2026-08-21T12:00:00.000Z")))
      .toEqual(new Date("2026-08-23T09:00:00.000Z"));
  });

  it("treats full-range day fields as wildcards in day matching", () => {
    const after = new Date("2026-07-21T00:00:00Z");
    expect(nextCronOccurrence("0 0 */1 * 1", after).toISOString()).toBe("2026-07-27T00:00:00.000Z");
    expect(nextCronOccurrence("0 0 1-31 * 1", after).toISOString()).toBe("2026-07-27T00:00:00.000Z");
    expect(nextCronOccurrence("0 0 1 * */1", after).toISOString()).toBe("2026-08-01T00:00:00.000Z");
    expect(nextCronOccurrence("0 0 1 * 0-6", after).toISOString()).toBe("2026-08-01T00:00:00.000Z");
  });

  it.each(["* * *", "60 * * * *", "*/0 * * * *", "* 24 * * *", "* * 0 * *", "* * * * 8", "a * * * *"])(
    "rejects invalid expression %s", (expression) => expect(cronExpressionSchema.safeParse(expression).success).toBe(false),
  );

  it("rejects expressions with no calendar occurrence", () => {
    expect(cronExpressionSchema.safeParse("0 0 31 2 *").success).toBe(false);
  });
});
