import { z } from "zod";

const FIELD_RANGES = [[0, 59], [0, 23], [1, 31], [1, 12], [0, 6]] as const;
const DAY_OF_WEEK_PARSE_MAXIMUM = 7;

export const cronExpressionSchema = z.string().min(9).max(128).superRefine((value, context) => {
  try {
    parseCronExpression(value);
  } catch (error) {
    context.addIssue({ code: "custom", message: String(error) });
  }
});

export function nextCronOccurrence(expression: string, after: Date): Date {
  const { fields, dayOfMonthWildcard, dayOfWeekWildcard } = parseCronExpression(expression);
  const candidate = new Date(after.getTime());
  candidate.setUTCSeconds(0, 0);
  candidate.setUTCMinutes(candidate.getUTCMinutes() + 1);
  const limit = 366 * 24 * 60 * 5;
  for (let checked = 0; checked < limit; checked += 1) {
    const dayOfMonthMatches = fields[2].has(candidate.getUTCDate());
    const dayOfWeekMatches = fields[4].has(candidate.getUTCDay());
    const dayMatches = dayOfMonthWildcard || dayOfWeekWildcard
      ? dayOfMonthMatches && dayOfWeekMatches
      : dayOfMonthMatches || dayOfWeekMatches;
    if (fields[0].has(candidate.getUTCMinutes())
      && fields[1].has(candidate.getUTCHours())
      && dayMatches
      && fields[3].has(candidate.getUTCMonth() + 1)
    ) return candidate;
    candidate.setUTCMinutes(candidate.getUTCMinutes() + 1);
  }
  throw new Error("cron expression has no occurrence within five years");
}

function parseCronExpression(expression: string): {
  fields: [Set<number>, Set<number>, Set<number>, Set<number>, Set<number>];
  dayOfMonthWildcard: boolean;
  dayOfWeekWildcard: boolean;
} {
  const parts = expression.trim().split(/\s+/);
  if (parts.length !== 5) throw new Error("cron expression must contain five fields");
  const fields = parts.map((part, index) => {
    const range = FIELD_RANGES[index]!;
    const maximum = index === 4 ? DAY_OF_WEEK_PARSE_MAXIMUM : range[1];
    const values = parseField(part!, range[0], maximum);
    return index === 4
      ? new Set([...values].map((value) => value === 7 ? 0 : value))
      : values;
  }) as [Set<number>, Set<number>, Set<number>, Set<number>, Set<number>];
  return {
    fields,
    dayOfMonthWildcard: spansEntireRange(fields[2], ...FIELD_RANGES[2]),
    dayOfWeekWildcard: spansEntireRange(fields[4], ...FIELD_RANGES[4]),
  };
}

function spansEntireRange(values: Set<number>, minimum: number, maximum: number): boolean {
  if (values.size !== maximum - minimum + 1) return false;
  for (let value = minimum; value <= maximum; value += 1) {
    if (!values.has(value)) return false;
  }
  return true;
}

function parseField(field: string, minimum: number, maximum: number): Set<number> {
  const values = new Set<number>();
  for (const item of field.split(",")) {
    const [range, rawStep, ...extra] = item.split("/");
    if (!range || extra.length || (rawStep !== undefined && !/^\d+$/.test(rawStep))) throw new Error(`invalid cron field ${field}`);
    const step = rawStep === undefined ? 1 : Number(rawStep);
    if (step < 1 || step > maximum - minimum + 1) throw new Error(`invalid cron step in ${field}`);
    let start: number;
    let end: number;
    if (range === "*") [start, end] = [minimum, maximum];
    else if (/^\d+$/.test(range)) start = end = Number(range);
    else {
      const match = /^(\d+)-(\d+)$/.exec(range);
      if (!match) throw new Error(`invalid cron range in ${field}`);
      [start, end] = [Number(match[1]), Number(match[2])];
    }
    if (start < minimum || end > maximum || start > end) throw new Error(`cron value outside ${minimum}-${maximum}`);
    for (let value = start; value <= end; value += step) values.add(value);
  }
  if (!values.size) throw new Error(`empty cron field ${field}`);
  return values;
}
