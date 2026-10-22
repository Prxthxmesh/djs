import { CronExpressionParser } from 'cron-parser';

export type MisfirePolicy = 'skip' | 'run_once';

export function validateCron(expression: string, timezone: string): void {
  try {
    CronExpressionParser.parse(expression, { tz: timezone, currentDate: new Date() });
  } catch (err) {
    throw new Error(`Invalid cron expression or timezone: ${String(err)}`);
  }
}

export function nextCronOccurrence(expression: string, timezone: string, after: Date): Date {
  const interval = CronExpressionParser.parse(expression, { tz: timezone, currentDate: after });
  return interval.next().toDate();
}

export type ScheduleDecision = {
  /** The occurrence timestamp to trigger, if any. */
  triggerAt: Date | null;
  /** Value to persist as next_run_at. */
  nextRunAt: Date;
  missed: boolean;
  skipped: number;
};

/**
 * Decide what to do for a cron job whose persisted next_run_at is `slot`.
 * On-time or slightly late occurrences run. Beyond the misfire grace window:
 * - run_once triggers exactly one catch-up for the missed slot, then jumps to the future.
 * - skip triggers nothing and jumps to the next future occurrence.
 * All calculations are calendar-aware in the job timezone (DST safe).
 */
export function decideCron(
  expression: string,
  timezone: string,
  slot: Date,
  now: Date,
  policy: MisfirePolicy,
  graceMs: number,
): ScheduleDecision {
  const late = now.getTime() - slot.getTime();
  if (late <= Math.max(0, graceMs)) {
    return {
      triggerAt: slot,
      nextRunAt: nextCronOccurrence(expression, timezone, slot),
      missed: false,
      skipped: 0,
    };
  }
  let next = slot;
  let skipped = 0;
  while (next.getTime() <= now.getTime()) {
    next = nextCronOccurrence(expression, timezone, next);
    skipped++;
    if (skipped > 100000) throw new Error('Cron catch-up limit exceeded');
  }
  if (policy === 'run_once') {
    return { triggerAt: slot, nextRunAt: next, missed: true, skipped: skipped - 1 };
  }
  return { triggerAt: null, nextRunAt: next, missed: true, skipped };
}
