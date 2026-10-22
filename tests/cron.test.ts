import { describe, expect, it } from 'vitest';
import { decideCron, nextCronOccurrence, validateCron } from '../src/domain/cron';

describe('timezone-aware cron and DST', () => {
  it('shifts a nonexistent spring-forward local time forward exactly once', () => {
    const from = new Date('2024-03-09T07:30:00.000Z');
    const next = nextCronOccurrence('30 2 * * *', 'America/New_York', from);
    expect(next.toISOString()).toBe('2024-03-10T07:30:00.000Z');
    const after = nextCronOccurrence('30 2 * * *', 'America/New_York', next);
    expect(after.toISOString()).toBe('2024-03-11T06:30:00.000Z');
  });

  it('fires one occurrence for an ambiguous fall-back local time', () => {
    const first = nextCronOccurrence('30 1 * * *', 'America/New_York', new Date('2024-11-03T04:00:00.000Z'));
    const second = nextCronOccurrence('30 1 * * *', 'America/New_York', first);
    expect(first.toISOString()).toBe('2024-11-03T05:30:00.000Z');
    expect(second.toISOString()).toBe('2024-11-04T06:30:00.000Z');
    expect(second.getTime()).toBeGreaterThan(first.getTime());
  });

  it('keeps normal local wall time across a DST change', () => {
    const next = nextCronOccurrence('0 9 * * *', 'Europe/Berlin', new Date('2024-03-30T08:00:00.000Z'));
    expect(next.toISOString()).toBe('2024-03-31T07:00:00.000Z');
  });

  it('runs on time within the grace window', () => {
    const slot = new Date('2026-01-01T00:00:00.000Z');
    const decision = decideCron('0 * * * *', 'UTC', slot, new Date('2026-01-01T00:00:20.000Z'), 'skip', 30000);
    expect(decision.triggerAt?.toISOString()).toBe(slot.toISOString());
    expect(decision.missed).toBe(false);
  });

  it('skip policy drops missed occurrences and advances into the future', () => {
    const slot = new Date('2026-01-01T00:00:00.000Z');
    const now = new Date('2026-01-01T05:30:00.000Z');
    const decision = decideCron('0 * * * *', 'UTC', slot, now, 'skip', 30000);
    expect(decision.triggerAt).toBeNull();
    expect(decision.missed).toBe(true);
    expect(decision.nextRunAt.toISOString()).toBe('2026-01-01T06:00:00.000Z');
  });

  it('run_once policy performs exactly one catch-up then advances', () => {
    const slot = new Date('2026-01-01T00:00:00.000Z');
    const now = new Date('2026-01-01T05:30:00.000Z');
    const decision = decideCron('0 * * * *', 'UTC', slot, now, 'run_once', 30000);
    expect(decision.triggerAt?.toISOString()).toBe(slot.toISOString());
    expect(decision.nextRunAt.toISOString()).toBe('2026-01-01T06:00:00.000Z');
    expect(decision.skipped).toBe(5);
  });

  it('rejects invalid expressions and timezones', () => {
    expect(() => validateCron('not a cron', 'UTC')).toThrow();
    expect(() => validateCron('* * * * *', 'Mars/Olympus')).toThrow();
  });
});
