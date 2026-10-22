/** Exponential backoff with full jitter. `attempt` is the number of attempts already made. */
export function retryDelayMs(attempt: number, baseMs = 500, capMs = 30000, random = Math.random): number {
  const exp = Math.min(capMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.floor(random() * exp);
}

/** SQL expression equivalent used by the lease reaper (full jitter, 500ms base, 30s cap). */
export const RETRY_SQL =
  "(now() + (LEAST(30000, 500 * power(2, GREATEST(attempt - 1, 0))) * random()) * interval '1 millisecond')";
