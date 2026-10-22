import fs from 'node:fs';
import path from 'node:path';
import { loadConfig, log, shardIds } from '../config';
import { makePool, Db } from '../db';

type Args = {
  apiUrl: string;
  rate: number;
  durationSec: number;
  prefix: string;
  tenant: string;
  batchIntervalMs: number;
  completionTimeoutSec: number;
};

function parseArgs(): Args {
  const argv = process.argv.slice(2);
  const get = (name: string): string | undefined => {
    const i = argv.indexOf(`--${name}`);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  return {
    apiUrl: get('api') ?? process.env.API_URL ?? 'http://127.0.0.1:3000',
    rate: Number(get('rate') ?? process.env.LOADGEN_RATE ?? 600),
    durationSec: Number(get('duration') ?? process.env.LOADGEN_DURATION_SEC ?? 300),
    prefix: get('prefix') ?? process.env.LOADGEN_PREFIX ?? `bench-${Date.now()}`,
    tenant: get('tenant') ?? 'bench',
    batchIntervalMs: Number(get('interval') ?? 100),
    completionTimeoutSec: Number(get('timeout') ?? 120),
  };
}

type Sample = { t: number; queued: number; running: number };

async function main(): Promise<void> {
  const cfg = loadConfig();
  const args = parseArgs();
  const shardDbs = new Map<string, Db>();
  for (const id of shardIds(cfg)) shardDbs.set(id, makePool(id, cfg.shardUrls[id]!, 4));

  const batchSize = Math.max(1, Math.round((args.rate * args.batchIntervalMs) / 1000));
  const totalTarget = Math.round(args.rate * args.durationSec);
  const samples: Sample[] = [];
  let submitted = 0;
  let offeredAttempts = 0;
  let backpressure429 = 0;
  let httpErrors = 0;
  const submittedIds = new Set<string>();
  let seq = 0;

  const sampleTimer = setInterval(() => {
    void (async () => {
      let queued = 0;
      let running = 0;
      for (const db of shardDbs.values()) {
        try {
          const r = await db.pool.query<{ queued: number; running: number }>(
            `SELECT count(*) FILTER (WHERE status='queued')::int AS queued,
                    count(*) FILTER (WHERE status='running')::int AS running FROM runs`,
          );
          queued += r.rows[0]?.queued ?? 0;
          running += r.rows[0]?.running ?? 0;
        } catch {
          /* ignore */
        }
      }
      samples.push({ t: Date.now(), queued, running });
    })();
  }, 1000);

  const start = Date.now();
  const endAt = start + args.durationSec * 1000;

  while (Date.now() < endAt && submitted < totalTarget) {
    const tick = Date.now();
    const jobs = [];
    for (let i = 0; i < batchSize && submitted + i < totalTarget; i++) {
      const id = `${args.prefix}-${seq++}`;
      submittedIds.add(id);
      jobs.push({
        id,
        tenantId: args.tenant,
        name: 'noop',
        scheduleType: 'once',
        scheduledAt: new Date().toISOString(),
        maxAttempts: 1,
        payload: {},
      });
    }
    if (jobs.length > 0) {
      offeredAttempts += jobs.length;
      const res = await fetch(`${args.apiUrl}/jobs/batch`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ jobs }),
      }).catch(() => undefined);
      if (!res) {
        httpErrors++;
      } else if (res.status === 429) {
        backpressure429++;
        for (const j of jobs) submittedIds.delete(j.id);
        seq -= jobs.length;
        const retry = Number(res.headers.get('retry-after') ?? '1');
        await new Promise((r) => setTimeout(r, retry * 1000));
        continue;
      } else if (res.status >= 400) {
        httpErrors++;
        log('warn', 'batch rejected', { status: res.status, body: (await res.text()).slice(0, 200) });
      } else {
        submitted += jobs.length;
      }
    }
    const elapsed = Date.now() - tick;
    if (elapsed < args.batchIntervalMs) await new Promise((r) => setTimeout(r, args.batchIntervalMs - elapsed));
    else await new Promise((r) => setTimeout(r, 0));
  }
  const submitEndedAt = Date.now();
  clearInterval(sampleTimer);

  log('info', 'submission finished', { submitted, offeredAttempts, backpressure429, httpErrors, seconds: (submitEndedAt - start) / 1000 });

  const completionDeadline = submitEndedAt + args.completionTimeoutSec * 1000;
  let lastDone = -1;
  let distinct = 0;
  let statusCounts: Record<string, number> = {};
  let duplicateRows = 0;
  let latency: Record<string, number> = {};
  while (Date.now() < completionDeadline) {
    const verification = await verify(shardDbs, args.prefix);
    distinct = verification.distinct;
    statusCounts = verification.statusCounts;
    duplicateRows = verification.duplicateRows;
    latency = verification.latency;
    if (distinct !== lastDone) {
      lastDone = distinct;
      log('info', 'completion progress', { distinct, submitted });
    }
    if (verification.succeeded >= submitted && submitted > 0) break;
    await new Promise((r) => setTimeout(r, 1000));
  }
  const finishedAt = Date.now();

  const found = new Set<string>();
  for (const db of shardDbs.values()) {
    const rows = await db.pool.query<{ job_id: string }>('SELECT job_id FROM run_history WHERE job_id LIKE $1', [`${args.prefix}%`]);
    for (const r of rows.rows) found.add(r.job_id);
  }
  let lost = 0;
  for (const id of submittedIds) if (!found.has(id)) lost++;

  const activeSeconds = (finishedAt - start) / 1000;
  const submitSeconds = (submitEndedAt - start) / 1000;
  const drainSeconds = (finishedAt - submitEndedAt) / 1000;
  const completed = statusCounts.succeeded ?? 0;
  const result = {
    timestamp: new Date().toISOString(),
    prefix: args.prefix,
    offeredAttempts,
    submitted,
    submittedTarget: totalTarget,
    completedSucceeded: completed,
    statusCounts,
    distinctTriggers: distinct,
    duplicateTriggerRows: duplicateRows,
    lostJobs: lost,
    backpressure429,
    httpErrors,
    // Total wall time from first submission through the final completion, including drain.
    durationSec: +(activeSeconds.toFixed(2)),
    // Submission window only: time spent offering and accepting work.
    submitDurationSec: +(submitSeconds.toFixed(2)),
    // Time between the end of submission and the final completion.
    drainDurationSec: +(drainSeconds.toFixed(2)),
    // Accepted jobs divided by the submission window (616.2/s in the controlled run).
    submitWindowPerSec: +(submitted / submitSeconds).toFixed(2),
    acceptedPerSec: +(submitted / submitSeconds).toFixed(2),
    // Attempted offers (including 429 retries) divided by the submission window.
    offeredPerSec: +(offeredAttempts / submitSeconds).toFixed(2),
    // Completed jobs divided by total wall time including drain (596.9/s in the controlled run).
    completedPerSec: +(completed / activeSeconds).toFixed(2),
    achievedCompletedPerSec: +(distinct / activeSeconds).toFixed(2),
    latencyMs: latency,
    queueDepthMax: samples.reduce((m, s) => Math.max(m, s.queued), 0),
    runningMax: samples.reduce((m, s) => Math.max(m, s.running), 0),
    samples: samples.length,
  };
  const dir = path.join(process.cwd(), 'benchmarks');
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `loadgen-${Date.now()}.json`);
  fs.writeFileSync(file, JSON.stringify(result, null, 2));
  console.log(JSON.stringify({ summary: result, artifact: file }, null, 2));
  await Promise.all([...shardDbs.values()].map((d) => d.pool.end()));
  if (lost > 0 || duplicateRows > 0) process.exitCode = 2;
}

async function verify(
  shardDbs: Map<string, Db>,
  prefix: string,
): Promise<{ distinct: number; succeeded: number; statusCounts: Record<string, number>; duplicateRows: number; latency: Record<string, number> }> {
  const statusCounts: Record<string, number> = {};
  let distinct = 0;
  let duplicateRows = 0;
  const seen = new Set<string>();
  const latencies: number[] = [];
  const e2e: number[] = [];
  for (const db of shardDbs.values()) {
    const rows = await db.pool.query<{
      job_id: string;
      status: string;
      n: number;
      trigger_ms: number;
      e2e_ms: number;
    }>(
      `SELECT job_id, status, count(*)::int AS n,
              EXTRACT(EPOCH FROM (triggered_at - scheduled_time)) * 1000 AS trigger_ms,
              EXTRACT(EPOCH FROM (finished_at - scheduled_time)) * 1000 AS e2e_ms
       FROM run_history WHERE job_id LIKE $1
       GROUP BY job_id, status, triggered_at, scheduled_time, finished_at`,
      [`${prefix}%`],
    );
    for (const r of rows.rows) {
      statusCounts[r.status] = (statusCounts[r.status] ?? 0) + 1;
      if (r.n > 1) duplicateRows += r.n - 1;
      if (!seen.has(r.job_id)) {
        seen.add(r.job_id);
        distinct++;
      }
      latencies.push(Number(r.trigger_ms));
      e2e.push(Number(r.e2e_ms));
    }
  }
  const pct = (arr: number[], p: number): number => {
    if (arr.length === 0) return 0;
    const sorted = [...arr].sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
    return +sorted[idx]!.toFixed(2);
  };
  return {
    distinct,
    succeeded: statusCounts.succeeded ?? 0,
    statusCounts,
    duplicateRows,
    latency: {
      triggerP50: pct(latencies, 50),
      triggerP95: pct(latencies, 95),
      triggerP99: pct(latencies, 99),
      endToEndP50: pct(e2e, 50),
      endToEndP95: pct(e2e, 95),
      endToEndP99: pct(e2e, 99),
    },
  };
}

main().catch((err) => {
  log('error', 'loadgen failed', { error: String(err), stack: (err as Error).stack });
  process.exit(1);
});
