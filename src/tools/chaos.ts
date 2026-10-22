import { execSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { loadConfig, log, shardIds } from '../config';
import { Db, makePool } from '../db';

type Counts = { succeeded: number; dead: number; cancelled: number; active: number };

const API = process.env.API_URL ?? 'http://127.0.0.1:3000';

function sh(cmd: string): string {
  return execSync(cmd, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

async function sleep(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

async function submit(prefix: string, count: number, payload: Record<string, unknown>, maxAttempts = 3): Promise<void> {
  const size = 100;
  for (let i = 0; i < count; i += size) {
    const jobs = [] as Array<Record<string, unknown>>;
    for (let j = i; j < Math.min(count, i + size); j++) {
      jobs.push({
        id: `${prefix}-${j}`,
        tenantId: 'chaos',
        name: 'chaos',
        scheduleType: 'once',
        maxAttempts,
        payload,
      });
    }
    const res = await fetch(`${API}/jobs/batch`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jobs }),
    });
    if (!res.ok) throw new Error(`submit failed ${res.status}: ${(await res.text()).slice(0, 300)}`);
  }
}

async function counts(dbs: Map<string, Db>, prefix: string): Promise<Counts> {
  const out: Counts = { succeeded: 0, dead: 0, cancelled: 0, active: 0 };
  for (const db of dbs.values()) {
    for (const table of ['run_history', 'runs']) {
      const statuses = table === 'run_history' ? "('succeeded','dead','cancelled')" : "('queued','running')";
      try {
        const rows = await db.pool.query<{ status: string; n: number }>(
          `SELECT status, count(*)::int AS n FROM ${table} WHERE job_id LIKE $1 AND status IN ${statuses} GROUP BY status`,
          [`${prefix}-%`],
        );
        for (const r of rows.rows) {
          if (r.status === 'succeeded' || r.status === 'dead' || r.status === 'cancelled') out[r.status] += r.n;
          else out.active += r.n;
        }
      } catch {
        // A restarting shard can briefly refuse connections; report what is observable.
      }
    }
  }
  return out;
}

async function waitComplete(dbs: Map<string, Db>, prefix: string, expected: number, timeoutMs: number): Promise<{ elapsedMs: number; final: Counts }> {
  const start = Date.now();
  let last: Counts = { succeeded: 0, dead: 0, cancelled: 0, active: 0 };
  while (Date.now() - start < timeoutMs) {
    try {
      last = await counts(dbs, prefix);
    } catch {
      /* retry */
    }
    if (last.succeeded === expected) return { elapsedMs: Date.now() - start, final: last };
    await sleep(500);
  }
  return { elapsedMs: Date.now() - start, final: last };
}

async function waitUntil(check: () => Promise<boolean>, timeoutMs: number): Promise<number> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return Date.now() - start;
    await sleep(200);
  }
  return -1;
}

async function main(): Promise<void> {
  const smoke = process.argv.includes('--smoke');
  process.env.CONTROL_DATABASE_URL ??= 'postgres://jobsched:jobsched@127.0.0.1:55432/jobsched_control';
  process.env.SHARD_DSN_MAP ??= JSON.stringify({
    'shard-a': 'postgres://jobsched:jobsched@127.0.0.1:55433/jobsched_shard',
    'shard-b': 'postgres://jobsched:jobsched@127.0.0.1:55434/jobsched_shard',
  });
  process.env.SHARD_IDS ??= 'shard-a,shard-b';
  const cfg = loadConfig();
  const control = makePool('control', cfg.CONTROL_DATABASE_URL, 4);
  const dbs = new Map<string, Db>();
  for (const id of shardIds(cfg)) dbs.set(id, makePool(id, cfg.shardUrls[id]!, 4));
  const results: Record<string, unknown> = { timestamp: new Date().toISOString(), mode: smoke ? 'smoke' : 'full' };
  const stamp = Date.now().toString(36);

  try {
    // 1. Worker crash during active runs.
    {
      const prefix = `chaos-worker-${stamp}`;
      const expected = smoke ? 30 : 300;
      await submit(prefix, expected, { sleepMs: 4000 });
      const running = async () =>
        (await shardIds(cfg).reduce(async (accP, id) => {
          const acc = await accP;
          const r = await dbs.get(id)!.pool.query<{ n: number }>(
            "SELECT count(*)::int AS n FROM runs WHERE worker_id='worker-1' AND status='running'",
          );
          return acc + (r.rows[0]?.n ?? 0);
        }, Promise.resolve(0))) as number;
      await waitUntil(async () => (await running()) > 0, 5000);
      sh('docker update --restart=no jobsched-worker-1');
      sh('docker kill jobsched-worker-1');
      const recoveryMs = await waitUntil(async () => (await running()) === 0, 30000);
      const completion = await waitComplete(dbs, prefix, expected, 120000);
      sh('docker update --restart=unless-stopped jobsched-worker-1');
      sh('docker start jobsched-worker-1');
      results.workerCrash = {
        expected,
        killToAllLeasesReleasedMs: recoveryMs,
        leaseTtlMs: cfg.WORKER_LEASE_TTL_MS,
        targetMs: cfg.WORKER_LEASE_TTL_MS + 2000,
        completion,
      };
    }

    if (!smoke) {
      await sleep(3000);

      // 2. Scheduler crash: partitions must be recovered by the surviving scheduler.
    {
      const owned = async () => {
        const r = await control.pool.query<{ n: number }>(
          "SELECT count(*)::int AS n FROM partition_leases WHERE owner_id='scheduler-1'",
        );
        return r.rows[0]?.n ?? 0;
      };
      const unowned = async () => {
        const r = await control.pool.query<{ n: number }>("SELECT count(*)::int AS n FROM partition_leases WHERE lease_expires_at < now()");
        return r.rows[0]?.n ?? 0;
      };
      await waitUntil(async () => (await owned()) > 0, 10000);
      sh('docker update --restart=no jobsched-scheduler-1');
      sh('docker kill jobsched-scheduler-1');
      const recoveryMs = await waitUntil(async () => (await owned()) === 0 && (await unowned()) === 0, 30000);
      const prefix = `chaos-sched-${stamp}`;
      await submit(prefix, 50, {});
      const completion = await waitComplete(dbs, prefix, 50, 60000);
      sh('docker update --restart=unless-stopped jobsched-scheduler-1');
      sh('docker start jobsched-scheduler-1');
      results.schedulerCrash = {
        recoveryMs,
        targetMs: 15000,
        completion,
      };
    }

    await sleep(3000);

    // 3. Shard restart while runs are in flight.
    {
      const prefix = `chaos-shard-${stamp}`;
      const expected = 120;
      await submit(prefix, expected, { sleepMs: 3000 });
      await waitUntil(async () => (await counts(dbs, prefix)).active > 10, 5000);
      const restartAt = Date.now();
      sh('docker restart jobsched-shard-a');
      const completion = await waitComplete(dbs, prefix, expected, 180000);
      let health = '';
      const healthMs = await waitUntil(async () => {
        health = sh('docker inspect --format "{{.State.Health.Status}}" jobsched-shard-a');
        return health === 'healthy';
      }, 30000);
      results.shardRestart = {
        restartWallMs: Date.now() - restartAt,
        shardHealthyAfterMs: healthMs,
        shardHealth: health,
        completion,
      };
    }

    // 4. Slow-database worker: 150 ms injected on every DB operation. Stop normal workers so
    // only the latency-injected worker can claim, proving it actually executed the jobs.
    {
      sh('docker stop -t 30 jobsched-worker-1 jobsched-worker-2 jobsched-worker-3 jobsched-worker-4');
      const name = `jobsched-latency-${stamp}`;
      const network = 'jobsched_jobsched';
      const envFile = path.join(process.cwd(), 'benchmarks', `latency-${stamp}.env`);
      fs.mkdirSync(path.dirname(envFile), { recursive: true });
      const containerShardMap = JSON.stringify({
        'shard-a': 'postgres://jobsched:jobsched@shard-a:5432/jobsched_shard',
        'shard-b': 'postgres://jobsched:jobsched@shard-b:5432/jobsched_shard',
      });
      fs.writeFileSync(
        envFile,
        [
          'CONTROL_DATABASE_URL=postgres://jobsched:jobsched@control-db:5432/jobsched_control',
          `SHARD_DSN_MAP=${containerShardMap}`,
          'SHARD_IDS=shard-a,shard-b',
          'WORKER_ID=latency-worker',
          'OPS_PORT=9102',
          'DB_LATENCY_MS=150',
        ].join('\n'),
      );
      sh(
        `docker run -d --name ${name} --network ${network} --memory 384m --env-file "${envFile}" ` +
          `jobsched-app:local node dist/services/worker-main.js`,
      );
      try {
        await sleep(3000);
        const running = sh(`docker inspect --format "{{.State.Running}}" ${name}`);
        if (running !== 'true') throw new Error(`latency worker failed to start: ${sh(`docker logs ${name} 2>&1`).slice(0, 500)}`);
        const prefix = `chaos-latency-${stamp}`;
        const expected = 50;
        await submit(prefix, expected, {});
        const completion = await waitComplete(dbs, prefix, expected, 120000);
        let processedByLatencyWorker = 0;
        for (const db of dbs.values()) {
          const r = await db.pool.query<{ n: number }>(
            "SELECT count(*)::int AS n FROM run_history WHERE job_id LIKE $1 AND worker_id='latency-worker'",
            [`${prefix}-%`],
          );
          processedByLatencyWorker += r.rows[0]?.n ?? 0;
        }
        results.latencyInjection = { dbLatencyMs: 150, expected, processedByLatencyWorker, completion };
      } finally {
        try {
          sh(`docker rm -f ${name}`);
        } catch {
          /* already gone */
        }
        try {
          fs.unlinkSync(envFile);
        } catch {
          /* ignore */
        }
        sh('docker start jobsched-worker-1 jobsched-worker-2 jobsched-worker-3 jobsched-worker-4');
      }
    }
    }

    const artifactDir = path.join(process.cwd(), 'benchmarks');
    fs.mkdirSync(artifactDir, { recursive: true });
    const file = path.join(artifactDir, `chaos-${stamp}.json`);
    fs.writeFileSync(file, JSON.stringify(results, null, 2));
    console.log(JSON.stringify({ results, artifact: file }, null, 2));
  } finally {
    await Promise.all([control, ...dbs.values()].map((d) => d.pool.end()));
  }
}

main().catch((err) => {
  log('error', 'chaos run failed', { error: String(err), stack: (err as Error).stack });
  process.exit(1);
});

