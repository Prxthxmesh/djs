import { execFileSync, execSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { loadConfig, log, shardIds } from '../config';
import { Db, makePool } from '../db';

type Args = {
  apiUrl: string;
  rate: number;
  durationSec: number;
  prefix: string;
  intervalMs: number;
  timeoutSec: number;
  viaContainer: boolean;
};

type PgSample = Record<string, unknown>;

function parseArgs(): Args {
  const argv = process.argv.slice(2);
  const get = (name: string): string | undefined => {
    const i = argv.indexOf(`--${name}`);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  return {
    apiUrl: get('api') ?? process.env.API_URL ?? 'http://127.0.0.1:3000',
    rate: Number(get('rate') ?? 600),
    durationSec: Number(get('duration') ?? 300),
    prefix: get('prefix') ?? `tuned-${Date.now()}`,
    intervalMs: Number(get('interval') ?? 20000),
    timeoutSec: Number(get('timeout') ?? 300),
    viaContainer: argv.includes('--via-container') || process.env.BENCHMARK_VIA_CONTAINER === '1',
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function sh(cmd: string): string {
  return execSync(cmd, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

function dockerStats(): unknown {
  const out = sh(
    'docker stats --no-stream --format "{{json .}}" jobsched-api jobsched-control-db ' +
      'jobsched-shard-a jobsched-shard-b jobsched-worker-1 jobsched-worker-2 jobsched-worker-3 ' +
      'jobsched-worker-4 jobsched-scheduler-1 jobsched-scheduler-2',
  );
  return out
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return { raw: line };
      }
    });
}

function dockerLimits(): unknown {
  try {
    const raw = sh(
      'docker inspect jobsched-api jobsched-control-db jobsched-shard-a jobsched-shard-b jobsched-worker-1 jobsched-worker-2 jobsched-worker-3 jobsched-worker-4 jobsched-scheduler-1 jobsched-scheduler-2',
    );
    return JSON.parse(raw).map((c: { Name: string; HostConfig: Record<string, unknown> }) => ({
      name: c.Name.replace(/^\//, ''),
      memoryLimitBytes: c.HostConfig.Memory,
      nanoCpus: c.HostConfig.NanoCpus,
      cpuQuota: c.HostConfig.CpuQuota,
      cpuPeriod: c.HostConfig.CpuPeriod,
      memorySwap: c.HostConfig.MemorySwap,
    }));
  } catch (err) {
    return { error: String(err) };
  }
}

let lastCpu: { total: number; idle: number } | null = null;
let lastDisk: { read: number; write: number; at: number } | null = null;

function hostStats(): unknown {
  try {
    if (process.platform === 'win32') {
      const script =
        "$cpu=(Get-CimInstance Win32_PerfFormattedData_PerfOS_Processor | Where-Object { $_.Name -eq '_Total' }).PercentProcessorTime; " +
        "$d=Get-CimInstance Win32_PerfFormattedData_PerfDisk_PhysicalDisk | Where-Object { $_.Name -eq '_Total' }; " +
        '[pscustomobject]@{cpuPercent=[double]$cpu;diskPercentTime=[double]$d.PercentDiskTime;' +
        'diskReadBytesPerSec=[double]$d.DiskReadBytesPerSec;diskWriteBytesPerSec=[double]$d.DiskWriteBytesPerSec} | ConvertTo-Json -Compress';
      const out = execFileSync('powershell', ['-NoProfile', '-Command', script], {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
      }).trim();
      return JSON.parse(out);
    }
    const stat = fs.readFileSync('/proc/stat', 'utf8').split('\n')[0]!.trim().split(/\s+/).slice(1).map(Number);
    const total = stat.reduce((a, b) => a + b, 0);
    const idle = (stat[3] ?? 0) + (stat[4] ?? 0);
    const cpuPercent = lastCpu ? (100 * (1 - (idle - lastCpu.idle) / Math.max(1, total - lastCpu.total))) : null;
    lastCpu = { total, idle };
    const disks = fs
      .readFileSync('/proc/diskstats', 'utf8')
      .split('\n')
      .filter((l) => /\s(sd[a-z]|nvme\d+n\d+|vd[a-z]|xvd[a-z])\s/.test(l))
      .map((l) => l.trim().split(/\s+/).map(Number))
      .reduce((acc, f) => ({ read: acc.read + (f[5] ?? 0) * 512, write: acc.write + (f[9] ?? 0) * 512 }), {
        read: 0,
        write: 0,
      });
    const at = Date.now();
    const disk = lastDisk
      ? {
          readBytesPerSec: (1000 * (disks.read - lastDisk.read)) / Math.max(1, at - lastDisk.at),
          writeBytesPerSec: (1000 * (disks.write - lastDisk.write)) / Math.max(1, at - lastDisk.at),
        }
      : null;
    lastDisk = { ...disks, at };
    return { cpuPercent, ...disk };
  } catch (err) {
    return { error: String(err) };
  }
}

async function pgSnapshot(dbs: Array<{ id: string; db: Db; tables: string[] }>): Promise<PgSample> {
  const out: PgSample = {};
  for (const { id, db, tables } of dbs) {
    try {
      const database = (
        await db.pool.query(
          `SELECT datname, numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
                  tup_inserted, tup_updated, tup_deleted, deadlocks, temp_files, temp_bytes,
                  conflicts, checksum_failures
           FROM pg_stat_database WHERE datname=current_database()`,
        )
      ).rows[0];
      const tbl = (
        await db.pool.query(
          `SELECT relname, n_live_tup, n_dead_tup, n_tup_ins, n_tup_upd, n_tup_del,
                  autovacuum_count, autoanalyze_count, last_autovacuum, last_autoanalyze
           FROM pg_stat_user_tables WHERE relname = ANY($1::text[]) ORDER BY relname`,
          [tables],
        )
      ).rows;
      const bgwriter = (
        await db.pool.query(
          `SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time,
                  buffers_checkpoint, buffers_clean, maxwritten_clean, buffers_backend,
                  buffers_backend_fsync, buffers_alloc
           FROM pg_stat_bgwriter`,
        )
      ).rows[0];
      const vacuum = (
        await db.pool.query(
          `SELECT pid, relid::regclass::text AS relation, phase, heap_blks_total, heap_blks_scanned,
                  heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples
           FROM pg_stat_progress_vacuum`,
        )
      ).rows;
      const activity = (
        await db.pool.query(
          `SELECT count(*)::int AS total,
                  count(*) FILTER (WHERE state='active')::int AS active,
                  count(*) FILTER (WHERE wait_event_type='Lock')::int AS waiting_on_lock
           FROM pg_stat_activity WHERE datname=current_database()`,
        )
      ).rows[0];
      out[id] = { database, tables: tbl, bgwriter, vacuum, activity };
    } catch (err) {
      out[id] = { error: String(err) };
    }
  }
  return out;
}

function newestArtifact(prefix: string, sinceMs: number): string | undefined {
  const dir = path.join(process.cwd(), 'benchmarks');
  if (!fs.existsSync(dir)) return undefined;
  const candidates = fs
    .readdirSync(dir)
    .filter((f) => /^loadgen-\d+\.json$/.test(f))
    .map((f) => {
      const full = path.join(dir, f);
      const stat = fs.statSync(full);
      if (stat.mtimeMs < sinceMs) return undefined;
      try {
        const parsed = JSON.parse(fs.readFileSync(full, 'utf8')) as { prefix?: string };
        if (parsed.prefix !== prefix) return undefined;
      } catch {
        return undefined;
      }
      return { full, mtime: stat.mtimeMs };
    })
    .filter((x): x is { full: string; mtime: number } => Boolean(x))
    .sort((a, b) => b.mtime - a.mtime);
  return candidates[0]?.full;
}

async function main(): Promise<void> {
  const args = parseArgs();
  process.env.CONTROL_DATABASE_URL ??= 'postgres://jobsched:jobsched@127.0.0.1:55432/jobsched_control';
  process.env.SHARD_DSN_MAP ??= JSON.stringify({
    'shard-a': 'postgres://jobsched:jobsched@127.0.0.1:55433/jobsched_shard',
    'shard-b': 'postgres://jobsched:jobsched@127.0.0.1:55434/jobsched_shard',
  });
  process.env.SHARD_IDS ??= 'shard-a,shard-b';
  const cfg = loadConfig();
  const control = makePool('control', cfg.CONTROL_DATABASE_URL, 2);
  const shardDbs = new Map<string, Db>();
  for (const id of shardIds(cfg)) shardDbs.set(id, makePool(id, cfg.shardUrls[id]!, 2));
  const pgTargets = [
    { id: 'control', db: control, tables: ['jobs'] },
    ...[...shardDbs.entries()].map(([id, db]) => ({ id, db, tables: ['runs', 'run_history', 'run_keys', 'partition_fence'] })),
  ];

  const dir = path.join(process.cwd(), 'benchmarks');
  fs.mkdirSync(dir, { recursive: true });
  const stamp = Date.now();
  const samples: Array<{ at: string; docker?: unknown; host?: unknown; pg?: PgSample }> = [];
  const limits = dockerLimits();
  let sampling = true;
  const sampler = (async () => {
    while (sampling) {
      const at = new Date().toISOString();
      const sample: { at: string; docker?: unknown; host?: unknown; pg?: PgSample } = { at };
      try {
        sample.docker = dockerStats();
      } catch (err) {
        sample.docker = { error: String(err) };
      }
      sample.host = hostStats();
      sample.pg = await pgSnapshot(pgTargets);
      samples.push(sample);
      await sleep(Math.max(500, args.intervalMs));
    }
  })();

  // Host -> published-port HTTP adds enough overhead to depress the offered rate. Prefer the
  // containerized load generator (container-to-container), matching the historical benchmarks.
  const npx = process.env.npm_config_user_agent ? 'npx' : process.platform === 'win32' ? 'npx.cmd' : 'npx';
  const commonArgs = [
    '--api',
    args.viaContainer ? 'http://api:3000' : args.apiUrl,
    '--rate',
    String(args.rate),
    '--duration',
    String(args.durationSec),
    '--timeout',
    String(args.timeoutSec),
    '--prefix',
    args.prefix,
  ];
  const loadgenArgs = args.viaContainer
    ? ['compose', 'run', '--rm', '--no-deps', '-T', 'loadgen', 'node', 'dist/tools/loadgen.js', ...commonArgs]
    : ['tsx', 'src/tools/loadgen.ts', ...commonArgs];
  const command = args.viaContainer ? 'docker' : npx;
  const startedAt = Date.now();
  const logPath = path.join(dir, `${args.prefix}-run.log`);
  const logStream = fs.createWriteStream(logPath);
  log('info', 'benchmark starting', { ...args, log: logPath });
  const child = spawn(command, loadgenArgs, {
    cwd: process.cwd(),
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
    shell: process.platform === 'win32',
  });
  child.stdout?.on('data', (d: Buffer) => {
    process.stdout.write(d);
    logStream.write(d);
  });
  child.stderr?.on('data', (d: Buffer) => {
    process.stderr.write(d);
    logStream.write(d);
  });
  const code = await new Promise<number | null>((resolve) => child.on('close', resolve));
  logStream.end();
  const finishedAt = Date.now();
  sampling = false;
  await sampler;

  const artifact = newestArtifact(args.prefix, startedAt - 1000);
  const loadgen = artifact ? (JSON.parse(fs.readFileSync(artifact, 'utf8')) as unknown) : null;

  const logs: Record<string, string> = {};
  for (const name of ['control-db', 'shard-a', 'shard-b']) {
    try {
      logs[name] = sh(`docker logs --since ${Math.ceil((finishedAt - startedAt) / 1000) + 10}s jobsched-${name} 2>&1`);
    } catch (err) {
      logs[name] = String(err);
    }
  }
  const autovacuumLogPath = path.join(dir, `${args.prefix}-autovacuum.log`);
  fs.writeFileSync(
    autovacuumLogPath,
    Object.entries(logs)
      .map(([name, text]) => `=== ${name} ===\n${text.split(/\r?\n/).filter((l) => /autovacuum|vacuum|checkpoint/i.test(l)).join('\n')}`)
      .join('\n\n'),
  );

  const combined = {
    timestamp: new Date().toISOString(),
    args,
    loadgenExitCode: code,
    loadgenArtifact: artifact ?? null,
    runLog: logPath,
    autovacuumLog: autovacuumLogPath,
    durationSec: (finishedAt - startedAt) / 1000,
    containerLimits: limits,
    sampleCount: samples.length,
    samples,
    loadgen,
  };
  const file = path.join(dir, `benchmark-${stamp}.json`);
  fs.writeFileSync(file, JSON.stringify(combined, null, 2));
  console.log(JSON.stringify({ artifact: file, loadgenArtifact: artifact, samples: samples.length, exitCode: code }, null, 2));

  await Promise.all([control, ...shardDbs.values()].map((d) => d.pool.end()));
  if (code !== 0) process.exitCode = code ?? 1;
}

main().catch((err) => {
  log('error', 'benchmark failed', { error: String(err), stack: (err as Error).stack });
  process.exit(1);
});
