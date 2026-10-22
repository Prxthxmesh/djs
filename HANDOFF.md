# Handoff

## Status

The scheduler is implemented, tested, benchmarked, and documented. The local Docker stack is running
and healthy with an **empty** production database and the default alternating shard map (32 partitions
per shard).

Last verification (October 20, 2026):

- `npm test` - **27/27 tests passed** across 6 files.
- `npm run lint` - passed with `--max-warnings=0`.
- `npm run typecheck` - passed (server and web).
- `docker compose ps` - API, 2 schedulers, 4 workers, control DB, shard-a, shard-b all `healthy`.
- Latest load artifact: `benchmarks/loadgen-1792504181362.json` (185,380 jobs, 617.8/s submission,
  0 lost, 0 duplicates) with tuned autovacuum enabled.
- Metrics artifact: `benchmarks/tuned620c-metrics.jsonl`.
- Latest chaos artifact: `benchmarks/chaos-mvgady8z.json` (all scenarios passed).

## What is here

| Area | Files |
|---|---|
| API/UI | `src/services/api.ts`, `src/services/api-main.ts`, `web/` |
| Scheduler | `src/services/scheduler.ts`, `src/scheduler/` |
| Worker | `src/services/worker.ts`, `src/services/worker-main.ts` |
| Data access | `src/db/control.ts`, `src/db/shard.ts`, `src/db.ts` |
| Schema | `src/migrations/control/`, `src/migrations/shard/` |
| Domain | `src/domain/` (partitioning, cron, retry, types) |
| Tools | `src/tools/loadgen.ts`, `src/tools/chaos.ts`, `src/tools/retention.ts`, `src/tools/benchmark.ts` |
| Tests | `tests/` |
| Docs | `README.md`, `DESIGN.md`, `DECISIONS.md`, `BENCHMARKS.md`, this file |
| Deploy | `Dockerfile`, `docker-compose.yml`, `.env.example`, `.github/workflows/ci.yml` |
| Ops helpers | `scripts/capture-metrics.ps1` (lightweight host/container/PG sampler) |

## Reproduce from scratch

```powershell
docker build -t jobsched-app:local .
docker compose up -d
docker compose ps
```

The `migrate` one-shot applies migrations and seeds only missing `shard_map` rows. It does not
overwrite an existing map.

Smoke test:

```powershell
curl.exe -s 127.0.0.1:3000/health
$body = @{tenantId='demo'; name='smoke'; scheduleType='once'; payload=@{}} | ConvertTo-Json
Invoke-RestMethod -Uri http://127.0.0.1:3000/jobs -Method Post -ContentType 'application/json' -Body $body
```

Run the full verification:

```powershell
npm ci
npm --prefix web ci
npm test
npm run lint
npm run typecheck
```

If the app image predates a source change:

```powershell
docker build -t jobsched-app:local .
docker compose up -d --force-recreate api scheduler-1 scheduler-2 worker-1 worker-2 worker-3 worker-4
```

Do not use `docker compose build` to retag this compose file; the shared `x-app` image causes a
parallel tag conflict. Build explicitly with `docker build`.

## Current environment state

- Production DB is intentionally empty: `jobs`, `partition_leases`, `scheduler_registry` and all shard
  `runs`/`run_history`/`run_keys`/`partition_fence` tables were truncated after the final benchmark.
- `shard_map` is alternating: even -> `shard-a`, odd -> `shard-b` (32/32).
- Tuned autovacuum is **active** in production databases:
  - `jobs`: vacuum scale 0.02/threshold 200, insert scale 0.02/threshold 1000, analyze 0.01/100,
    cost limit 4000, cost delay 5.
  - `runs`: vacuum scale 0.01/threshold 500, insert scale 0.02/threshold 1000, analyze 0.01/100,
    cost limit 4000, cost delay 2.
  - `run_keys` / `run_history`: vacuum scale 0.05/threshold 2000, insert scale 0.05/threshold 5000,
    analyze 0.02/1000, cost limit 4000, cost delay 2.
  - Server settings: `autovacuum_naptime=5s`, 3 workers on control, 4 on each shard,
    `log_autovacuum_min_duration=1000`.
- API auth is **disabled** by default (`API_AUTH_TOKEN` unset). Tenant quota is disabled by default
  (`TENANT_MAX_ACTIVE_JOBS=0`). Set both in production-shaped deployments.
- Retention is a manual tool (`npm run retention`), not a scheduled job.
- Test databases (`*_test`) are created and migrated by `tests/global-setup.ts` on each run.
- Host ports: API **3000**, control **55432**, shard-a **55433**, shard-b **55434**. A native
  PostgreSQL 17 may already own 5432, which is why these are remapped.
- Use **127.0.0.1** for host-to-container connections. `localhost` resolves to a broken IPv6 path on
  this machine.
- `jobsched-app:local` was rebuilt from the final source and all API/scheduler/worker containers were
  force-recreated after the last edits; all services are healthy. The bundled `dist/tools/*` are
  current.

## Measured results (short version)

- Five-minute tuned-autovacuum run (headline): **185,380 jobs**, **617.8/s** during the 300s window,
  **611.0/s** including a 3.3s drain, **0 lost / 0 duplicates / 0 errors**, trigger p99 **57.6ms**,
  e2e p99 **190.2ms**, `runningMax=64` against a 256 budget.
- Five-minute autovacuum-disabled run (historical capacity ceiling): 184,884 jobs at 616.2/s,
  0 lost/duplicates, trigger p99 176ms.
- Five-minute default-autovacuum run (failure evidence): 179,160 jobs, 0 lost/duplicates, but a
  **9.5s p99 stall** while autovacuum processed the high-churn `jobs` table. Fixed by per-table
  tuning (ADR-016).
- 30-second baseline: 17,940 jobs, 591.5/s, trigger p99 65.9ms.
- Single shard: ~2.6-2.9k no-op jobs/s ceiling. Two shards give ample headroom over 600/s.
- Chaos: worker lease recovery 4.7s, scheduler partition recovery 9.4s, shard restart 4.0s wall /
  378ms to healthy, 150ms latency worker processed 50/50.

See `BENCHMARKS.md` for methodology, exact artifacts, and the projection caveats.

## Known issues and risks

1. **At-least-once execution.** A worker crash after side effects but before completion can re-run
   the handler. Handlers must be idempotent. Triggering is exactly-once per occurrence.
2. **Auth is optional and coarse.** `API_AUTH_TOKEN` is one shared bearer token for all protected
   routes; there is no per-tenant identity, rotation, authorization, or TLS termination. Tenant
   quotas limit active jobs per tenant at creation time, not request rate or executor concurrency.
3. **Retention is manual and destructive.** `npm run retention` hard-deletes terminal history and
   old terminal jobs in batches; there is no archival/export and it is not scheduled. `run_keys` is
   the duplicate-suppression ledger: deleting it too early can allow a very late scheduler retry to
   insert an old occurrence again. The default 90-day `run_keys` window is a trade-off.
4. **Shard data migration is still missing.** Cancellation now fans out to every shard, and run
   listing merges all shards, so moved partitions are handled at query time. Historical runs are not
   copied to the new shard, and there is no live-partition migration tool. Drain a partition before
   moving it if history must live on the new shard.
5. **Shard map change window.** A scheduler tick that already fetched jobs uses the map loaded for
   that tick. The next rebalance re-registers fences. Idempotency prevents duplicates, but a tick may
   briefly retry. The runtime-change regression test covers the steady-state fix.
6. **CI chaos coverage is worker-only.** The `chaos-smoke` job kills a worker with 30 jobs. Full
   worker/scheduler/shard/latency chaos still runs locally.
7. **Offered-rate measurement requires the container load generator on this host.** The host-to-
   published-port path topped out around 572/s in a 20s diagnostic; container-to-container reached
   617.8/s. This is an environment limitation, not a scheduler limit.
8. **Heavy metrics sampling can invalidate a run.** A 5s sampler combining `docker stats`, CIM, and
   PostgreSQL queries stretched a benchmark far beyond its deadline. Use
   `scripts/capture-metrics.ps1` (20s default) or no sampler for publishable numbers.
9. **64 logical partitions.** Fine for this scale; revisit if scheduler count grows well beyond a few
   dozen.
10. **Projections to 5k/7k jobs/s are not measured.** They assume near-linear shard scaling and
    production-shaped storage. Validate on the target infrastructure.

## Recommended next steps

1. Build a shard data-migration tool for moving live partitions safely.
2. Schedule retention in the deployment (external cron/Kubernetes CronJob) and add archival/export
   before hard delete; consider time-partitioning `run_history`.
3. Add per-tenant API keys, TLS termination, and request-rate quotas; keep the active-job quota as a
   backstop.
4. Validate the tuned configuration on multi-node AWS with production-shaped handlers and managed
   PostgreSQL.
5. Extend CI chaos to scheduler failover and shard restart when the runner has enough resources.
6. Optionally evaluate Redis-backed queues above ~6k jobs/s and Kafka above ~60k jobs/s, per
   `DECISIONS.md` ADR-001.
7. Consider `LISTEN/NOTIFY` for latency-sensitive low-volume tenants.

## Useful commands

```powershell
# Status / logs
docker compose ps
docker compose logs -f api scheduler-1 worker-1

# Metrics
curl.exe -s 127.0.0.1:3000/metrics

# Load generator (container path; host path is slower on this machine)
docker compose run --rm --no-deps loadgen node dist/tools/loadgen.js `
  --rate 620 --duration 300 --timeout 300 --prefix manual
npx tsx src/tools/chaos.ts
npm run chaos:smoke

# Lightweight metrics sampler while a run is in progress
pwsh -File scripts/capture-metrics.ps1 -Output benchmarks/metrics.jsonl -IntervalSec 20

# Retention dry run, then apply
npm run retention -- --days 30 --dry-run
npm run retention -- --days 30 --run-keys-days 90

# Auth / quota (set on api and restart it)
$env:API_AUTH_TOKEN="change-me"
$env:TENANT_MAX_ACTIVE_JOBS="10000"

# Verify shard mapping and queue state
docker exec jobsched-control-db psql -U jobsched -d jobsched_control -c "SELECT shard_id, count(*) FROM shard_map GROUP BY 1 ORDER BY 1"
docker exec jobsched-shard-a psql -U jobsched -d jobsched_shard -c "SELECT status, count(*) FROM runs GROUP BY 1"
```

## Pointers

- Why the architecture looks like this: `DESIGN.md`
- Why alternatives were rejected and thresholds for revisiting: `DECISIONS.md` (ADR-016 through
  ADR-020 cover autovacuum tuning, worker slot accounting, auth/quota, retention, and cancellation
  fan-out)
- Measured numbers, methodology, and artifacts: `BENCHMARKS.md`
- Usage, API, configuration, operations: `README.md`
- Runtime-change regression test: `tests/scheduler.test.ts` ("re-registers shard fences after a shard
  map change")
- Worker concurrency regression test: `tests/worker.test.ts` ("never exceeds the durable running
  budget while completions are slow")
- Cross-shard cancellation and lease-expiry dead-letter tests: `tests/runs.test.ts`
- Chaos orchestration including the latency worker env-file workaround: `src/tools/chaos.ts`
