# Benchmarks

All numbers below are **measured** on the local Docker stack unless explicitly labeled **projected**.
Every load run writes a machine-readable artifact under `benchmarks/`.

## Environment

| Item | Value |
|---|---|
| Host | Windows 11, Docker Desktop (WSL2 backend) |
| CPU | Intel Core i5-13420H |
| Host RAM | 16 GB (Docker Desktop allocated ~8 GB, 12 CPUs) |
| Containers | Node 20.20.2-alpine, PostgreSQL 16.15-alpine |
| Host tooling | Node 22.15.0 (TypeScript tooling only) |
| Topology | 1 API, 2 schedulers, 4 workers, 1 control DB, 2 shard DBs |
| Worker concurrency | 64 per worker (256 total) |
| Batch sizes | trigger 500, claim 100, completion 100 |
| Durability | `fsync=on`, `synchronous_commit=on`, `full_page_writes=on` |
| Autovacuum | per-table overrides in migrations; `naptime=5s`, 3 workers control / 4 per shard, log >= 1s |
| Payload | no-op handler (`payload: {}`), one-off jobs scheduled at submission time |

The host is a laptop-class machine running Docker Desktop. Treat absolute throughput as an
order-of-magnitude indicator; relative comparisons and the failure results are the durable findings.

## Methodology

- Load generator submits `POST /jobs/batch` at a configured offered rate (default 100ms batches) via
  `src/tools/loadgen.ts`.
- Jobs are unique by prefix. After submission, the generator queries **both shard databases directly**
  and checks:
  - distinct triggered instances vs submitted jobs,
  - duplicate trigger rows (`run_keys`/history identity),
  - lost jobs,
  - terminal status counts,
  - trigger latency (`triggered_at - scheduled_time`) and end-to-end latency
    (`finished_at - scheduled_time`),
  - queued/running samples and max queue depth.
- A run passes only if `lostJobs=0` and `duplicateTriggerRows=0`.
- Rate fields are deliberately separated:
  - `submitWindowPerSec` / `acceptedPerSec` = `submitted / submitDurationSec`
  - `offeredPerSec` = `offeredAttempts / submitDurationSec`
  - `completedPerSec` = `completed / durationSec` (includes drain)
  - `achievedCompletedPerSec` is retained as an alias for `completedPerSec`.

Earlier artifacts were written before the reporting fix. Their `offeredPerSec` divided by total
duration, including drain, so it understated the submission-window rate. For example, the 184,884-job
artifact `loadgen-1792481792672.json` contains `offeredPerSec=596.92`; the correct submission-window
rate is **616.2/s**, while **596.9/s** is the completed rate including drain. Both numbers are reported
below.

### 200,000-job interpretation

The five-minute target is 600 jobs/s. Three conditions were measured:

1. **Default maintenance (untuned autovacuum)** at 600 offered/s, 300s. This exposed the tail stall.
2. **Tuned autovacuum** at 620 offered/s, 300s, using the per-table settings in the checked-in
   migrations. This is the production-representative headline run.
3. **Autovacuum-disabled capacity run** at 620 offered/s, 300s, with manual `VACUUM (ANALYZE)` and
   autovacuum paused per table. This isolates application throughput from background maintenance and
   remains useful as a ceiling.

## Results

### Five-minute run with tuned autovacuum (headline)

Artifact: `benchmarks/loadgen-1792504181362.json`
Metrics samples: `benchmarks/tuned620c-metrics.jsonl` (13 samples, every 20s)
Raw log: `benchmarks/tuned620c-run.log`

| Metric | Value |
|---|---|
| Offered rate | 620/s target |
| Duration | 300.06s submission window, 303.38s total |
| Submitted / succeeded | **185,380 / 185,380** |
| Submitted per second (window) | **617.8/s** |
| Completed per second (including 3.3s drain) | **611.0/s** |
| Lost jobs | **0** |
| Duplicate trigger rows | **0** |
| HTTP errors / 429s | 0 / 0 |
| Trigger p50 / p95 / p99 | **29.4 / 48.2 / 57.6 ms** |
| End-to-end p50 / p95 / p99 | **65.4 / 134.4 / 190.2 ms** |
| Max queued depth | 124 |
| Max running | 64 (budget 256) |
| Samples | 300 |

All 185,380 jobs reached `succeeded`; none were dead-lettered or cancelled. The run exceeded the
600/s target during the measurement window and drained to zero in 3.3 seconds.

This run supersedes the autovacuum-disabled run as the headline: throughput is statistically the same,
but trigger p99 is **57.6ms instead of 176.2ms** with maintenance enabled. The earlier 9.5s
autovacuum-induced tail did not recur.

### Autovacuum-disabled capacity run (historical)

Artifact: `benchmarks/loadgen-1792481792672.json`
Docker samples: `benchmarks/final620-docker-stats.txt`
Raw log: `benchmarks/final620-run.log`

| Metric | Value |
|---|---|
| Offered rate | 620/s target |
| Duration | 300.02s submission window |
| Submitted / succeeded | **184,884 / 184,884** |
| Submitted per second (window) | **616.2/s** |
| Completed per second (including 9.7s drain) | **596.9/s** |
| Lost jobs | **0** |
| Duplicate trigger rows | **0** |
| HTTP errors / 429s | 0 / 0 |
| Trigger p50 / p95 / p99 | **49.3 / 125.4 / 176.2 ms** |
| End-to-end p50 / p95 / p99 | **102.5 / 199.9 / 260.5 ms** |
| Max queued depth | 135 |
| Max running | 112 |
| Samples | 299 |

This run used manual vacuuming with autovacuum paused. Do not use it as evidence that autovacuum can
be left untuned, and do not disable autovacuum in production.

### Five-minute run with default autovacuum (failure evidence)

Artifact: `benchmarks/loadgen-1792481335685.json`
Raw log: `benchmarks/final600-run.log`

| Metric | Value |
|---|---|
| Offered rate | 600/s target |
| Submitted / succeeded | 179,160 / 179,160 |
| Completed per second (including drain) | 552.1/s |
| Completed at first progress check (~7s after submission) | 176,193 |
| Lost / duplicate | 0 / 0 |
| Trigger p50 / p95 / p99 | 40.2 / 116.4 / **9,542.5 ms** |
| End-to-end p50 / p95 / p99 | 84.9 / 190.1 / 9,676.4 ms |
| Max queued depth | 115 |

At 07:28:29 UTC the control database reached ~200% CPU while both shards were nearly idle; autovacuum
began on the high-churn `jobs` table at 07:28:32. Trigger p99 degraded to 9.5s. Throughput and
correctness were unaffected (all jobs completed), but the latency tail was not acceptable. This is
the regression that motivated ADR-016.

### 30-second baseline

Artifact: `benchmarks/loadgen-1792475554458.json`

| Metric | Value |
|---|---|
| Offered / submitted | 600/s / 17,940 |
| Completed | 17,940 |
| Achieved completed/s | 591.5 |
| Lost / duplicate | 0 / 0 |
| Trigger p50 / p95 / p99 | 32.8 / 54.2 / 65.9 ms |
| End-to-end p50 / p95 / p99 | 66.4 / 132.7 / 161.5 ms |
| Max queue depth | 60 |

### Single-shard ceiling

Method: temporarily map all 64 partitions to `shard-a`, truncate test data, manual
`VACUUM (ANALYZE)`, autovacuum paused, run load generator, then restore the alternating map. This
measures the PostgreSQL write/trigger/claim/completion path with no help from shard-b.

| Offered rate | Jobs | Duration | Achieved/s | Trigger p99 | Max queue | Artifact |
|---|---|---|---|---|---|---|
| 3,000/s for 20s | 58,800 | 23.0s | 2,560 | 1,135 ms | 936 | `loadgen-1792481898284.json` |
| 5,000/s for 15s | 72,500 | 27.2s | 2,668 | 8,708 ms | 1,000 | `loadgen-1792481947272.json` |

Interpretation: one shard sustains roughly **2.6-2.9k no-op jobs/s** before the offered rate exceeds
capacity. At 3,000 offered/s the shard kept up (58,800 completed in 23s including drain). At 5,000
offered/s a backlog formed (queue depth hit 1,000) and drained at roughly the same ~2.7k/s ceiling.
The two-shard deployment therefore has roughly 9x nominal shard-side headroom over the 600/s target,
though the control plane and scheduler count must be validated before claiming that end-to-end.

This is not a claim about production workloads: handlers with external I/O, larger payloads, or
slower disks will shift the bottleneck.

### Failure injection (chaos)

Artifact: `benchmarks/chaos-mvgady8z.json`

Command: `npx tsx src/tools/chaos.ts`

| Scenario | Result |
|---|---|
| Kill a worker with 300 in-flight runs | All worker-1 leases released/recovered in **4,716 ms** (lease TTL 5,000ms; target 7,000ms). All 300 succeeded, 0 lost, 0 duplicates. |
| Kill a scheduler while owning 32 partitions | Surviving scheduler recovered all owned partitions in **9,448 ms** (target 15,000ms). 50 new jobs all succeeded. |
| Restart shard-a with 120 in-flight runs | Restart wall time 3,986ms; health returned in 378ms; all 120 succeeded, 0 lost, 0 duplicates. Services did not crash. |
| 150ms injected DB latency on a dedicated worker | Normal workers stopped; latency worker processed **50/50** jobs itself; all succeeded. |

The shard-restart step originally crashed app processes with an unhandled pool error and failed on
`ECONNABORTED`; both were fixed and re-tested. See `DECISIONS.md` ADR-012.

A `--smoke` mode runs only the worker-crash scenario with 30 jobs and is executed by the CI
`chaos-smoke` job.

## Correctness checks performed by the generator

For the tuned-autovacuum headline run:

- `distinctTriggers = completedSucceeded = submitted = 185,380`
- `duplicateTriggerRows = 0`
- `lostJobs = 0`
- `statusCounts = {succeeded: 185380}`
- no HTTP 5xx and no backpressure rejections

Direct verification against both shards after the run:

```text
shard-a run_history: 92,570 rows, 92,570 distinct job_ids
shard-b run_history: 92,810 rows, 92,810 distinct job_ids
control jobs:        185,380 rows
active runs:         0 on shard-a, 0 on shard-b
```

The generator validates against both physical shards, not just the API, and cross-checks the control
plane job count.

## Resource and maintenance observations (tuned run)

Host, from 13 samples during the run:

| Metric | Average | Max |
|---|---|---|
| Host CPU | 70.4% | 93.0% |
| Host disk busy | 17.7% | 33.0% |
| Disk write | 14.3 MB/s | 20.6 MB/s |
| Disk read | 12.9 MB/s | 35.6 MB/s |

Container CPU and memory (`docker stats`, percent of one CPU / memory limit):

| Service | CPU avg | CPU max | Mem max | Limit |
|---|---|---|---|---|
| API | 5.9% | 7.9% | 9.6% | 384 MB |
| control DB | 23.1% | 33.6% | 20.8% | 512 MB |
| shard-a | 30.8% | 52.5% | 27.8% | 768 MB |
| shard-b | 29.1% | 39.0% | 26.1% | 768 MB |
| schedulers (each) | 7.7-7.9% | 11.6% | 12.9% | 256 MB |
| workers (each) | 6.0-6.8% | 15.4% | 8.0% | 384 MB |

No container hit its memory limit and no CPU quota is configured, so throttling was not a factor.

Autovacuum counters at end of run:

| Database | Table | Autovacuums | Autoanalyzes | Dead tuples at end | Max dead observed |
|---|---|---|---|---|---|
| control | `jobs` | 50 | 57 | 1,876 | 5,335 |
| shard-a | `runs` | 56 | 57 | 4,912 | 4,912 |
| shard-a | `run_keys` | 11 | 35 | 0 | 0 |
| shard-a | `run_history` | 11 | 35 | 0 | 0 |
| shard-b | `runs` | 57 | 57 | 3,052 | 4,048 |
| shard-b | `run_keys` | 11 | 36 | 0 | 0 |
| shard-b | `run_history` | 11 | 36 | 0 | 0 |

Dead tuples stayed bounded, no vacuum was sampled in progress, and `log_autovacuum_min_duration=1000`
produced **no autovacuum log lines over 1s** on any database. Note that
`pg_stat_user_tables.n_tup_ins` lagged during the run; the correctness counts above come from direct
`count(*)` queries, not those cumulative estimates.

`runningMax` was 64 against a 256 budget. The workload is a no-op handler at 620/s, so roughly 64
concurrent handler executions are sufficient; the number is not a capacity ceiling. The previous
384/512 samples were a worker accounting bug: pending completions were not occupying slots, so the
claim loop could overclaim while completion writes were slow. `availableSlots()` now counts active,
pending, and completing work, and the integration test asserts durable running rows never exceed the
configured concurrency (see `tests/worker.test.ts`).

## Invalidated run

`benchmarks/loadgen-1792503651994.json` is **not valid benchmark evidence**. It was collected while a
heavy metrics sampler (`docker stats` plus PowerShell CIM and PostgreSQL queries every 5s in the same
Docker Desktop session) ran alongside a nested `docker compose run`. Submission slowed to 102/s and the
load generator's completion loop stretched to 1,853s, far beyond its 300s deadline. The run still
reported 0 lost / 0 duplicates and `runningMax=256`, but its timing and rate fields are unusable.
Use the lightweight `scripts/capture-metrics.ps1` sampler, or no sampler, for publishable numbers.
`npm run benchmark:capture` now defaults to a 20s interval and samples only the ten jobsched
containers; the headline run above used the separate PowerShell sampler.

## Projected AWS capacity (not measured)

The original target also asks for projections toward 5,000 and 7,000 jobs/s. **These are projections,
not benchmark results.**

Assumptions:

- Bursting no-op/local-I/O handlers, similar batch sizes, PostgreSQL with durable commits.
- Single-shard capacity measured locally: ~2.7k jobs/s.
- Shards scale approximately linearly for independent partitions; the control plane and scheduler
  count must scale with them.
- Network and managed-database latency add overhead; managed PostgreSQL may have different fsync and
  IOPS characteristics.

| Target | Shards needed at ~2.7k/s each | Notes |
|---|---|---|
| 5,000 jobs/s | 2 shards minimum, 3 for headroom | Control DB and 2+ schedulers must sustain trigger/advance rate; workers scale horizontally. |
| 7,000 jobs/s | 3 shards minimum, 4 for headroom | Expect need for autovacuum tuning, larger control pool, and likely a dedicated control DB instance. |

At these rates, revisit ADR-001 thresholds. A 5k target is below the ~6k Redis evaluation line; a 7k
target is above it, so Redis or a partitioned queue should be evaluated with a production-shaped
workload. Kafka's ~60k threshold remains far above both targets.

These projections should be validated with a multi-node AWS test using the same verification tool.

## Reproduction commands

```bash
# Build once
docker build -t jobsched-app:local .
docker compose up -d

# Reset the production tables before a clean run (PowerShell)
docker exec jobsched-control-db psql -U jobsched -d jobsched_control -c \
  "TRUNCATE jobs, partition_leases, scheduler_registry; INSERT INTO partition_leases(partition_id) SELECT generate_series(0,63); UPDATE shard_map SET shard_id=CASE WHEN partition_id%2=0 THEN 'shard-a' ELSE 'shard-b' END"
docker exec jobsched-shard-a psql -U jobsched -d jobsched_shard -c \
  "TRUNCATE runs, run_history, run_keys, partition_fence"
docker exec jobsched-shard-b psql -U jobsched -d jobsched_shard -c \
  "TRUNCATE runs, run_history, run_keys, partition_fence"

# 5-minute run at 620 offered/s (container-to-container; the host port was slower on this host)
docker compose run --rm --no-deps loadgen node dist/tools/loadgen.js \
  --rate 620 --duration 300 --timeout 300 --prefix tuned620c

# Lightweight metrics sampling during a run (separate PowerShell process/job)
pwsh -File scripts/capture-metrics.ps1 -Output benchmarks/metrics.jsonl -IntervalSec 20

# Chaos
npx tsx src/tools/chaos.ts
npm run chaos:smoke

# Retention dry run
npm run retention -- --days 30 --dry-run

# Restore default shard map after a single-shard experiment
docker exec jobsched-control-db psql -U jobsched -d jobsched_control -c \
  "UPDATE shard_map SET shard_id=CASE WHEN partition_id%2=0 THEN 'shard-a' ELSE 'shard-b' END"
```

The tuned per-table autovacuum settings are applied by migrations;
`src/migrations/control/004_autovacuum.sql` and `src/migrations/shard/002_autovacuum.sql`. The
autovacuum-disabled capacity run was a one-off experiment and must not be used as production
configuration.

## Artifact index

| Artifact | Description |
|---|---|
| `benchmarks/loadgen-1792504181362.json` | **Headline:** five-minute tuned-autovacuum run, 185,380 jobs |
| `benchmarks/tuned620c-metrics.jsonl` | Host, container, and PostgreSQL samples during headline run |
| `benchmarks/tuned620c-run.log` | Generator log for headline run |
| `benchmarks/loadgen-1792503651994.json` | Invalidated run; do not use (sampler interference) |
| `benchmarks/loadgen-1792481792672.json` | Five-minute autovacuum-disabled run, 184,884 jobs |
| `benchmarks/final620-docker-stats.txt` | Docker samples during autovacuum-disabled run |
| `benchmarks/final620-run.log` | Generator log for autovacuum-disabled run |
| `benchmarks/loadgen-1792481335685.json` | Five-minute default-autovacuum failure evidence |
| `benchmarks/final600-docker-stats.txt` | Docker samples during default-autovacuum run |
| `benchmarks/loadgen-1792475554458.json` | 30-second baseline |
| `benchmarks/loadgen-1792481898284.json` | Single shard, 3,000 offered/s |
| `benchmarks/loadgen-1792481947272.json` | Single shard, 5,000 offered/s |
| `benchmarks/chaos-mvgady8z.json` | Worker/scheduler/shard/latency chaos results |
| `benchmarks/loadgen-*.json` | Additional exploratory runs |
