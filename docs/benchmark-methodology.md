# Benchmark Methodology

This document defines how the scheduler benchmarks were run, how each metric is calculated, and how
the evidence was checked. It is the methodology companion to
[`benchmark-report.md`](benchmark-report.md) and [`benchmark-results.csv`](benchmark-results.csv).

## 1. Evidence policy

- Raw run artifacts are kept in `benchmarks/` and are treated as immutable evidence.
- Report values are recomputed from the raw JSON/log fields where possible.
- A result is **Verified** only when the value can be recomputed from a checked-in artifact or read
  directly from source code or checked-in configuration.
- **Partially verified** means the numeric value is present but its interpretation, causal
  attribution, or exact-run coverage depends on information not captured in the artifact.
- **Unverified** means a projection, an environment detail without raw evidence, or a statistical
  claim that the run design cannot support.
- **Excluded** means an artifact exists but is not valid evidence for the conclusions (smoke,
  incomplete, or interference).
- No benchmark number is rounded into a stronger claim than the artifact supports. Derived percentages
  show their formulas.

## 2. System and environment

| Item | Value |
|---|---|
| Host | Windows 11, Docker Desktop with WSL2 backend |
| CPU | Intel Core i5-13420H |
| Host RAM | 16 GB; Docker Desktop allocated approximately 8 GB and 12 CPUs |
| Container runtime | Node 20.20.2-alpine; PostgreSQL 16.15-alpine |
| Host tooling | Node 22.15.0 (TypeScript tools only) |
| Topology | 1 API, 2 schedulers, 4 workers, 1 control DB, 2 shard DBs |
| Data layout | 64 logical partitions, alternating even/odd across shard-a and shard-b |
| Worker concurrency | 64 per worker; 256 aggregate |
| Batch sizes | trigger 500, claim 100, completion 100 |
| Durability | `fsync=on`, `synchronous_commit=on`, `full_page_writes=on` |
| Autovacuum | enabled; per-table overrides; `naptime=5s`; 3 workers on control, 4 per shard; log runs over 1 s |

Exact container limits are in `docker-compose.yml`: control 512 MB, each shard 768 MB, API/workers
384 MB, schedulers 256 MB. No CPU quota is configured.

The environment rows above are recorded from `BENCHMARKS.md` and `docker-compose.yml`. Host CPU model,
RAM allocation, OS build, and exact image digests were not independently captured in the raw
benchmark artifacts, so they are "as recorded" rather than artifact-verified.

## 3. Workload model

The load generator creates one-off jobs with:

- unique IDs of the form `<prefix>-<sequence>`;
- `scheduleType: "once"`;
- `scheduledAt` set to the submission time;
- `maxAttempts: 1`;
- empty payload `{}` and the default no-op handler;
- tenant `bench` unless overridden.

Because each job is scheduled at submission time, the recorded trigger latency includes time waiting
in the submission batch and the scheduler path. It is **submission-to-durable-trigger** latency, not
the cost of a scheduler tick in isolation. End-to-end latency adds handler execution and completion
time.

## 4. Load generation

The generator is `src/tools/loadgen.ts`, invoked through the `loadgen` Compose service so that traffic
stays inside the Compose network.

- Default batch interval: 100 ms.
- Batch size: `round(rate * interval_ms / 1000)`. At 620/s and 100 ms, each tick requests 62 jobs.
- The loop runs until the configured duration expires or the target number of submissions is reached.
- Each batch is one `POST /jobs/batch` request (maximum 500 jobs per request in the API).
- `offeredAttempts` counts jobs sent, including batches retried after a 429.
- `submitted` counts jobs accepted with a successful HTTP response.
- A 429 causes the generator to wait for `Retry-After` and resubmit that batch.
- `maxAttempts: 1` means no retry-generated duplicate terminal rows in these runs.

After the submission window, the generator polls both shard databases once per second until the
successful terminal count reaches the submitted count or the completion timeout expires. For the
headline run, the timeout was 300 s and the drain completed in 3.33 s.

## 5. Verification queries and metric definitions

The generator queries each shard directly; it does not rely on API aggregates for correctness.

### 5.1 Terminal rows and latency

For rows in `run_history` whose `job_id` starts with the run prefix:

```sql
SELECT job_id, status, count(*)::int AS n,
       EXTRACT(EPOCH FROM (triggered_at - scheduled_time)) * 1000 AS trigger_ms,
       EXTRACT(EPOCH FROM (finished_at - scheduled_time)) * 1000 AS e2e_ms
FROM run_history
WHERE job_id LIKE $1
GROUP BY job_id, status, triggered_at, scheduled_time, finished_at;
```

Definitions:

- **distinctTriggers:** number of distinct `job_id` values across both shards.
- **duplicateTriggerRows:** sum of `n - 1` over grouped rows. A group is
  `(job_id, status, triggered_at, scheduled_time, finished_at)`. This detects repeated terminal rows
  for the same job identity, not raw duplicate keys in `run_keys`.
- **statusCounts:** counts by terminal status (`succeeded`, `dead`, `cancelled`).
- **trigger latency:** `triggered_at - scheduled_time`, in milliseconds.
- **end-to-end latency:** `finished_at - scheduled_time`, in milliseconds.
- **lostJobs:** submitted job IDs that are absent from `run_history` at the final verification.

### 5.2 Lost jobs

The generator keeps a set of every ID it submitted. At the end it reads `job_id` from `run_history` on
both shards and counts submitted IDs that were never observed. This is the `lostJobs` field. A run
passes the correctness gate only when `lostJobs = 0` and `duplicateTriggerRows = 0`.

### 5.3 Queue and running samples

Every 1 s during submission, the generator queries each shard:

```sql
SELECT count(*) FILTER (WHERE status = 'queued')::int AS queued,
       count(*) FILTER (WHERE status = 'running')::int AS running
FROM runs;
```

It sums the two shards and records the sample. `queueDepthMax` and `runningMax` are the maxima over
the run, not averages. The 300 samples in the headline artifact correspond to the 300 s window.

### 5.4 Percentiles

The generator sorts each latency array ascending and selects:

```text
index = min(n - 1, floor(p / 100 * n))
```

There is no interpolation. The headline run has 185,380 terminal rows, so the percentile indices are
large and the values are stable to the reported 0.01 ms. The method is the same for trigger and
end-to-end latency.

### 5.5 Rate definitions

| Field | Formula | Meaning |
|---|---|---|
| `submitWindowPerSec` / `acceptedPerSec` | `submitted / submitDurationSec` | Accepted throughput during the offering window |
| `offeredPerSec` | `offeredAttempts / submitDurationSec` | Offered throughput, including rejected/retried batches |
| `completedPerSec` / `achievedCompletedPerSec` | `completed / durationSec` | Completion throughput including drain |
| `submitDurationSec` | submission end - start | Time spent offering work |
| `durationSec` | final completion - start | Total wall time including drain |
| `drainDurationSec` | final completion - submission end | Time to finish the backlog |

The distinction matters. The pre-tuning artifact `loadgen-1792481335685.json` was written before the
rate-definition fix: its `offeredPerSec = 552.1` actually uses total duration (324.51 s), so it is a
completed-throughput value, not an offered or accepted-window rate. The correct accepted-window rate
from its own fields is 179,160 / 300.03 = 597.14/s. The report always states which denominator it is
using.

## 6. Resource and maintenance sampling

Resource sampling was done by a separate PowerShell process, not by the load generator. The script is
`scripts/capture-metrics.ps1`; for the headline run it was invoked with a 20 s interval and wrote
`benchmarks/tuned620c-metrics.jsonl`.

Each sample contains:

- host CPU percent, disk busy percent, disk read/write bytes per second;
- `docker stats` CPU and memory for the ten jobsched containers;
- PostgreSQL database counters, table counters (`n_live_tup`, `n_dead_tup`, insert/update/delete
  counts, autovacuum/autoanalyze counts and timestamps), background writer counters, and vacuum
  activity for control, shard-a, and shard-b.

The headline artifact has 13 samples. Average and maximum values in the report are computed over those
13 samples. A 20 s interval can miss peaks; sampled maxima are lower bounds on true peaks. Heavy
sampling in the same Docker Desktop session was explicitly avoided after the invalidated run
`loadgen-1792503651994.json`, where a 5 s sampler plus CIM and PostgreSQL queries distorted timing.

Autovacuum logs were captured with `log_autovacuum_min_duration=1000`. The checked-in
`tuned620-autovacuum.log` has no autovacuum lines, but its timestamps align with the earlier
`tuned620` run, not the headline `tuned620c` run. The report therefore treats the "no vacuum over 1 s
during the headline" claim as Partially verified, while the autovacuum counters from the headline
metrics are Verified.

## 7. Correctness and failure-path tests

The chaos tool is `src/tools/chaos.ts`. It injects four scenarios through Docker and direct SQL:

1. **Worker crash:** submit 300 jobs with a 4 s handler, wait until `worker-1` owns running rows, kill
   that container, and measure the time until no `runs` row is running under `worker-1`. Then wait for
   all 300 jobs to reach terminal state. The lease TTL is 5,000 ms and the target is TTL + 2,000 ms.
2. **Scheduler crash:** wait until `scheduler-1` owns partitions, kill it, and measure the time until
   it owns zero leases and no lease is expired. Then submit 50 jobs and wait for completion. Target:
   15,000 ms.
3. **Shard restart:** submit 120 jobs with a 3 s handler, restart `shard-a`, measure wall time, wait
   for the health check to report healthy, and wait for all 120 jobs to complete.
4. **Injected DB latency:** stop the normal workers, start a dedicated worker with `DB_LATENCY_MS=150`,
   submit 50 jobs, and count terminal rows processed by `worker_id = 'latency-worker'`.

The chaos artifact records recovery times, expected counts, and terminal `succeeded`/`dead`/
`cancelled`/`active` counts. It does **not** contain a duplicate-trigger field or a lost-job field, so
chaos correctness is reported as terminal completion plus the separately documented "0 lost /
0 duplicates" claim, which is only Partially verified from the artifact.

The normal test suite (`npm test`, Vitest) covers:

- duplicate trigger suppression and stale fence rejection;
- lease expiry/recovery and fenced completion;
- retries, backoff exhaustion, dead letters, and attempt preservation;
- cancellation races and cross-shard cancellation after a partition move;
- worker concurrency accounting under slow completions;
- cron DST/gap/overlap and misfire behavior;
- scheduler fairness, restart recovery, map changes, and two-scheduler coordination.

This report does not re-run the suite; the tests are implementation evidence for the mechanisms, not a
replacement for the production-shaped load run.

## 8. Run provenance and exclusions

The benchmark evidence includes these runs:

| Run | Artifact | Included? | Reason |
|---|---|---|---|
| Tuned autovacuum, 620 offered/s, 300 s | `loadgen-1792504181362.json` | Yes, headline | Complete, correct, low tail |
| Default autovacuum, 600 offered/s, 300 s | `loadgen-1792481335685.json` | Yes, comparison | Complete, but 9.54 s p99 |
| Autovacuum disabled, 620 offered/s, 300 s | `loadgen-1792481792672.json` | Yes, comparison | Complete; historical ceiling |
| 30 s baseline | `loadgen-1792475554458.json` | Yes, baseline | Complete short run |
| Single shard, 3,000 offered/s, 20 s | `loadgen-1792481898284.json` | Yes, shard probe | Complete; large backlog |
| Single shard, 5,000 offered/s, 15 s | `loadgen-1792481947272.json` | Yes, shard probe | Complete; large backlog |
| Earlier tuned run, 620 offered/s | `loadgen-1792500575383.json` | No | Only 163,060 submitted; interrupted design iteration |
| `tuned620b` with heavy sampler | `loadgen-1792503651994.json` | No | Submission slowed to 102.7/s; timing unusable |
| Smoke run, 100 offered/s, 20 s | `loadgen-1792500215869.json`, `smoke-capture-run.log` | No | Too short; warm-up only |

Excluded artifacts are retained in `benchmarks/` but are not used in the report's conclusions.

## 9. Reproducibility

To repeat the headline shape:

```bash
docker build -t jobsched-app:local .
docker compose up -d

# Reset production tables; restore the alternating map
docker exec jobsched-control-db psql -U jobsched -d jobsched_control -c \
  "TRUNCATE jobs, partition_leases, scheduler_registry; INSERT INTO partition_leases(partition_id) SELECT generate_series(0,63); UPDATE shard_map SET shard_id=CASE WHEN partition_id%2=0 THEN 'shard-a' ELSE 'shard-b' END"
docker exec jobsched-shard-a psql -U jobsched -d jobsched_shard -c \
  "TRUNCATE runs, run_history, run_keys, partition_fence"
docker exec jobsched-shard-b psql -U jobsched -d jobsched_shard -c \
  "TRUNCATE runs, run_history, run_keys, partition_fence"

docker compose run --rm --no-deps loadgen node dist/tools/loadgen.js \
  --rate 620 --duration 300 --timeout 300 --prefix tuned620c

# In a separate shell, sample resources every 20 s
pwsh -File scripts/capture-metrics.ps1 -Output benchmarks/metrics.jsonl -IntervalSec 20
```

For chaos:

```bash
npx tsx src/tools/chaos.ts
npm run chaos:smoke
```

Each generator run writes a new `benchmarks/loadgen-<timestamp>.json`. The output includes the summary
and the artifact path. The correctness gate is `lostJobs = 0` and `duplicateTriggerRows = 0`; a
non-zero value causes the generator to exit with code 2.

## 10. Claims this method cannot support

- A statistical comparison of two configurations with one run each.
- A precise capacity curve without per-second throughput data.
- Proof of exactly-once side effects; execution is at-least-once by design.
- Production capacity predictions from a laptop Docker Desktop host.
- A causal autovacuum attribution without a matching autovacuum log and settings snapshot.
- A 200,000-jobs-in-five-minutes claim at the measured 617.8 accepted jobs/s.
