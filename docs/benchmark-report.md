# Benchmark Report: Durable Sharded Job Scheduler

**Benchmark date:** 2026-10-20  
**Report date:** 2026-10-21  
**Scope:** one five-minute capacity run, one pre-tuning failure run, one autovacuum-disabled comparison, short and single-shard probes, and four failure-injection scenarios  
**Environment:** single Windows 11 host, Docker Desktop/WSL2, PostgreSQL 16, Node.js 20 containers  
**Evidence:** raw artifacts in `benchmarks/`; source-level definitions in `src/tools/loadgen.ts`, `src/tools/chaos.ts`, and the migration files  
**Source revision:** not available; the workspace is not a Git repository

> This report distinguishes **Verified**, **Partially verified**, and **Unverified** claims. A value is
> Verified only when it can be recomputed from a checked-in raw artifact or read directly from source.
> Projections and unsupported interpretations are labeled separately. No numbers in this report were
> invented or normalized to hide measurement differences.

## 1. Executive summary

The scheduler sustained **617.8 accepted jobs/s** during a 300.06 s submission window and completed
**185,380 of 185,380 jobs** with **0 lost jobs and 0 duplicate trigger rows**. Trigger latency was
**29.4 / 48.2 / 57.6 ms** at p50/p95/p99, and end-to-end latency was **65.4 / 134.4 / 190.2 ms**.
The queue drained 3.33 s after submission stopped.

| Headline result | Measured value | Status |
|---|---:|---|
| Accepted / succeeded | 185,380 / 185,380 | Verified |
| Submission-window rate | 617.81 jobs/s | Verified |
| Completion rate including drain | 611.04 jobs/s | Verified |
| Trigger p50 / p95 / p99 | 29.42 / 48.19 / 57.61 ms | Verified |
| End-to-end p50 / p95 / p99 | 65.39 / 134.43 / 190.23 ms | Verified |
| Lost jobs / duplicate trigger rows | 0 / 0 | Verified |
| HTTP errors / backpressure 429s | 0 / 0 | Verified |
| Max queued / max running | 124 / 64 (256 budget) | Verified |
| Worker crash recovery | 4,716 ms | Verified |
| Scheduler crash recovery | 9,448 ms | Verified |
| Shard restart | 3,986 ms wall, 378 ms to healthy | Verified |

**Bottom line:** this is credible evidence that the deployment meets a **600 jobs/s** target on this
host with durability and autovacuum enabled. It is **not** evidence of 200,000 jobs in five minutes:
185,380 jobs is **92.7%** of that goal, which would require **666.7 jobs/s** over 300 s. The gap is
14,620 jobs, or about 7.3% of the 200,000-job goal.

The pre-tuning run exposed a **9.54 s trigger p99** while the control database reached **200.84% CPU**
in the available Docker samples. The tuned run reduced trigger p99 by **99.4%** to 57.61 ms. The
commonly quoted throughput improvement of **10.7%** is arithmetically correct for completion rate
including drain, but it is not a clean steady-state capacity gain: accepted rate rose only **3.5%**
(597.1 to 617.8 jobs/s), while drain time fell from about **24.5 s to 3.3 s**. The latency improvement
is the well-supported result; the throughput difference is a single-run observation and is partly a
tail-latency effect.

## 2. System under test

### 2.1 Topology

```text
HTTP load generator
        |
        v
  Fastify API + reference UI
        |  job definitions, reads
        v
  control PostgreSQL
  jobs, shard_map, partition_leases, scheduler_registry
        ^
        |  leases + monotonic fencing tokens
        |
  scheduler-1 / scheduler-2
        |  fenced trigger insert
        v
  shard-a PostgreSQL          shard-b PostgreSQL
  runs, run_keys,             runs, run_keys,
  run_history,                run_history,
  partition_fence             partition_fence
        ^                             ^
        |  claim / heartbeat / complete
        |
  worker-1 .. worker-4
```

The renderable diagram is in [`benchmark-architecture.mmd`](benchmark-architecture.mmd). The design
keeps scheduling metadata in the control database and run churn in the shard databases. There is no
Redis, Kafka, or external lock service.

### 2.2 Key implementation facts

| Property | Value | Evidence |
|---|---|---|
| Logical partitions | 64, fixed | `src/domain/partition.ts` (`PARTITION_COUNT = 64`) |
| Partition hash | first 8 bytes of `sha256(tenant_id + "\0" + job_id) mod 64` | `src/domain/partition.ts` |
| Shard routing | `shard_map`; alternating even/odd in this run, 32/32 | `docker-compose.yml`, migrations, `BENCHMARKS.md` |
| Trigger idempotency | unique `(job_id, scheduled_time)` in `run_keys` | `src/migrations/shard/001_shard.sql`, `src/db/shard.ts` |
| Fencing | monotonic token validated in control update and shard-side trigger insert | `src/db/control.ts`, `src/db/shard.ts` |
| Execution contract | exactly-once trigger, at-least-once execution | `DESIGN.md`, `DECISIONS.md` ADR-005/006 |
| Worker slots | 64 per worker, 256 total | `docker-compose.yml` |
| Batch sizes | trigger 500, claim 100, completion 100 | `docker-compose.yml`, `BENCHMARKS.md` |
| Durability | `fsync=on`, `synchronous_commit=on`, `full_page_writes=on` | `docker-compose.yml` |
| Autovacuum | enabled with per-table overrides | `src/migrations/control/004_autovacuum.sql`, `src/migrations/shard/002_autovacuum.sql` |

### 2.3 Environment as recorded

| Item | Value | Verification |
|---|---|---|
| Host | Windows 11, Docker Desktop with WSL2 backend | As recorded in `BENCHMARKS.md`; not independently queryable from artifacts |
| CPU / RAM | Intel Core i5-13420H, 16 GB host; ~8 GB and 12 CPUs allocated to Docker | As recorded; not independently verifiable |
| Container images | Node 20.20.2-alpine, PostgreSQL 16.15-alpine | As recorded |
| Host tooling | Node 22.15.0 | As recorded |
| Topology | 1 API, 2 schedulers, 4 workers, 1 control DB, 2 shard DBs | Compose file |
| Resource limits | control 512 MB; each shard 768 MB; app containers 256-384 MB | Compose file |
| Workload | one-off jobs, `payload: {}`, `maxAttempts: 1`, no-op handler | `src/tools/loadgen.ts` |

The host is laptop-class and runs Docker Desktop. Absolute throughput should be read as an
order-of-magnitude result for this environment, not as a portable capacity guarantee.

## 3. Methodology in brief

The full procedure and metric definitions are in [`benchmark-methodology.md`](benchmark-methodology.md).

- The generator posts unique one-off jobs to `POST /jobs/batch` every 100 ms. At 620 offered/s, each
  tick requests 62 jobs.
- Jobs are scheduled at submission time. Therefore trigger latency
  (`triggered_at - scheduled_time`) is effectively **submission-to-durable-trigger** latency, not
  handler execution time.
- After submission the generator queries both shard databases directly and computes distinct
  triggers, duplicate trigger rows, lost jobs, terminal counts, trigger latency, end-to-end latency,
  and queued/running maxima.
- A run passes only when `lostJobs = 0` and `duplicateTriggerRows = 0`.
- Rates are separated by denominator: `submitted / submitDurationSec` for accepted throughput,
  `offeredAttempts / submitDurationSec` for offered throughput, and
  `completed / durationSec` for completion throughput including drain.
- Percentiles use the generator's implementation: sort ascending, index
  `min(n - 1, floor(p / 100 * n))`, with no interpolation. All 185,380 terminal rows contribute.
- Resource samples came from a separate 20 s PowerShell sampler, not from the load-generator process.

## 4. Results

### 4.1 Five-minute tuned-autovacuum run

Artifact: `benchmarks/loadgen-1792504181362.json`  
Log: `benchmarks/tuned620c-run.log`  
Samples: `benchmarks/tuned620c-metrics.jsonl` (13 samples, 20 s interval)

Target offered rate was 620/s, or 186,000 jobs in 300 s. The generator accepted 185,380 jobs, or
99.7% of the target. All accepted jobs succeeded.

| Metric | Value |
|---|---:|
| Offered attempts / accepted / succeeded | 185,380 / 185,380 / 185,380 |
| Submission window | 300.06 s |
| Total duration including drain | 303.38 s |
| Drain after submission | 3.33 s |
| Accepted per second | 617.81 |
| Completed per second including drain | 611.04 |
| Trigger p50 / p95 / p99 | 29.42 / 48.19 / 57.61 ms |
| End-to-end p50 / p95 / p99 | 65.39 / 134.43 / 190.23 ms |
| Max queued / max running | 124 / 64 |
| Lost / duplicate / 429 / HTTP errors | 0 / 0 / 0 / 0 |

This is a **single run**. There are no repeated runs, confidence intervals, or randomized ordering,
so the result should not be treated as a statistically characterized capacity estimate.

### 4.2 Pre-tuning failure run and the before/after comparison

Artifact: `benchmarks/loadgen-1792481335685.json`  
Log: `benchmarks/final600-run.log`  
Docker samples: `benchmarks/final600-docker-stats.txt`

The default-autovacuum run accepted 179,160 jobs in 300.03 s and completed all of them, but the
latency tail was unacceptable. The load generator's artifact records a legacy `offeredPerSec` of
552.1; that value actually divides submitted jobs by **total duration including drain** (324.51 s),
not by the submission window. The correct submission-window rate is **597.1 jobs/s**
(179,160 / 300.03 s). The completion rate including drain is **552.1 jobs/s**.

| Metric | Default autovacuum | Tuned autovacuum | Change |
|---|---:|---:|---:|
| Accepted jobs | 179,160 | 185,380 | +6,220 |
| Submission window | 300.03 s | 300.06 s | ~equal |
| Total duration | 324.51 s | 303.38 s | -21.13 s |
| Drain | 24.48 s | 3.33 s | **-86.4%** |
| Accepted per second (window) | 597.14 | 617.81 | **+3.5%** |
| Completed per second (incl. drain) | 552.09 | 611.04 | **+10.7%** |
| Trigger p50 / p95 / p99 | 40.17 / 116.37 / 9,542.46 ms | 29.42 / 48.19 / 57.61 ms | p99 **-99.4%** |
| End-to-end p99 | 9,676.44 ms | 190.23 ms | **-98.0%** |
| Lost / duplicate | 0 / 0 | 0 / 0 | unchanged |
| Max queued / max running | 115 / 94 | 124 / 64 | both below the 256 budget |

**Comparability caveat.** The before run offered about 597 jobs/s and the after run about 618 jobs/s,
a 3.5% difference in offered load. The 10.7% improvement in completed throughput is dominated by the
removal of a long tail: the before run took 24.5 s to drain, the after run 3.3 s. The correct
steady-state comparison is the submission-window rate, which improved only ~3.5%. With one run per
configuration, that difference is not statistically established. The strong, directly observable
result is the reduction in p99 latency.

**Attribution caveat.** `final600-docker-stats.txt` shows control-db CPU reaching **200.84%** (percent
of one CPU) at **07:28:29 UTC**, consistent with the recorded account of an autovacuum pass on the
high-churn `jobs` table beginning at 07:28:32. The 9.54 s p99 is Verified from the generator artifact. The specific claim
that autovacuum began at 07:28:32 UTC and caused that tail is **Partially verified**: it is recorded
in `BENCHMARKS.md` and ADR-010, and the CPU evidence is present, but no raw autovacuum log for this
run is checked in.

### 4.3 Autovacuum-disabled capacity run

Artifact: `benchmarks/loadgen-1792481792672.json`  
Log: `benchmarks/final620-run.log`  
Docker samples: `benchmarks/final620-docker-stats.txt`

This run paused autovacuum and used manual `VACUUM (ANALYZE)`. It is a historical capacity probe, not
a production recommendation.

| Metric | Value |
|---|---:|
| Submitted / succeeded | 184,884 / 184,884 |
| Submission window / total | 300.02 s / 309.73 s |
| Drain | 9.71 s |
| Accepted per second (window) | 616.24 |
| Completed per second including drain | 596.92 |
| Trigger p50 / p95 / p99 | 49.25 / 125.39 / 176.22 ms |
| End-to-end p50 / p95 / p99 | 102.51 / 199.88 / 260.47 ms |
| Max queued / max running | 135 / 112 |
| Lost / duplicate | 0 / 0 |

Compared with the tuned run, the autovacuum-disabled run had **0.25% lower** accepted throughput and
**2.3% lower** completed throughput including drain; its trigger p99 was **3.06x higher**
(176.22 ms vs 57.61 ms), or equivalently the tuned p99 was **67.3% lower**. Those differences are
descriptive only: each configuration has one run, and no statistical test was performed. They do not
support the claim that the two throughputs are "statistically the same."

### 4.4 Short baseline and single-shard probes

| Scenario | Artifact | Offered target | Submitted | Accepted/s (window) | Completed/s (incl. drain) | Trigger p99 | Max queue | Lost/dup | Status |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| 30 s baseline | `loadgen-1792475554458.json` | 600/s | 17,940 | 597.4 | 591.5 | 65.94 ms | 60 | 0/0 | Verified |
| Single shard, 20 s | `loadgen-1792481898284.json` | 3,000/s | 58,800 | 2,938.5 | 2,560.0 | 1,135.13 ms | 936 | 0/0 | Verified |
| Single shard, 15 s | `loadgen-1792481947272.json` | 5,000/s | 72,500 | 4,826.9 | 2,668.0 | 8,708.26 ms | 1,000 | 0/0 | Verified |

The 30 s artifact's `offeredPerSec` field is 591.49 because it predates the rate-definition fix; the
submission-window rate recomputed from its own fields is 597.4 jobs/s.

The single-shard probes mapped all 64 partitions to `shard-a`, paused autovacuum, and manually
vacuumed. They show a completion ceiling of roughly **2.6-2.9k jobs/s**, but the interpretation needs
care: at 3,000 offered/s the queue still reached 936, and at 5,000 offered/s it reached 1,000. The
shard drained at roughly 2.6-2.7k/s rather than accepting 3-5k/s with no backlog. The probes support
a shard-side ceiling estimate; they do not establish end-to-end capacity at those offered rates.

### 4.5 Failure injection (chaos)

Artifact: `benchmarks/chaos-mvgady8z.json`  
Command: `npx tsx src/tools/chaos.ts`

| Scenario | Injection | Recovery | Completion | Status |
|---|---|---|---:|---|
| Worker crash | Kill `worker-1` with 300 four-second jobs | 4,716 ms until no running rows owned by that worker; lease TTL 5,000 ms, target 7,000 ms | 300 succeeded in 5,031 ms | Verified |
| Scheduler crash | Kill `scheduler-1` while it owns 32 partitions | 9,448 ms until no leases owned by it and no expired leases; target 15,000 ms | 50/50 succeeded in 868 ms | Verified |
| Shard restart | Restart `shard-a` with 120 three-second jobs in flight | 3,986 ms wall; 378 ms to healthy | 120 succeeded in 2,897 ms | Verified |
| Injected DB latency | 150 ms per DB operation on a dedicated worker; other workers stopped | n/a | 50/50 processed by the latency worker in 1,564 ms | Verified |

The recovery metrics are defined by the chaos tool's polling conditions, not by an end-to-end
client-visible SLA. The artifact records terminal counts but does **not** record a duplicate-trigger
or lost-job field for each scenario, so the "0 lost / 0 duplicates" wording used for chaos elsewhere
is **Partially verified**; it is not present in `chaos-mvgady8z.json` itself. The completed counts are
directly recorded.

### 4.6 Resource utilization during the headline run

Thirteen samples were taken every 20 s during the tuned run. Host and container CPU percentages are
not directly comparable: host CPU is a share of the whole host, while Docker container CPU can exceed
100% because it is a share of one logical CPU.

| Host metric | Average | Maximum |
|---|---:|---:|
| CPU | 70.4% | 93.0% |
| Disk busy time | 17.7% | 33.0% |
| Disk write | 14.3 MB/s | 20.6 MB/s |
| Disk read | 12.9 MB/s | 35.6 MB/s |

| Container | CPU avg | CPU max | Memory max | Limit | Memory used |
|---|---:|---:|---:|---:|---:|
| API | 5.9% | 7.9% | 36.9 MiB | 384 MiB | 9.6% |
| control DB | 23.1% | 33.6% | 106.6 MiB | 512 MiB | 20.8% |
| shard-a | 30.8% | 52.5% | 213.5 MiB | 768 MiB | 27.8% |
| shard-b | 29.1% | 39.0% | 200.4 MiB | 768 MiB | 26.1% |
| scheduler-1 | 7.7% | 10.4% | 32.9 MiB | 256 MiB | 12.9% |
| scheduler-2 | 7.9% | 11.6% | 31.1 MiB | 256 MiB | 12.2% |
| workers (each) | 6.0-6.8% | 10.2-15.4% | 30.0-30.6 MiB | 384 MiB | 7.8-8.0% |

No container hit its memory limit. No CPU quota is configured, so CPU throttling was not a factor.
The 20 s sampling interval can miss short peaks; these are sampled values, not continuous maxima.

### 4.7 Correctness evidence

For the headline run:

- `submitted = completedSucceeded = distinctTriggers = 185,380`.
- `duplicateTriggerRows = 0`, `lostJobs = 0`.
- `statusCounts = { succeeded: 185380 }`; no dead or cancelled jobs.
- No HTTP errors and no 429 responses.
- `runningMax = 64` against a 256-slot budget.

`BENCHMARKS.md` additionally reports direct shard counts of 92,570 and 92,810 rows in `run_history`,
summing to 185,380, with zero active runs at the end. Those direct counts are **Partially verified**:
they are documented but not encoded in the load-generator JSON, so they cannot be independently
recomputed from the checked-in artifact alone.

The correctness mechanisms are covered by checked-in tests for duplicate trigger suppression, stale
fence rejection, lease recovery, cancellation races, and worker concurrency accounting. This report
did not re-run the test suite; the tests are implementation evidence, not a substitute for the
measured run.

### 4.8 Autovacuum counters

Final values from the last of the 13 resource samples:

| Database | Table | Autovacuums | Autoanalyzes | Dead at last sample | Max dead sampled |
|---|---|---|---:|---:|---:|
| control | `jobs` | 50 | 57 | 1,876 | 5,335 |
| shard-a | `runs` | 56 | 57 | 4,912 | 4,912 |
| shard-a | `run_keys` | 11 | 35 | 0 | 0 |
| shard-a | `run_history` | 11 | 35 | 0 | 0 |
| shard-b | `runs` | 57 | 57 | 3,052 | 4,048 |
| shard-b | `run_keys` | 11 | 36 | 0 | 0 |
| shard-b | `run_history` | 11 | 36 | 0 | 0 |

Dead tuples stayed in the low thousands and were reclaimed repeatedly. The claim that **no autovacuum
run exceeded 1 s during the headline run** is **Partially verified**: the checked-in
`tuned620-autovacuum.log` contains no autovacuum lines, but it belongs to an earlier `tuned620` run
(timestamp 12:44-12:49 UTC), not the 13:44-13:49 UTC headline run. The headline metrics samples show
no vacuum in progress, and the low p99 is consistent with short maintenance passes, but a dedicated
headline autovacuum log is not present.

## 5. The 200,000-job question

There are two different targets in the source material:

1. **600 jobs/s for 300 s** = 180,000 jobs. The headline run exceeds this with 185,380 jobs.
2. **200,000 jobs in 300 s** = 666.7 jobs/s. The headline run does **not** meet this.

| Target | Required rate | Measured | Result |
|---|---:|---:|---|
| 180,000 jobs in 300 s | 600.0/s | 617.8/s accepted, 185,380 jobs | Met |
| 200,000 jobs in 300 s | 666.7/s | 617.8/s accepted, 185,380 jobs | Not met; 92.7% of goal, 14,620 short |

At the measured accepted rate, 200,000 jobs would take about **323.7 s**, not 300 s. The single-shard
probe suggests the storage layer has headroom, but the control-plane and scheduler path was not
validated at 666.7 offered/s. Any claim that the system can do 200,000 in five minutes is a
**projection**, not a result in this evidence set.

## 6. Verification matrix

| Claim | Status | Basis |
|---|---|---|
| Headline accepted/s, completed/s, latency percentiles, queue/running maxima | Verified | Recomputed from `loadgen-1792504181362.json` |
| Headline 0 lost / 0 duplicate / 0 HTTP errors / 0 429s | Verified | Generator artifact |
| 9.54 s trigger p99 in default-autovacuum run | Verified | `loadgen-1792481335685.json` |
| Default-autovacuum run had 552.1 accepted/s | **Partially verified / corrected** | 552.1 is completed/s including 24.5 s drain; accepted/window is 597.1/s |
| Autovacuum caused the 9.54 s tail | **Partially verified** | 9.54 s p99 and 200.84% control CPU verified; no raw autovacuum log for that run checked in |
| Per-table autovacuum counters and dead-tuple bounds | Verified | `tuned620c-metrics.jsonl`; log coverage caveat above |
| No autovacuum run >1 s during headline | **Partially verified** | Checked-in log belongs to the earlier `tuned620` run, not the headline |
| Autovacuum-disabled 184,884 jobs / 616.2 accepted/s / 176.22 ms p99 | Verified | `loadgen-1792481792672.json` |
| 30 s baseline 17,940 jobs / 591.5 completed/s | Verified | `loadgen-1792475554458.json`; accepted/window recomputed as 597.4/s |
| Single-shard ~2.6-2.9k jobs/s ceiling | Verified values; **Partially verified** interpretation | Artifacts support completion rate; queues reached 936/1,000, so not zero-backlog acceptance |
| Chaos recovery times and completion counts | Verified | `chaos-mvgady8z.json` |
| Chaos "0 lost / 0 duplicates" | **Partially verified** | Not recorded as fields in the chaos artifact |
| Resource averages/maxima | Verified | Recomputed from the 13 resource samples |
| Host CPU model, RAM allocation, OS, exact image versions | As recorded; **Unverified** | Documented in `BENCHMARKS.md`; not independently captured in artifacts |
| Throughput unchanged versus autovacuum-disabled | **Unverified** | One run per configuration; no statistical test |
| 5k/7k jobs/s AWS projections | **Unverified** | Explicitly projections in `BENCHMARKS.md` |
| 200,000 jobs in five minutes | **Not met** | 185,380 jobs; would require 666.7/s |

## 7. Threats to validity

1. **One run per configuration.** No confidence intervals, repeated trials, or randomized ordering.
   Run-to-run variance on a laptop is unknown. Small differences (for example, 617.8 vs 616.2
   accepted/s) should not be treated as real effects.
2. **No throughput time series in the artifact.** The generator stores maxima and percentiles, not a
   per-second completed/queued series. A saturation curve or a precise "time at capacity" analysis
   cannot be produced from the checked-in JSON without new instrumentation.
3. **Tail-latency confounding.** The before/after completion-rate comparison includes drain. The
   before run's 24.5 s drain and 9.54 s p99 make the 10.7% completed-rate difference look larger than
   the 3.5% accepted-rate difference.
4. **Autovacuum attribution is not fully isolated.** The control CPU spike is present, but there is no
   checked-in autovacuum log or configuration snapshot for the failure run. A single correlated
   observation cannot prove causation.
5. **Headline autovacuum log mismatch.** The `tuned620-autovacuum.log` timestamps align with the
   earlier `tuned620` run, not `tuned620c`. The headline's "no vacuum over 1 s" claim is therefore
   supported by metrics and latency behavior, not by a dedicated log.
6. **Laptop and shared Docker resources.** Host CPU averaged 70.4% during the run. Background host
   activity, thermal state, and Docker Desktop scheduling can affect absolute results.
7. **Container-to-container load path.** `HANDOFF.md` records that host-to-published-port submissions
   topped out near 572/s in a 20 s diagnostic, while container-to-container reached 617.8/s. The
   measured result depends on the in-network load path.
8. **Generator verification scope.** Duplicate detection groups terminal rows by
   `job_id, status, triggered_at, scheduled_time, finished_at` in `run_history`; it is not a raw
   duplicate count over `run_keys`. Lost-job detection is based on submitted IDs absent from
   `run_history` at the end. The checks are meaningful but not a formal proof of exactly-once
   triggering under every failure.
9. **Single-shard probes are not production-shaped.** They use a remapped shard map, paused
   autovacuum, manual vacuum, and short durations with large backlogs.
10. **No source revision.** Without Git history, this report cannot pin the exact source tree used for
    the run beyond the checked-in files and local image name.
11. **Execution is at-least-once.** The zero-loss result concerns triggers and terminal bookkeeping,
    not exactly-once side effects. A worker crash after a side effect but before completion can re-run
    the handler.

## 8. Conclusions and recommendations

**Conclusions**

- On this host, with durability and tuned autovacuum enabled, the deployment supports the 600 jobs/s
  target with zero observed lost jobs or duplicate trigger rows.
- The tuned configuration avoids the multi-second trigger tail seen in the default-autovacuum run.
  Trigger p99 fell from 9.54 s to 57.6 ms; this is the strongest before/after finding.
- The measured headline does not demonstrate 200,000 jobs in five minutes. It demonstrates 185,380
  jobs at 617.8/s, about 92.7% of the 666.7/s needed for that goal.
- Chaos results show bounded recovery: 4.7 s for a worker, 9.4 s for a scheduler, and a 4.0 s shard
  restart with 378 ms to healthy.

**Recommendations**

1. Repeat the headline and comparison configurations at least three times, alternate their order, and
   report median and spread. Do not present single-run differences as statistical effects.
2. Add a per-second time series to the load-generator artifact (submitted, completed, queued,
   running, control/shard CPU) so saturation and maintenance stalls can be analyzed directly.
3. If 200,000 jobs in 300 s is a requirement, run a dedicated 666.7/s test. The current evidence only
   supports ~618/s accepted on this host.
4. Keep autovacuum enabled with the per-table settings; do not disable durability or maintenance for a
   headline number. Capture the autovacuum log and full PostgreSQL settings for every publishable run.
5. Validate on production-shaped, multi-node infrastructure, including managed PostgreSQL fsync and
   IOPS behavior, before converting these results into capacity commitments.
6. Extend the load verifier and chaos artifact to record raw `run_keys` duplicate counts and explicit
   lost/duplicate fields per scenario.
7. Revisit shard and partition counts only with measured scaling data; 64 partitions and two
   schedulers are sufficient for this workload but bound scheduler parallelism.

## 9. Reproduction

```bash
# Build and start the stack
docker build -t jobsched-app:local .
docker compose up -d

# Reset production tables before a clean run (PowerShell)
docker exec jobsched-control-db psql -U jobsched -d jobsched_control -c \
  "TRUNCATE jobs, partition_leases, scheduler_registry; INSERT INTO partition_leases(partition_id) SELECT generate_series(0,63); UPDATE shard_map SET shard_id=CASE WHEN partition_id%2=0 THEN 'shard-a' ELSE 'shard-b' END"
docker exec jobsched-shard-a psql -U jobsched -d jobsched_shard -c \
  "TRUNCATE runs, run_history, run_keys, partition_fence"
docker exec jobsched-shard-b psql -U jobsched -d jobsched_shard -c \
  "TRUNCATE runs, run_history, run_keys, partition_fence"

# Headline-equivalent run: 620 offered/s for 300 s, container-to-container
docker compose run --rm --no-deps loadgen node dist/tools/loadgen.js \
  --rate 620 --duration 300 --timeout 300 --prefix tuned620c

# Lightweight resource sampling in a separate PowerShell process
pwsh -File scripts/capture-metrics.ps1 -Output benchmarks/metrics.jsonl -IntervalSec 20

# Failure injection
npx tsx src/tools/chaos.ts
npm run chaos:smoke
```

The load generator writes a new `benchmarks/loadgen-<timestamp>.json`; the sampler writes the
requested output path. Existing artifacts should be treated as immutable evidence.

## 10. Evidence index

| Artifact | Contents | Use in this report |
|---|---|---|
| `benchmarks/loadgen-1792504181362.json` | Headline tuned run | Primary result |
| `benchmarks/tuned620c-run.log` | Headline generator log | Cross-check of headline |
| `benchmarks/tuned620c-metrics.jsonl` | 13 host/container/PostgreSQL samples | Resource and autovacuum tables |
| `benchmarks/loadgen-1792481335685.json` | Default-autovacuum failure run | Before/after comparison |
| `benchmarks/final600-run.log` | Failure-run generator log | Cross-check |
| `benchmarks/final600-docker-stats.txt` | Docker CPU/memory samples | 200.84% control CPU evidence |
| `benchmarks/loadgen-1792481792672.json` | Autovacuum-disabled run | Capacity comparison |
| `benchmarks/final620-run.log` | Autovacuum-disabled generator log | Cross-check |
| `benchmarks/final620-docker-stats.txt` | Docker samples | Resource context |
| `benchmarks/loadgen-1792475554458.json` | 30 s baseline | Short-run sanity check |
| `benchmarks/loadgen-1792481898284.json` | Single shard, 3,000 offered/s | Shard ceiling probe |
| `benchmarks/loadgen-1792481947272.json` | Single shard, 5,000 offered/s | Shard ceiling probe |
| `benchmarks/chaos-mvgady8z.json` | Worker/scheduler/shard/latency chaos | Failure recovery |
| `benchmarks/loadgen-1792500575383.json` | Incomplete earlier `tuned620` run (163,060 jobs) | **Excluded** from conclusions |
| `benchmarks/loadgen-1792503651994.json` | Invalidated sampler-interference run | **Excluded** from conclusions |
| `benchmarks/benchmark-1792500254196.json` | Capture artifact for the earlier `tuned620` run | Not headline evidence; source of log-coverage caveat |
| `benchmarks/smoke-capture-run.log` | 1,730-job smoke run | Not used for capacity claims |
| `docs/benchmark-results.csv` | Tabular results, including exclusions | Machine-readable summary |
| `docs/benchmark-methodology.md` | Detailed method and metric definitions | Methodology authority |
| `docs/benchmark-architecture.mmd` | Renderable topology | Architecture figure |

## Appendix A: Calculation examples

```text
accepted/window       = 185380 / 300.06            = 617.81 jobs/s
completed incl. drain = 185380 / 303.38            = 611.04 jobs/s
before accepted/window= 179160 / 300.03            = 597.14 jobs/s
before completed      = 179160 / 324.51            = 552.09 jobs/s
before drain          = 324.51 - 300.03            =  24.48 s
trigger p99 reduction = (9542.46 - 57.61) / 9542.46 = 99.4%
accepted-rate change  = (617.81 - 597.14) / 597.14  =  3.5%
completed-rate change = (611.04 - 552.09) / 552.09  = 10.7%
200k requirement      = 200000 / 300                = 666.67 jobs/s
headline vs 200k      = 185380 / 200000             = 92.7%
```

## Appendix B: Status definitions

| Status | Meaning |
|---|---|
| **Verified** | Recomputed from a checked-in raw artifact, or read directly from source/configuration, with no contradictory evidence. |
| **Partially verified** | The numeric value is present, but a causal claim, interpretation, or exact-run coverage depends on context that is not in the checked-in artifact. |
| **Unverified** | A projection, an environment detail without raw evidence, or a statistical claim not supported by repeated measurements. |
| **Excluded** | Artifact exists but is not valid evidence for the stated conclusions (smoke, incomplete, or sampler interference). |

