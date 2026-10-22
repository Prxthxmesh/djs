# Decisions

This document records the major architectural and operational choices, including alternatives
considered and the measured thresholds at which those alternatives should be revisited.

## ADR-001: PostgreSQL is the only infrastructure dependency

**Decision:** Use PostgreSQL for job state, shard routing, scheduler coordination, run queues,
idempotency, and history. Do not add Redis, Kafka, RabbitMQ, or an ORM.

**Rationale:**

- The target workload is 600 jobs/s. A single PostgreSQL shard measured ~2.7k no-op jobs/s under
  backlog, and the two-shard deployment measured 616 submissions/s for five minutes.
- PostgreSQL provides the required primitives directly: transactions, `FOR UPDATE SKIP LOCKED`,
  conditional updates, unique constraints, and durable writes with `fsync=on`.
- Fewer moving parts means one backup/restore model, one consistency model, and no cross-store
  dual-write problem.
- A queue broker would still need a database for job definitions and run history, so it would add
  a second source of truth without removing PostgreSQL.

**Thresholds to revisit:**

- Around **~6,000 jobs/s (about 10x the 600 target)**, evaluate Redis-backed ready queues or a
  purpose-built queue for claim latency and fan-out.
- Around **~60,000 jobs/s (about 100x the 600 target)**, evaluate Kafka or a similar partitioned log
  for ingestion and dispatch.
- Also revisit on measured PostgreSQL write saturation, cross-region requirements, or a need for
  multi-tenant priority scheduling that cannot be expressed with indexes and leases.

These are planning thresholds, not measured limits of the current implementation.

## ADR-002: Fixed logical partitions plus a data-driven shard map

**Decision:** Hash `(tenant_id, job_id)` into one of 64 logical partitions. Store
`partition_id -> shard_id` in `shard_map`.

**Alternatives:**

- Consistent hashing across worker/scheduler nodes: more complex ownership handoff and data
  movement.
- One table per partition: excessive schema objects, harder migrations.
- Direct modulo by shard count: rebalancing changes job-to-shard assignment and requires rehashing.

**Consequences:**

- Job-to-partition assignment is stable forever.
- Moving a partition is an explicit data operation, not a code change.
- Scheduler ownership is small (64 lease rows) and fair-share rebalancing is cheap.
- 64 partitions caps scheduler-level parallelism. Increase the constant if scheduler count grows
  far above a few dozen.

## ADR-003: Split control plane from shard data plane

**Decision:** Keep job definitions, schedules, leases, and the shard map in a control database.
Keep run queues, leashes, idempotency keys, and run history in per-shard databases.

**Rationale:**

- Job creation and scheduler coordination have different load and scaling characteristics from
  worker claims/completions.
- A worker cannot overload the control database with run churn.
- Shards can be added independently, and a shard restart does not take down job creation or
  scheduling for other shards.

**Trade-off:** The API reads job rows from control and runs from shards, so it must combine results.
This is acceptable for the reference UI and operational queries; a larger deployment would use a
read model or paginated per-shard APIs.

## ADR-004: Lease-based ownership with fencing tokens, not leader election

**Decision:** Schedulers acquire partition leases. Each acquisition increments a monotonic fencing
token. Every state transition that depends on ownership validates the token.

**Why not a single elected leader?**

- A leader becomes a throughput bottleneck for triggering and a failover bottleneck.
- Partition leases let all schedulers make progress and recover locally.
- Fencing makes stale owners harmless, which is stronger than relying on lease expiry alone.

**Why not advisory locks?**

- Advisory locks are session-scoped and do not survive reconnects.
- They do not provide a durable, monotonic token that a shard can validate inside an insert.
- Leases expose ownership and expiry for observability and fair-share rebalancing.

## ADR-005: Durable trigger before control advancement

**Decision:** Insert the run on the shard first, validating the shard-side fence and enforcing a
unique `(job_id, scheduled_time)` key. Advance the control-plane job only if every trigger in the
batch was accepted.

**Rationale:**

- If the control row advances first and the shard insert fails, the occurrence is lost.
- If the shard insert happens first and the scheduler dies, retrying the same trigger is safe due to
  the idempotency key.
- Rejecting the whole group on a fence mismatch prevents partial dispatch and makes the retry path
  deterministic.

**Trade-off:** A batch may be retried several times while fences settle. This is bounded and visible
in logs/metrics; it is preferable to silently skipping jobs.

## ADR-006: Execution is at-least-once; triggering is exactly-once

**Decision:** Guarantee one trigger row per scheduled occurrence, but allow a run to execute more
than once after a worker crash.

**Rationale:** A worker can perform side effects and die before committing completion. Making
execution exactly-once would require distributed transactions with every external side effect or a
deduplication protocol imposed on handlers. The honest contract is at-least-once execution with
idempotent handlers.

**Consequences:** The README and API documentation state this explicitly. Tests cover stale
completion and cancellation races.

## ADR-007: Separate `runs` and `run_history` tables

**Decision:** Active work lives in `runs`; terminal work moves to `run_history`.

**Rationale:**

- Queue indexes and `SKIP LOCKED` scans stay small.
- History queries and retention do not compete with live leases.
- Dead-letter and cancellation auditing are straightforward.

**Trade-off:** Completion is a move/delete plus insert in one transaction, creating dead tuples.
Autovacuum must be tuned for high churn; see ADR-010.

## ADR-008: `migrate` seeds only missing shard mappings

**Decision:** `seedShardMap` uses `ON CONFLICT DO NOTHING`.

**Rationale:** An earlier implementation used `DO UPDATE`, which silently reset operator shard moves
every time Compose started dependent services. That made runtime rebalancing and single-shard
benchmarks unsafe. Seeding must create a default topology only on first boot.

## ADR-009: Re-register fences against the current shard map

**Decision:** `registerOwnedFences` resolves each partition's shard from the current map on every
rebalance; it does not use the shard captured when the lease was acquired.

**Rationale:** When `shard_map` changes, a partition's lease is unchanged but its target shard moves.
A scheduler that kept the old `shardId` registered the fence on the old shard, then routed triggers to
the new shard, where no fence existed. Triggers were rejected forever and jobs were not advanced.
The fix also adds a regression test that changes the map at runtime.

**Known window:** A trigger tick that has already fetched jobs can still use the map it loaded for
that tick. The next rebalance updates routing; the durable idempotency key prevents duplicates.

## ADR-010: Keep database durability on; tune autovacuum operationally

**Decision:** Never disable `fsync`, `synchronous_commit`, or `full_page_writes`. Keep autovacuum
enabled by default. For clean capacity measurements, pause autovacuum, run manual
`VACUUM (ANALYZE)`, and state that condition in the benchmark report.

**Evidence:** One five-minute run with default autovacuum showed a tail stall while autovacuum
processed the high-churn `jobs` table: control CPU rose to ~200%, shards were nearly idle, and
trigger p99 reached 9.5s. In a controlled run with maintenance paused, 184,828 of 184,884 jobs were
complete about 5s after the submission window and all 184,884 finished at 309.7s, with trigger p99
176ms.

**Production guidance:** Tune per-table autovacuum scale factor/cost settings for `jobs`, `runs`,
`run_keys`, and `run_history`; monitor dead tuples and autovacuum duration. Do not treat the paused
benchmark as a recommendation to disable autovacuum in production. The tuning was implemented and
re-measured; see ADR-016.

## ADR-011: Add a due-time index for high-backlog behavior

**Decision:** Add `jobs_due_time_idx` on `(next_run_at) WHERE status='active' AND next_run_at IS NOT
NULL`.

**Rationale:** At high offered rates the scheduler can accumulate a large set of due jobs. The
partition-first index is correct for per-partition work, but the global due-time ordering benefits
from a dedicated partial index. This is a small, additive migration.

## ADR-012: Pool-level error handling

**Decision:** Every `pg.Pool` gets an `error` listener that logs transient idle-client failures.

**Rationale:** A shard restart terminates pooled idle connections. Without a listener, Node treats
the pool error as unhandled and crashes API/worker/scheduler processes. This was observed during the
first shard-restart chaos run. The listener does not hide query errors; it keeps the pool alive and
allows reconnection.

## ADR-013: Polling with adaptive backoff, not LISTEN/NOTIFY

**Decision:** Workers poll with `SKIP LOCKED` and adaptive 50ms-1s backoff. Schedulers poll every
25ms while behind, then back off.

**Rationale:**

- Polling has no missed-notification failure mode and needs no reconnect/state machine.
- At the measured scale, polling cost is small: 600 jobs/s used four workers and two schedulers with
  low CPU.
- `LISTEN/NOTIFY` can be added later as a latency optimization without changing correctness.

## ADR-014: Batch API is capped at 500 jobs

**Decision:** `/jobs/batch` accepts at most 500 jobs per request and inserts them in one transaction.

**Rationale:** Keeps JSON/body limits and transaction duration bounded, matches the trigger batch size,
and is sufficient to sustain 600+/s with 100ms load generator ticks. Larger batches increase lock
duration and failure blast radius without improving the measured bottleneck.

## ADR-015: Health checks use 127.0.0.1 inside containers

**Decision:** Container health checks call `http://127.0.0.1:<port>/health`.

**Rationale:** Alpine's resolver can try IPv6 `::1` first while Node binds IPv4. Using `localhost`
made every service report "starting"/unhealthy even though it was listening on `0.0.0.0`. The
explicit loopback address matches the listener family.

## ADR-016: Tune autovacuum per table instead of disabling it

**Decision:** Keep autovacuum enabled and add per-table overrides in migrations:

- `jobs`: vacuum scale 0.02 / threshold 200, insert scale 0.02 / threshold 1000, analyze scale 0.01 /
  threshold 100, cost limit 4000, cost delay 5ms.
- `runs`: vacuum scale 0.01 / threshold 500, insert scale 0.02 / threshold 1000, analyze scale 0.01 /
  threshold 100, cost limit 4000, cost delay 2ms.
- `run_keys` and `run_history`: vacuum scale 0.05 / threshold 2000, insert scale 0.05 / threshold
  5000, analyze scale 0.02 / threshold 1000, cost limit 4000, cost delay 2ms.
- Server settings: `autovacuum_naptime=5s`, `autovacuum_max_workers=3` on control / `4` per shard,
  `log_autovacuum_min_duration=1000`.

**Context:** The untuned default run hit trigger p99 9.5s when autovacuum processed the high-churn
`jobs` table while control CPU rose to ~200%. The previous headline paused autovacuum entirely, which
is not a production configuration.

**Evidence:** With tuning enabled, a five-minute 620/s run completed **185,380 jobs** at **617.8
submissions/s** and **611.0 completions/s**, with **0 lost / 0 duplicates**, trigger p99 **57.6ms**,
and e2e p99 **190.2ms**. `jobs` autovacuumed 50 times; `runs` 56-57 times per shard; dead tuples stayed
bounded; no autovacuum run exceeded 1s in the logs. Throughput matched the autovacuum-disabled run
within noise, so disabling maintenance buys no measurable capacity.

**Alternatives:**

- Leave defaults: rejected; multi-second tail at target load.
- Disable autovacuum and vacuum on a timer: rejected; dead tuples and bloat accumulate, and manual
  vacuum competes with traffic anyway.
- Time-partition `jobs`: deferred; more invasive than per-table tuning and not needed at this scale.
- Raise cost limit to 10,000: not selected; 4000 with short delays was enough and keeps each pass
  short.

**Consequences:** Maintenance runs frequently in short passes. Monitor
`pg_stat_user_tables.n_dead_tup`, autovacuum duration, and bloat; revisit if batch sizes, row width,
or shard count change substantially. Migrations `control/004_autovacuum.sql` and
`shard/002_autovacuum.sql` are idempotent `ALTER TABLE ... SET` statements.

## ADR-017: Count pending and completing work against worker concurrency

**Decision:** A worker may claim only while
`availableSlots(concurrency, active, pending) > 0`, where the in-flight count includes active handler
executions, pending claims, and completions currently being flushed to the database. The
`completing` counter is incremented before `flushPending` and released in `finally`.

**Context:** Load samples reported 384 or 512 durable running rows with a 256 total budget. The claim
loop counted `active.size + pending.length`, but runs whose handlers had finished and whose completion
was still being written occupied neither bucket. Under slow completion writes, the loop overclaimed
until the previous batch's rows finally left `running`.

**Evidence:** The integration test in `tests/worker.test.ts` installs a slow DELETE trigger and proves
durable running rows never exceed the configured concurrency (before the fix it reached 8 with a
budget of 4; after the fix it stays <= 4). In the tuned five-minute run, `runningMax` was 64 against
the 256 budget.

**Trade-off:** Accounting is conservative during completion flushes, so a worker may briefly execute
fewer handlers than the nominal limit. That is preferable to overclaiming and violating per-worker
concurrency. `runningMax` below the total budget is expected for fast no-op handlers; it is not a
capacity ceiling.

## ADR-018: Optional shared-token auth and per-tenant active-job quotas

**Decision:** Support an optional `API_AUTH_TOKEN` checked on `/jobs`, `/runs`, `/dead-letters`, and
`/shards` via `Authorization: Bearer` or `X-API-Key`, and an optional `TENANT_MAX_ACTIVE_JOBS` quota
enforced at job creation.

**Rationale:**

- The reference deployment previously had no way to protect mutating or data-bearing routes.
- A single shared token is enough to keep the API off an open network without introducing users,
  sessions, or an identity provider.
- The quota prevents one tenant from filling the `jobs` table or the scheduler's due set. It counts
  `active`/`paused`/`triggered` rows under a per-tenant advisory lock so concurrent creates cannot
  race past the limit.
- `/health`, `/metrics`, and the static UI stay unauthenticated for probes and local use.

**Trade-offs:**

- One token means no per-tenant identity or authorization; anyone with the token can see all tenants.
- There is no key rotation, audit trail, or TLS termination in the app.
- The quota is a creation-time cap, not a rate limit or worker-concurrency quota.
- Defaults are disabled (`unset` and `0`) to keep local development friction low. Production
  deployments must set both intentionally.

**Revisit when:** there are multiple independent tenants or compliance requirements. At that point
add per-tenant keys/roles, request-rate limiting, and TLS at the edge.

## ADR-019: Manual batched retention, not automatic deletion

**Decision:** Provide `src/tools/retention.ts` (`npm run retention`) to delete:

- `run_history` rows older than `--days` (default 30),
- `run_keys` rows older than `--run-keys-days` (default `max(3x days, 90)`) that have no matching live
  run,
- terminal (`completed`/`cancelled`) `jobs` older than `--days`,

in batches (default 5000) with a required explicit invocation and a `--dry-run` mode. Do not delete
from workers or schedulers automatically.

**Rationale:**

- Retention policy is deployment-specific; a hard-coded TTL can destroy audit data or idempotency
  state.
- `run_keys` is the duplicate-suppression ledger. Deleting it while a very old occurrence might still
  be retried can permit a duplicate trigger insert. Keeping it at least 3x the history window is a
  conservative default, not a proof.
- Batched deletes avoid long transactions, lock spikes, and WAL bursts on the hot tables.
- Keeping deletion external makes it schedulable by cron/Kubernetes CronJob without adding a
  self-managing behavior to the services.

**Consequences:** Operators must run it and monitor its output. There is no archive/export; deleted
rows are gone. Time-partitioning `run_history` is the natural next step if retention becomes frequent.
Never run aggressive `run_keys` deletion without understanding the scheduler retry horizon.

## ADR-020: Fan out cancellation and run listing across all shards

**Decision:** Maintain `shard_map` for trigger routing, but implement cancellation and
`/jobs/:id/runs` by querying every configured shard. A job's runs may therefore be found on a shard
that no longer owns its partition.

**Context:** A partition can move while runs are still active on the old shard. The previous API
resolved the job's current shard only, so cancellation silently missed old runs and run listings were
incomplete after a move.

**Rationale:**

- Cancellation is a safety operation; missing a run is worse than an extra query.
- The number of shards is small (currently 2); an all-shard query is cheap.
- Run rows carry `run_keys` identity, so duplicate suppression is unchanged.

**Trade-offs:** Cost grows linearly with shard count, and a slow/unavailable shard delays cancellation
or listing. There is still no live data migration: cancellation reaches old runs, but history stays on
the old shard. For a large shard fleet, replace the fan-out with a tenant/shard index or a read model.

## ADR-021: CI runs a worker-crash chaos smoke test

**Decision:** Add a `chaos-smoke` GitHub Actions job that builds the image, starts the Compose stack,
waits for health, runs `npm run chaos:smoke` (30 jobs, worker crash only), uploads
`benchmarks/chaos-*.json`, and always tears the stack down.

**Rationale:** The full chaos suite restarts schedulers and shards and injects DB latency, which is
slow and sensitive to runner resources. The worker-crash path is the highest-value recovery check and
fits the 20-minute job budget. Full chaos remains a local/release-gate procedure.

**Trade-offs:** Scheduler failover and shard restart are not covered on every push. Treat a green CI
run as a smoke signal, not a substitute for the full chaos artifact.

## Rejected alternatives

| Alternative | Why rejected |
|---|---|
| BullMQ / Redis queue | Adds a second datastore and still needs durable job definitions and history; unnecessary below the ~10x threshold. |
| Kafka ingestion | Operationally heavy relative to 600/s; ordering and exactly-once side effects still need handler idempotency. |
| ORM | The critical path uses `FOR UPDATE SKIP LOCKED`, `ON CONFLICT`, JSONB batch recordsets, and CTEs; SQL is clearer and easier to optimize. |
| Leader-elected scheduler | Bottleneck and failover risk; partition leases with fencing are simpler to scale and verify. |
| Advance control row before shard insert | Risks lost occurrences on a shard write failure. |
| Two-phase commit across control and shards | Not supported by the deployment model and unnecessary given idempotent triggering. |
| Exactly-once execution | Requires distributed transactions with arbitrary side effects; at-least-once with idempotent handlers is the honest contract. |
| File/CSV logging for benchmarks | Machine-readable JSON artifacts with direct DB verification are reproducible and auditable. |

## Open questions / future work

- Provide a shard data-migration tool for moving live partitions and their history.
- Schedule retention externally and add archival/export; evaluate time-partitioning `run_history`.
- Add per-tenant API keys, request-rate limits, audit logging, and TLS termination.
- Validate the tuned configuration on multi-node AWS and managed PostgreSQL.
- Extend CI chaos to scheduler failover and shard restart if runner capacity allows.
- Evaluate `LISTEN/NOTIFY` for latency-sensitive low-volume workloads.
- Revisit the 64-partition constant if scheduler count grows well beyond a few dozen.
- Revisit the 64-partition constant if scheduler count grows beyond a few dozen.
