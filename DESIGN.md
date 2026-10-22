# Design

## Goals

1. Durable job creation and dispatch across process and database failures.
2. No lost jobs and no duplicate trigger rows for the same scheduled occurrence.
3. Horizontal scaling of API, schedulers, and workers.
4. PostgreSQL only: no external queue, cache, consensus service, or ORM.
5. Operationally simple failure recovery with explicit fencing rather than leader election.
6. Measured behavior under load and injected failures, with artifacts checked in.

## Non-goals

- Exactly-once side effects. Execution is at-least-once; handlers must be idempotent.
- Multi-region consensus or cross-region active-active scheduling.
- Per-job container isolation.
- Full multi-tenant identity, per-tenant authorization, API-key rotation, or TLS termination. A single
  optional shared token and tenant active-job quotas are provided as operational guardrails.

## Component topology

```text
API (Fastify)  ---- writes jobs, reads runs/history/dead letters
  |
  +--> control PostgreSQL
  |
  +--> shard PostgreSQL instances (read-only status views for API)

Scheduler N ---- control DB: scheduler registry, partition leases, due jobs
  |
  +--> shard DB: partition fences + durable trigger insert

Worker N ------- shard DB: claim, heartbeat, completion, reaper
  |
  +--> in-process handler execution
```

Every service has its own database pool. PostgreSQL is the coordination substrate; there is no
separate queue or lock service.

## Partitioning

```text
partition = first 8 bytes of sha256(tenant_id || 0x00 || job_id) mod 64
```

- 64 logical partitions are fixed and independent of shard count.
- `shard_map(partition_id, shard_id)` is authoritative routing data.
- Adding shards later means updating mappings and optionally migrating historical runs; job IDs do
  not need to be rehashed.
- Cron occurrences of the same job stay on the same partition and shard.

The 64-partition limit bounds scheduler ownership metadata and makes fair-share rebalancing cheap.
At the measured workload, 64 partitions provide enough distribution; a production system with much
higher scheduler counts would increase this constant.

## Control schema

```sql
jobs(
  id text primary key,
  tenant_id text not null,
  name text not null,
  partition_id smallint not null,       -- 0..63
  schedule_type text not null,          -- once | cron
  cron_expression text,
  timezone text not null,
  next_run_at timestamptz,
  misfire_policy text not null,         -- skip | run_once
  payload jsonb not null,
  max_attempts int not null,
  status text not null,                 -- active | paused | cancelled | completed | triggered
  created_at, updated_at, last_triggered_at
)

shard_map(partition_id smallint primary key, shard_id text not null, updated_at)

partition_leases(
  partition_id smallint primary key,
  owner_id text,
  fencing_token bigint not null,
  lease_expires_at timestamptz not null
)

scheduler_registry(id text primary key, started_at, last_seen)
```

Indexes include `jobs_due_idx (partition_id, next_run_at) WHERE status='active'`,
`jobs_due_time_idx (next_run_at) WHERE status='active' AND next_run_at IS NOT NULL`,
and listing indexes for the API.

## Shard schema

```sql
runs(
  id text primary key,
  job_id text, tenant_id text, partition_id smallint,
  scheduled_time timestamptz,
  idempotency_key text,
  payload jsonb,
  status text,                          -- queued | running
  attempt int,
  run_at timestamptz,
  triggered_at timestamptz,
  worker_id text,
  lease_token bigint,
  lease_expires_at timestamptz,
  cancel_requested boolean,
  last_error text
)

run_keys(job_id, scheduled_time, idempotency_key, run_id, primary key(job_id, scheduled_time))

run_history( -- terminal rows, same identity fields as runs
  status text,                          -- succeeded | dead | cancelled
  finished_at timestamptz,
  ...
)

partition_fence(partition_id primary key, fencing_token bigint, owner_id, fence_expires_at)
```

`status` columns are constrained with checks. `runs` and `run_history` are separate so active-table
indexes remain small and terminal history can be queried without scanning live leases.

## Scheduling algorithm

Each scheduler loop:

1. Heartbeat `scheduler_registry`.
2. Reload `shard_map`.
3. Renew owned leases; drop locally owned partitions that were not renewed.
4. Compute fair share `ceil(64 / active_schedulers)`.
5. Release excess partitions or acquire expired partitions. Acquisition increments the fencing token.
6. Re-register the **current** map's shard-side fence for every owned partition.
7. Fetch due jobs:

```sql
SELECT j.*, l.fencing_token
FROM jobs j
JOIN partition_leases l ON l.partition_id = j.partition_id
WHERE j.status='active'
  AND j.next_run_at IS NOT NULL
  AND j.next_run_at <= now() + lead
  AND j.partition_id = ANY($partitions)
  AND l.owner_id=$owner
  AND l.lease_expires_at > now()
ORDER BY j.next_run_at
LIMIT 500
```

8. For each job, build a trigger and an advancement operation.
9. Group triggers by current shard.
10. Insert triggers with fence validation and idempotency in one statement.
11. Only if every trigger in the group was accepted, advance the control rows
    (`once -> triggered`, `next_run_at=NULL`; cron -> next occurrence).

If any trigger in the group is rejected, no job in that group is advanced. This prevents partial
dispatch from silently skipping occurrences. On the next tick the same jobs are retried; `run_keys`
suppresses already-inserted occurrences.

### Why 64 partitions with 2 schedulers

With two schedulers each owns 32 partitions. If one dies, its leases expire after 10s and the survivor
takes all 64 within one renewal cycle. The chaos run observed ~9.4s recovery, matching lease TTL plus
renewal.

## Fencing protocol

Ownership is protected at two levels:

1. **Control-plane advancement** validates `owner_id + fencing_token + lease not expired` in the same
   UPDATE as the job state transition.
2. **Shard-side trigger** joins `partition_fence` on `(partition_id, trigger_fence)` and requires an
   unexpired fence before writing `run_keys` and `runs`.

A stale scheduler cannot:
- advance a job after losing its partition;
- insert a run after a new scheduler has taken over and registered a higher fence.

Fencing tokens are monotonically incremented on acquisition and are never reused.

On a shard map change, `registerOwnedFences` resolves the target shard from the *current* map rather
than the shard captured at acquisition. This prevents triggers from being routed to a new shard whose
fence was never registered. A regression test changes `shard_map` at runtime and verifies a job whose
partition was previously on shard-b is triggered on shard-a.

## Exactly-once trigger / at-least-once execution

Trigger insert:

```text
input -> validate fence -> insert run_keys (ON CONFLICT DO NOTHING)
      -> insert runs for keys actually won (ON CONFLICT DO NOTHING)
```

- Duplicate scheduler delivery: `run_keys` conflict -> no second run.
- Scheduler crash after shard insert but before control advancement: retry reuses the same
  `(job_id, scheduled_time)` key; one run row remains; control is advanced exactly once.
- Scheduler crash before shard insert: no run; control remains due; retry dispatches.

Workers execute handlers after claiming. Completion is fenced by `(worker_id, lease_token)` and
deletes/moves the run in a transaction. If a worker dies after side effects but before completion,
the reaper requeues the run, so side effects can occur again. This is the documented trade-off.

## Worker execution

- `claim` uses `FOR UPDATE SKIP LOCKED` to avoid contention and claims at most available concurrency.
- In-flight accounting counts active handlers, pending claims, and completions currently being
  flushed. A finished handler still occupies a slot until its completion write succeeds.
- Each run gets a lease token and `lease_expires_at`.
- Heartbeats extend live leases and return `cancel_requested`.
- The default handler supports `sleepMs`, `fail`, and `failTimes` for tests/benchmarks.
- Completion batches are grouped by shard, fenced, and applied atomically per shard.
- The reaper runs every second: expired running leases are requeued with attempt+1 or moved to
  `run_history` as `dead` when attempts are exhausted. Cancelled runs are never requeued.
- SIGTERM stops claiming, waits for active work up to `SHUTDOWN_GRACE_MS`, aborts remaining work,
  releases leases, flushes pending completions, and closes pools.

Retries use exponential backoff with full jitter (`retryDelayMs` + `backoffWithJitter`), capped by
configuration.

## Cancellation

API cancellation:

1. Mark the job record `cancelled`.
2. On **every configured shard**, find active runs for the job and archive queued runs as
   `cancelled`. This reaches runs left behind by a shard-map move, not only the current partition
   owner.
3. For running runs, set `cancel_requested`; the worker heartbeat observes it and aborts via
   `AbortSignal`.
4. If no active runs exist, set the job to `completed` and report that it was already complete.

Running completion racing a cancellation: cancellation wins. The completion path checks
`cancel_requested`/status and records `cancelled` rather than `succeeded`. The reaper also never
requeues a cancelled run. `/jobs/:id/runs` merges results from all shards for the same reason.

## Cron and misfire semantics

- Cron evaluation uses `cron-parser@5.3.1` with IANA time zones.
- `skip`: missed occurrences are skipped; the next future occurrence is scheduled.
- `run_once`: one missed occurrence is dispatched, then the schedule advances past the gap.
- DST gap (nonexistent local time) moves forward once (e.g. 02:30 America/New_York -> 03:30 EDT).
- DST overlap (ambiguous local time) fires once on the first occurrence, then the next day.
- The scheduler compares `date_trunc('milliseconds', expected)` to avoid microsecond equality
  failures at Postgres/JS precision boundaries.

## Backpressure

The API periodically samples queued-run counts across shards and caches the total. Writes above
`QUEUE_BACKPRESSURE_THRESHOLD` return HTTP 429 with `Retry-After: 1`. Batch creation is also
transactional; no partial batch is inserted.

## Authentication and tenant quotas

Authentication is an optional shared secret. When `API_AUTH_TOKEN` is set, the API requires
`Authorization: Bearer <token>` or `X-API-Key: <token>` on `/jobs`, `/runs`, `/dead-letters`, and
`/shards`. `/health`, `/metrics`, and the static UI remain open. Comparison is constant-time; missing
or wrong credentials return 401.

`TENANT_MAX_ACTIVE_JOBS` optionally caps the number of `active`/`paused`/`triggered` jobs per tenant.
Creation takes a per-tenant advisory lock, counts active jobs, and rejects the whole batch with 429 if
the limit would be exceeded. This prevents one tenant from monopolizing the due set and the `jobs`
table. It is not a request-rate limit or a worker-concurrency quota. Both features default to off.

## Retention

Terminal state grows forever if nothing deletes it. `npm run retention` removes old `run_history`,
old terminal `jobs`, and `run_keys` rows that no longer have a live run, in bounded batches. The
`run_keys` window defaults to at least three times the history window and at least 90 days because it
is the duplicate-suppression ledger: deleting it too early can let a very late scheduler retry insert
an old occurrence again. Retention is a manual, dry-run-capable tool rather than a service loop; an
external scheduler should invoke it. There is no archive/export yet.

## Observability

Prometheus metrics from API, schedulers, and workers:

- `jobsched_trigger_latency_seconds` (scheduled -> durable insert)
- `jobsched_claim_wait_seconds{shard}`
- `jobsched_triggers_inserted_total{shard}`
- `jobsched_runs_completed_total{shard,status}`
- `jobsched_retries_total`, `jobsched_dead_letters_total`
- `jobsched_lease_expirations_total`
- `jobsched_queue_depth{shard}`
- `jobsched_owned_partitions`
- `jobsched_backpressure_rejections_total`
- `jobsched_db_pool_connections{db,state}`
- Node process metrics via `prom-client`

The API exposes `/metrics`; each scheduler/worker also serves metrics on its ops port.

## API and UI

Fastify validates bodies with Zod and serves the built React UI from `web/dist`. The UI provides job
creation (one-off/cron), list/detail, cancel, recent runs, dead letters, and shard/queue status.
The UI is intentionally a reference client, not an authenticated product; it does not manage tokens,
so use API clients when `API_AUTH_TOKEN` is enabled.

## Migration and deployment behavior

- Migrations are versioned SQL files recorded in `schema_migrations`.
- `seedShardMap` inserts only missing partitions (`ON CONFLICT DO NOTHING`); it must not overwrite
  operator shard moves on every deployment.
- The `migrate` Compose service runs before API/schedulers/workers; it is idempotent.
- The Docker image contains compiled server JS, migration SQL, and built web assets.
- App health checks use `127.0.0.1` inside containers because `localhost` can prefer IPv6 while
  Node listens on IPv4.

## Failure analysis

| Failure | Detection | Recovery | Guarantee |
|---|---|---|---|
| Scheduler crash | partition lease expiry | surviving scheduler acquires and re-registers fences | no duplicate triggers; <= TTL gap |
| Worker crash | run lease expiry | reaper requeues with attempt+1 | at-least-once execution |
| Shard restart | connection error | pool retry/reconnect, trigger insert is atomic | no trigger without durable run; no duplicate key |
| API crash | orchestrator restart | stateless restart | no write acknowledged before commit |
| Scheduler duplicate delivery | `run_keys` conflict | suppressed | one run per occurrence |
| Stale scheduler | fence mismatch | trigger rejected; no advancement | old owner cannot write |
| Cancellation vs completion | cancel flag + fenced completion | cancellation wins | cancelled run never requeued |
| Run left on old shard after map move | all-shard cancellation/listing | API queries every shard | cancellation reaches old shard; data is not migrated |
| Worker completion write is slow | in-flight slot accounting | claim stops until a slot frees | durable running rows <= concurrency |

## Capacity model

Measured (local Docker, 2 shards, 4 workers): a five-minute run with tuned autovacuum sustained
**617.8 submissions/s** and completed **185,380 jobs** with zero loss/duplicates, trigger p99 57.6ms.
A single shard sustained ~2.7k no-op jobs/s under backlog. Capacity is dominated by PostgreSQL
write/fsync throughput and scheduler trigger batching, not by Node CPU. Horizontal scaling requires
adding shards and updating `shard_map`; adding workers alone does not increase a single shard's write
ceiling.

Autovacuum is tuned per table and stays enabled. The untuned default produced a 9.5s trigger p99 during
one five-minute run; per-table settings (ADR-016) brought the same load down to 57.6ms without
disabling maintenance. `runningMax` below the aggregate worker budget is expected for fast handlers;
the durable running count is bounded by the per-worker slot accounting (ADR-017).
