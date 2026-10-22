# Distributed Job Scheduler

A durable, horizontally sharded job scheduler built on **Node.js + TypeScript + PostgreSQL**.
It supports one-off jobs, cron jobs, retries with backoff, dead letters, cooperative cancellation,
partition ownership with fencing tokens, and crash recovery without losing or duplicating jobs.
It also includes optional shared-token API auth, per-tenant active-job quotas, and a batched
retention tool.

No Redis, Kafka, RabbitMQ, BullMQ, or ORM is used. PostgreSQL is the only dependency.

Design and operations: [DESIGN.md](DESIGN.md) - [DECISIONS.md](DECISIONS.md) -
[BENCHMARKS.md](BENCHMARKS.md) - [HANDOFF.md](HANDOFF.md).

## Contents

- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Local development](#local-development)
- [API](#api)
- [Configuration](#configuration)
- [Tests](#tests)
- [Benchmarks and failure tests](#benchmarks-and-failure-tests)
- [Operational notes](#operational-notes)
- [Limitations](#limitations)

## Architecture

```text
                       +-------------------------+
  HTTP clients ------->|  Fastify API + React UI |---+
                       +-------------------------+   |
                                                     | create / read / cancel
                                                     v
                                          +---------------------+
                                          |  control PostgreSQL |
                                          |  jobs, shard_map,   |
                                          |  leases, registry   |
                                          +----------+----------+
                                                     ^
                          partition leases + fencing |
                                                     |
                         +---------------------------+---------------------------+
                         |                                                       |
                 +-------+--------+                                      +-------+--------+
                 |  scheduler-1   |                                      |  scheduler-2   |
                 | owner: 32 parts|                                      | owner: 32 parts|
                 +-------+--------+                                      +-------+--------+
                         |  fenced trigger insert                                |
           +-------------+------------------------------+-----------------------+
           |                                            |
           v                                            v
  +------------------+                          +------------------+
  |  shard-a (PG)    |                          |  shard-b (PG)    |
  |  runs, history,  |                          |  runs, history,  |
  |  run_keys, fence |                          |  run_keys, fence |
  +--------+---------+                          +--------+---------+
           ^     ^                                       ^     ^
           |     |                                       |     |
      +----+-----+----+                             +----+-----+----+
      | worker 1 / 2  |                             | worker 3 / 4  |
      | FOR UPDATE    |                             | FOR UPDATE    |
      | SKIP LOCKED   |                             | SKIP LOCKED   |
      +---------------+                             +---------------+
```

### Control plane

- **64 logical partitions** derived from `sha256(tenant_id + "\0" + job_id)` (first 8 bytes mod 64).
  Every job therefore has a stable partition independent of process or host.
- `shard_map` maps each logical partition to a physical shard. It is data, not code; partitions can be
  moved without rehashing job IDs.
- `partition_leases` assigns partitions to schedulers. Leases expire, and every acquisition increments
  a **monotonic fencing token**.
- `scheduler_registry` tracks live schedulers so ownership is split with fair share
  `ceil(64 / active_schedulers)` and rebalanced when instances join or leave.
- Jobs are stored once in `jobs`; the scheduler fetches due rows only for partitions it currently owns.
- One trigger row per `(job_id, scheduled_time)` is written durably before the control-plane job is advanced.

### Data plane

Each shard owns durable run state:

| Table | Purpose |
|---|---|
| `runs` | queued/running work, leases, attempts, cancellation flag |
| `run_keys` | unique `(job_id, scheduled_time)` idempotency ledger |
| `run_history` | terminal runs (succeeded/dead/cancelled) |
| `partition_fence` | scheduler fence token, validated inside the trigger insert |

Workers claim with `FOR UPDATE SKIP LOCKED`, renew leases by heartbeat, run the handler with a
concurrency limit, and submit fenced completion batches. Expired leases are reaped and requeued
(or dead-lettered when attempts are exhausted).

### Exactly-once trigger, at-least-once execution

The trigger insert validates the scheduler's shard-side fence and writes `run_keys` + `runs` in one
statement. `ON CONFLICT` on the idempotency key makes duplicate scheduler deliveries safe. A stale
scheduler cannot insert after ownership moves.

Execution is at-least-once: a worker can complete side effects and die before recording completion,
so job handlers must be idempotent. Completion is fenced by `(worker_id, lease_token)`; a stale
worker cannot overwrite a recovered run.

### Failure behavior

- Scheduler crash: surviving scheduler acquires expired partitions and re-registers their fences.
- Worker crash: leases expire; the reaper requeues non-terminal runs with the next attempt.
- Shard restart: trigger/completion requests fail transiently; clients retry; no job is advanced
  unless its trigger was durably written. Connection-pool error handlers prevent an idle client
  disconnect from crashing the service.
- Shard map change: schedulers reload the map and re-register fences on the new target shard; triggers
  then route to the new shard.
- Cancellation: the API marks the job cancelled, cancels queued runs on every shard, and flags
  running runs for heartbeat-driven abort. This fans out to old shards after a partition move.
- Retention: terminal history and old terminal jobs can be deleted in bounded batches with
  `npm run retention`; this is a manual operational tool, not an automatic policy.

## Quick start

Prerequisite: Docker Desktop / Docker Engine with Compose V2.

```bash
docker build -t jobsched-app:local .
docker compose up -d
```

Open <http://127.0.0.1:3000> for the UI, or use the API directly.

Ports are intentionally non-default because a native PostgreSQL may already use 5432:

| Service | Host port |
|---|---|
| API / UI | 3000 |
| control PostgreSQL | 55432 |
| shard-a PostgreSQL | 55433 |
| shard-b PostgreSQL | 55434 |

`docker compose ps` should show all services healthy. App health endpoints use `127.0.0.1`
inside the container (not `localhost`, which may resolve to IPv6 first).

The one-shot `migrate` service applies migrations and inserts missing `shard_map` rows. It never
overwrites an existing mapping, so operator shard moves survive a deployment.

Create a job:

```bash
curl -s 127.0.0.1:3000/jobs -H 'content-type: application/json' -d '{
  "tenantId": "demo",
  "name": "hello",
  "scheduleType": "once",
  "scheduledAt": "2030-01-01T00:00:00Z",
  "payload": {"sleepMs": 100}
}'
```

For an immediate job, omit `scheduledAt` (defaults to now) or pass a past timestamp.

## Local development

```powershell
docker compose up -d control-db shard-a shard-b
npm ci
npm --prefix web ci
$env:CONTROL_DATABASE_URL="postgres://jobsched:jobsched@127.0.0.1:55432/jobsched_control"
$env:SHARD_DSN_MAP='{"shard-a":"postgres://jobsched:jobsched@127.0.0.1:55433/jobsched_shard","shard-b":"postgres://jobsched:jobsched@127.0.0.1:55434/jobsched_shard"}'
$env:SHARD_IDS="shard-a,shard-b"
npx tsx src/migrate.ts
```

Run services in separate terminals with the same three variables exported:

```powershell
npm run dev:api
npm run dev:scheduler
npm run dev:worker
```

The app does not load `.env` automatically. Build `web/dist` (`npm --prefix web run build`) if you
need the UI on `http://127.0.0.1:3000`; otherwise the API still serves all endpoints.

Build and typecheck:

```bash
npm run build
npm run typecheck
npm run lint
```

## API

| Method | Path | Description |
|---|---|---|
| GET | `/health` | API health and configured shards |
| GET | `/metrics` | Prometheus metrics |
| GET | `/jobs?tenantId=&limit=` | List recent jobs |
| GET | `/jobs/:id` | Get one job |
| POST | `/jobs` | Create a one-off or cron job |
| POST | `/jobs/batch` | Create up to 500 jobs in one transaction |
| POST | `/jobs/:id/cancel` | Cancel job; cancels queued runs and flags running runs |
| GET | `/jobs/:id/runs?limit=` | Run history for a job |
| GET | `/runs?limit=` | Recent runs across shards |
| GET | `/dead-letters?limit=` | Recent dead-lettered runs |
| GET | `/shards` | Shard map and queue depth |

Create-job body:

```json
{
  "tenantId": "acme",
  "name": "nightly-report",
  "scheduleType": "cron",
  "cronExpression": "0 2 * * *",
  "timezone": "America/New_York",
  "misfirePolicy": "run_once",
  "maxAttempts": 5,
  "payload": {"report": "daily"}
}
```

For `once`, use `scheduledAt` instead of cron fields. A dispatched one-off job has status
`triggered`; cron jobs stay `active` with an advanced `next_run_at`.

When queue depth exceeds `QUEUE_BACKPRESSURE_THRESHOLD`, create endpoints return HTTP 429 with
`Retry-After: 1`.

If `API_AUTH_TOKEN` is set, `/jobs`, `/jobs/*`, `/runs`, `/dead-letters`, and `/shards` require the
token in `Authorization: Bearer <token>` or `X-API-Key: <token>`. `/health`, `/metrics`, and the
static UI remain unauthenticated. Missing or wrong credentials return 401.

If `TENANT_MAX_ACTIVE_JOBS` is greater than 0, job creation counts the tenant's
`active`/`paused`/`triggered` jobs under a per-tenant advisory lock and returns 429 when the limit
would be exceeded. `0` disables the quota.

## Configuration

All settings are environment variables; see `.env.example` for the complete list and defaults.

Key values:

| Variable | Default | Meaning |
|---|---|---|
| `WORKER_CONCURRENCY` | 64 | concurrent handler executions per worker |
| `CLAIM_BATCH_SIZE` | 100 | max runs claimed per shard per loop |
| `WORKER_LEASE_TTL_MS` | 5000 | run lease duration |
| `WORKER_HEARTBEAT_MS` | 1500 | lease renewal interval |
| `SCHEDULER_LEASE_TTL_MS` | 10000 | partition lease duration |
| `SCHEDULER_RENEW_MS` | 2000 | rebalance/lease renewal interval |
| `TRIGGER_BATCH_SIZE` | 500 | due jobs per trigger tick |
| `QUEUE_BACKPRESSURE_THRESHOLD` | 20000 | queued+running threshold for 429 |
| `API_AUTH_TOKEN` | unset | shared bearer/`X-API-Key` token; unset disables API auth |
| `TENANT_MAX_ACTIVE_JOBS` | 0 | max active/paused/triggered jobs per tenant; 0 disables |
| `RETENTION_DAYS` | 30 | default age for terminal jobs and `run_history` in the retention tool |
| `RETENTION_RUN_KEYS_DAYS` | max(3x days, 90) | default age for `run_keys` (idempotency ledger) |
| `DB_LATENCY_MS` | 0 | test-only delay wrapper on every DB operation |

## Tests

Tests recreate isolated `*_test` databases, apply migrations, and exercise concurrency/fencing
against the Docker PostgreSQL instances.

```bash
docker compose up -d control-db shard-a shard-b
npm test
```

Coverage includes:

- partition hashing and multi-shard routing
- duplicate trigger suppression and stale fence rejection
- lease expiry/recovery and fenced completion; expired runs dead-letter with the worker ID preserved
- retries, backoff exhaustion, dead letters, and attempt preservation
- cancellation races (queued, running, recovery) and cross-shard cancellation after a partition move
- worker concurrency accounting under slow completions (durable running rows never exceed the budget)
- cron DST/gap/overlap and misfire behavior
- scheduler fairness, restart recovery, map changes, and two-scheduler coordination

## Benchmarks and failure tests

See [BENCHMARKS.md](BENCHMARKS.md) for environment, methodology, artifacts, and measured results.
For the audit-ready report with verification status, comparability analysis, and detailed methodology,
see [docs/benchmark-report.md](docs/benchmark-report.md),
[docs/benchmark-methodology.md](docs/benchmark-methodology.md), and
[docs/benchmark-results.csv](docs/benchmark-results.csv).

Short version on the local Docker stack:

- 5-minute run with tuned autovacuum: **185,380 jobs**, **0 lost**, **0 duplicate triggers**,
  **617.8 submitted/s** during the 300s window, **611.0 completed/s** including a 3.3s drain,
  trigger p99 **57.6ms**, e2e p99 **190.2ms**.
- 5-minute default-autovacuum run (before tuning): 179,160 jobs, 0 lost/duplicates, but trigger p99
  **9.5s** during an autovacuum pass. This regression motivated the checked-in per-table settings.
- 30-second run at 600 offered/s: **591.5 completed/s**, trigger p99 65.9ms.
- Single shard: **~2.7k jobs/s** under backlog, so a two-shard deployment has substantial headroom
  over the 600/s target.

Failure scenarios:

```bash
npx tsx src/tools/chaos.ts
```

The chaos tool kills a worker, kills a scheduler, restarts a shard, and runs a latency-injected worker.
It writes results to `benchmarks/chaos-*.json`.

Load generator:

```bash
docker compose run --rm --no-deps loadgen node dist/tools/loadgen.js \
  --rate 600 --duration 300 --timeout 300 --prefix my-run
```

The generator verifies distinct `run_keys`, duplicate trigger rows, lost jobs, retry/dead counts,
queue depth, and trigger/end-to-end latency percentiles, then writes `benchmarks/loadgen-*.json`.
Its summary separates `submitWindowPerSec`/`acceptedPerSec`, `offeredPerSec`, and
`completedPerSec` (which includes drain) so a backlogged run is not mistaken for steady-state
capacity.

For resource and PostgreSQL samples during a run, use the lightweight sampler in a separate shell:

```powershell
pwsh -File scripts/capture-metrics.ps1 -Output benchmarks/metrics.jsonl -IntervalSec 20
```

Avoid heavy `docker stats`/CIM/PostgreSQL sampling in the same process as the load generator on a
laptop Docker Desktop host; it can distort the run.

## Operational notes

- Database durability stays on (`fsync=on`, `synchronous_commit=on`, `full_page_writes=on`).
- Autovacuum stays enabled. Per-table overrides for `jobs`, `runs`, `run_keys`, and `run_history` are
  applied by migrations, with `naptime=5s`, 3 workers on control, 4 per shard, and logging for
  autovacuum runs over 1s. A five-minute run with these settings held trigger p99 at 57.6ms.
- Scale schedulers and workers independently; schedulers rebalance partitions automatically.
- To move partitions, update `shard_map`; schedulers reload it on their next rebalance and re-register
  fences on the target shard. Cancellation and run listing fan out across all shards, but historical
  run data is not migrated to the new shard. Drain a live partition before moving it if history must
  remain on the target shard.
- `shard_map` seeding is insert-only; deployments do not reset operator changes.
- Monitor `jobsched_queue_depth`, `jobsched_trigger_latency_seconds`, worker claim/completion latency,
  retries, dead letters, lease reaps, and pool stats from `/metrics`.
- Run retention in bounded batches, preferably from an external scheduler:
  `npm run retention -- --days 30 --run-keys-days 90 --batch 5000`; use `--dry-run` first. Deleting
  `run_keys` too aggressively weakens duplicate suppression for very old scheduled occurrences.
- Set `API_AUTH_TOKEN` and `TENANT_MAX_ACTIVE_JOBS` for any deployment exposed beyond localhost. The
  token is a single shared secret, not a full identity system; terminate TLS in front of the API.

## Limitations

- Authentication is an optional single shared token, with no per-tenant identity, authorization,
  key rotation, or TLS termination. The API/UI is a reference implementation, not a hardened
  multi-tenant control plane.
- Tenant quotas cap active job count at creation time; they are not request-rate limits or executor
  concurrency quotas.
- Handlers run in the worker process; there is no container-per-job isolation.
- Execution is at-least-once, not exactly-once; handlers must be idempotent.
- `jobs` and `run_history` grow unless the manual retention tool is scheduled; there is no archival
  or time-partitioned history.
- Shard map changes affect only new triggers; historical runs remain on their original shard. There is
  no live data-migration tool.
- Benchmark projections to AWS are estimates, not measurements. See [BENCHMARKS.md](BENCHMARKS.md).

