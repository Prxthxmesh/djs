param(
  [string]$Output = "benchmarks/metrics.jsonl",
  [int]$IntervalSec = 20
)

$ErrorActionPreference = 'Continue'
$containers = @(
  'jobsched-api', 'jobsched-control-db', 'jobsched-shard-a', 'jobsched-shard-b',
  'jobsched-worker-1', 'jobsched-worker-2', 'jobsched-worker-3', 'jobsched-worker-4',
  'jobsched-scheduler-1', 'jobsched-scheduler-2'
)

$pgSql = @'
SELECT json_build_object(
  'at', now(),
  'database', (SELECT row_to_json(d) FROM (SELECT datname, numbackends, xact_commit, xact_rollback, blks_read, blks_hit, tup_inserted, tup_updated, tup_deleted, deadlocks, temp_files, temp_bytes FROM pg_stat_database WHERE datname=current_database()) d),
  'tables', (SELECT coalesce(json_agg(row_to_json(t)), '[]'::json) FROM (SELECT relname, n_live_tup, n_dead_tup, n_tup_ins, n_tup_upd, n_tup_del, autovacuum_count, autoanalyze_count, last_autovacuum, last_autoanalyze FROM pg_stat_user_tables WHERE relname IN ('jobs','runs','run_history','run_keys','partition_fence')) t),
  'bgwriter', (SELECT row_to_json(b) FROM (SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time, buffers_checkpoint, buffers_clean, maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc FROM pg_stat_bgwriter) b),
  'vacuum', (SELECT coalesce(json_agg(row_to_json(v)), '[]'::json) FROM (SELECT pid, relid::regclass::text AS relation, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples FROM pg_stat_progress_vacuum) v),
  'activity', (SELECT row_to_json(a) FROM (SELECT count(*)::int AS total, count(*) FILTER (WHERE state='active')::int AS active, count(*) FILTER (WHERE wait_event_type='Lock')::int AS waiting_on_lock FROM pg_stat_activity WHERE datname=current_database()) a)
)
'@

$targets = @(
  @('control', 'jobsched-control-db', 'jobsched_control'),
  @('shard-a', 'jobsched-shard-a', 'jobsched_shard'),
  @('shard-b', 'jobsched-shard-b', 'jobsched_shard')
)

while ($true) {
  $entry = [ordered]@{ at = (Get-Date).ToUniversalTime().ToString('o') }

  try {
    $cpu = (Get-CimInstance Win32_PerfFormattedData_PerfOS_Processor -Filter "Name='_Total'").PercentProcessorTime
    $disk = Get-CimInstance Win32_PerfFormattedData_PerfDisk_PhysicalDisk -Filter "Name='_Total'"
    $entry.host = [ordered]@{
      cpuPercent          = [double]$cpu
      diskPercentTime     = [double]$disk.PercentDiskTime
      diskReadBytesPerSec = [double]$disk.DiskReadBytesPerSec
      diskWriteBytesPerSec = [double]$disk.DiskWriteBytesPerSec
    }
  } catch {
    $entry.host = @{ error = "$_" }
  }

  try {
    $stats = & docker stats --no-stream --format "{{json .}}" $containers 2>$null | ForEach-Object { $_ | ConvertFrom-Json }
    $entry.containers = @($stats)
  } catch {
    $entry.containers = @{ error = "$_" }
  }

  $pg = [ordered]@{}
  foreach ($t in $targets) {
    try {
      $json = & docker exec $t[1] psql -U jobsched -d $t[2] -tAc $pgSql
      $pg[$t[0]] = $json | ConvertFrom-Json
    } catch {
      $pg[$t[0]] = @{ error = "$_" }
    }
  }
  $entry.pg = $pg

  try {
    ($entry | ConvertTo-Json -Depth 8 -Compress) | Add-Content -LiteralPath $Output
  } catch {
    Write-Error "failed to write sample: $_"
  }

  Start-Sleep -Seconds $IntervalSec
}
