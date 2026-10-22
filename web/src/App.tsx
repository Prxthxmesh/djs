import { FormEvent, useEffect, useState } from 'react';

type Job = {
  id: string;
  tenant_id: string;
  name: string;
  partition_id: number;
  schedule_type: 'once' | 'cron';
  cron_expression: string | null;
  timezone: string;
  next_run_at: string | null;
  misfire_policy: string;
  status: string;
  max_attempts: number;
  shardId: string | null;
};

type Run = {
  id: string;
  job_id: string;
  status: string;
  attempt: number;
  max_attempts: number;
  scheduled_time: string;
  triggered_at: string;
  started_at: string | null;
  finished_at: string | null;
  last_error: string | null;
  worker_id: string | null;
  shard_id?: string;
};

type Tab = 'jobs' | 'runs' | 'dead';

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  return (await res.json()) as T;
}

function fmt(value: string | null | undefined): string {
  if (!value) return '-';
  return new Date(value).toLocaleString();
}

function duration(run: Run): string {
  if (!run.started_at || !run.finished_at) return '-';
  return `${new Date(run.finished_at).getTime() - new Date(run.started_at).getTime()} ms`;
}

export default function App() {
  const [tab, setTab] = useState<Tab>('jobs');
  const [jobs, setJobs] = useState<Job[]>([]);
  const [runs, setRuns] = useState<Run[]>([]);
  const [dead, setDead] = useState<Run[]>([]);
  const [selected, setSelected] = useState<Job | null>(null);
  const [jobRuns, setJobRuns] = useState<Run[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function refresh() {
    try {
      const [j, r, d] = await Promise.all([
        getJson<{ jobs: Job[] }>('/jobs?limit=200'),
        getJson<{ runs: Run[] }>('/runs?limit=200'),
        getJson<{ deadLetters: Run[] }>('/dead-letters?limit=200'),
      ]);
      setJobs(j.jobs);
      setRuns(r.runs);
      setDead(d.deadLetters);
      setError(null);
    } catch (err) {
      setError(String(err));
    }
  }

  useEffect(() => {
    void refresh();
    const t = setInterval(() => void refresh(), 3000);
    return () => clearInterval(t);
  }, []);

  useEffect(() => {
    if (!selected) return;
    getJson<{ runs: Run[] }>(`/jobs/${selected.id}/runs?limit=100`)
      .then((r) => setJobRuns(r.runs))
      .catch((err) => setError(String(err)));
  }, [selected]);

  async function cancelJob(id: string) {
    setLoading(true);
    try {
      const res = await fetch(`/jobs/${id}/cancel`, { method: 'POST' });
      const body = (await res.json()) as { message?: string; cancelledQueued?: number; flaggedRunning?: number };
      setNotice(body.message ?? `cancelled: ${body.cancelledQueued ?? 0} queued, ${body.flaggedRunning ?? 0} running flagged`);
      await refresh();
    } catch (err) {
      setError(String(err));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="shell">
      <header>
        <div>
          <h1>Job Scheduler</h1>
          <p className="muted">Durable runs across two PostgreSQL shards</p>
        </div>
        <div className="stats">
          <span>{jobs.length} jobs</span>
          <span>{runs.length} recent runs</span>
          <span className={dead.length ? 'dangerText' : ''}>{dead.length} dead letters</span>
        </div>
      </header>

      {error && <div className="banner error">{error}</div>}
      {notice && <div className="banner">{notice}</div>}

      <nav>
        <button className={tab === 'jobs' ? 'active' : ''} onClick={() => setTab('jobs')}>
          Jobs
        </button>
        <button className={tab === 'runs' ? 'active' : ''} onClick={() => setTab('runs')}>
          Run history
        </button>
        <button className={tab === 'dead' ? 'active' : ''} onClick={() => setTab('dead')}>
          Dead letters
        </button>
      </nav>

      {tab === 'jobs' && (
        <div className="grid">
          <CreateForm onCreated={() => void refresh()} />
          <section className="panel">
            <h2>Jobs</h2>
            <table>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Schedule</th>
                  <th>Next</th>
                  <th>Status</th>
                  <th>Shard</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={job.id} className={selected?.id === job.id ? 'selected' : ''} onClick={() => setSelected(job)}>
                    <td>
                      <strong>{job.name}</strong>
                      <div className="muted small">{job.tenant_id}</div>
                    </td>
                    <td>{job.schedule_type === 'cron' ? `${job.cron_expression} (${job.timezone})` : fmt(job.next_run_at)}</td>
                    <td>{fmt(job.next_run_at)}</td>
                    <td>
                      <span className={`pill ${job.status}`}>{job.status}</span>
                    </td>
                    <td>{job.shardId ?? '-'}</td>
                    <td>
                      <button
                        className="ghost"
                        disabled={loading || (job.status !== 'active' && job.status !== 'triggered')}
                        onClick={(e) => {
                          e.stopPropagation();
                          void cancelJob(job.id);
                        }}
                      >
                        Cancel
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {jobs.length === 0 && <p className="muted">No jobs yet. Create one on the left.</p>}
          </section>

          {selected && (
            <section className="panel wide">
              <h2>
                Runs for {selected.name} <span className="muted small">{selected.id}</span>
              </h2>
              <RunTable runs={jobRuns} />
            </section>
          )}
        </div>
      )}

      {tab === 'runs' && (
        <section className="panel">
          <h2>Recent durable runs</h2>
          <RunTable runs={runs} />
        </section>
      )}

      {tab === 'dead' && (
        <section className="panel">
          <h2>Dead letters</h2>
          <p className="muted">Terminal runs that exhausted retries. The final error and attempt count are preserved.</p>
          <RunTable runs={dead} />
        </section>
      )}
    </div>
  );
}

function RunTable({ runs }: { runs: Run[] }) {
  return (
    <table>
      <thead>
        <tr>
          <th>Job</th>
          <th>Status</th>
          <th>Attempt</th>
          <th>Scheduled</th>
          <th>Duration</th>
          <th>Worker</th>
          <th>Error</th>
        </tr>
      </thead>
      <tbody>
        {runs.map((run) => (
          <tr key={run.id}>
            <td>
              <div className="mono small">{run.job_id}</div>
              <div className="muted small">{run.shard_id ?? ''}</div>
            </td>
            <td>
              <span className={`pill ${run.status}`}>{run.status}</span>
            </td>
            <td>
              {run.attempt}/{run.max_attempts}
            </td>
            <td>{fmt(run.scheduled_time)}</td>
            <td>{duration(run)}</td>
            <td className="small">{run.worker_id ?? '-'}</td>
            <td className="errorCell small">{run.last_error ?? ''}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function CreateForm({ onCreated }: { onCreated: () => void }) {
  const [tenantId, setTenantId] = useState('demo');
  const [name, setName] = useState('nightly-report');
  const [scheduleType, setScheduleType] = useState<'once' | 'cron'>('once');
  const [cronExpression, setCronExpression] = useState('*/5 * * * *');
  const [timezone, setTimezone] = useState('UTC');
  const [scheduledAt, setScheduledAt] = useState('');
  const [misfirePolicy, setMisfirePolicy] = useState<'skip' | 'run_once'>('skip');
  const [maxAttempts, setMaxAttempts] = useState(5);
  const [payload, setPayload] = useState('{}');
  const [message, setMessage] = useState<string | null>(null);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setMessage(null);
    let parsedPayload: Record<string, unknown>;
    try {
      parsedPayload = JSON.parse(payload) as Record<string, unknown>;
    } catch {
      setMessage('Payload must be valid JSON');
      return;
    }
    const body = {
      tenantId,
      name,
      scheduleType,
      cronExpression: scheduleType === 'cron' ? cronExpression : undefined,
      timezone,
      scheduledAt: scheduleType === 'once' && scheduledAt ? new Date(scheduledAt).toISOString() : undefined,
      misfirePolicy,
      maxAttempts,
      payload: parsedPayload,
    };
    const res = await fetch('/jobs', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = (await res.json()) as { error?: string; detail?: unknown };
    if (!res.ok) {
      setMessage(`Create failed: ${data.error ?? res.status} ${data.detail ? JSON.stringify(data.detail) : ''}`);
      return;
    }
    setMessage('Job created');
    onCreated();
  }

  return (
    <section className="panel">
      <h2>Create job</h2>
      <form onSubmit={(e) => void submit(e)} className="form">
        <label>
          Tenant
          <input value={tenantId} onChange={(e) => setTenantId(e.target.value)} required />
        </label>
        <label>
          Name
          <input value={name} onChange={(e) => setName(e.target.value)} required />
        </label>
        <label>
          Type
          <select value={scheduleType} onChange={(e) => setScheduleType(e.target.value as 'once' | 'cron')}>
            <option value="once">One-off</option>
            <option value="cron">Cron</option>
          </select>
        </label>
        {scheduleType === 'cron' ? (
          <>
            <label>
              Cron expression
              <input value={cronExpression} onChange={(e) => setCronExpression(e.target.value)} required />
            </label>
            <label>
              Timezone
              <input value={timezone} onChange={(e) => setTimezone(e.target.value)} required />
            </label>
            <label>
              Misfire policy
              <select value={misfirePolicy} onChange={(e) => setMisfirePolicy(e.target.value as 'skip' | 'run_once')}>
                <option value="skip">skip</option>
                <option value="run_once">run once</option>
              </select>
            </label>
          </>
        ) : (
          <label>
            Run at (empty means now)
            <input type="datetime-local" value={scheduledAt} onChange={(e) => setScheduledAt(e.target.value)} />
          </label>
        )}
        <label>
          Max attempts
          <input type="number" min={1} max={100} value={maxAttempts} onChange={(e) => setMaxAttempts(Number(e.target.value))} />
        </label>
        <label>
          Payload (JSON)
          <textarea rows={4} value={payload} onChange={(e) => setPayload(e.target.value)} />
        </label>
        <button type="submit">Create job</button>
        {message && <p className="small muted">{message}</p>}
      </form>
    </section>
  );
}
