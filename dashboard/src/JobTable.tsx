import { useState } from 'react';
import { api, JOB_STATUSES, type Job, type JobStatus } from './api';
import { relativeTime, shortId } from './format';
import { StatusBadge } from './StatusBadge';
import { usePolling } from './usePolling';

const FILTERS: (JobStatus | undefined)[] = [undefined, ...JOB_STATUSES];

function filterLabel(status?: JobStatus) {
  return status ? status.charAt(0) + status.slice(1).toLowerCase() : 'All';
}

/** The 50 most recently created jobs, refreshed every 2 seconds. */
export function JobTable({ onChange }: { onChange: () => void }) {
  const [status, setStatus] = useState<JobStatus>();
  const { data: jobs, error, refresh } = usePolling(() => api.jobs(status), 2000, [status]);
  const [actionError, setActionError] = useState<string>();

  const cancel = async (job: Job) => {
    setActionError(undefined);
    try {
      await api.cancel(job.id);
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    }
    refresh();
    onChange();
  };

  return (
    <section aria-labelledby="jobs-heading">
      <div className="section-header">
        <h2 id="jobs-heading">Recent jobs</h2>
        <div className="filters" role="group" aria-label="Filter by status">
          {FILTERS.map((f) => (
            <button
              key={f ?? 'all'}
              className={`chip ${f === status ? 'chip-selected' : ''}`}
              aria-pressed={f === status}
              onClick={() => setStatus(f)}
            >
              {filterLabel(f)}
            </button>
          ))}
        </div>
      </div>

      {(error || actionError) && <p className="error-text">{actionError ?? `Could not load jobs: ${error}`}</p>}

      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th scope="col">Job</th>
              <th scope="col">Type</th>
              <th scope="col">Status</th>
              <th scope="col" className="num">Attempts</th>
              <th scope="col" className="num">Priority</th>
              <th scope="col">Worker</th>
              <th scope="col">Created</th>
              <th scope="col">Last error</th>
              <th scope="col"><span className="visually-hidden">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            {jobs?.map((job) => (
              <tr key={job.id}>
                <td className="mono" title={job.id}>{shortId(job.id)}</td>
                <td>{job.type}</td>
                <td><StatusBadge job={job} /></td>
                <td className="num">{job.attempts} / {job.maxAttempts}</td>
                <td className="num">{job.priority}</td>
                <td className="mono muted" title={job.workerId ?? undefined}>{job.workerId ?? '–'}</td>
                <td className="muted nowrap" title={job.createdAt}>
                  {relativeTime(job.createdAt)}
                  {new Date(job.runAt) > new Date() && job.status === 'PENDING' && (
                    <div className="subtle">runs {relativeTime(job.runAt)}</div>
                  )}
                </td>
                <td className="error-cell" title={job.lastError ?? undefined}>{job.lastError ?? ''}</td>
                <td>
                  {job.status === 'PENDING' && (
                    <button className="button-quiet" onClick={() => cancel(job)}>Cancel</button>
                  )}
                </td>
              </tr>
            ))}
            {jobs?.length === 0 && (
              <tr>
                <td colSpan={9} className="empty">No {status ? filterLabel(status).toLowerCase() : ''} jobs yet.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
