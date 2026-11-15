import { api } from './api';
import { DeadLetterQueue } from './DeadLetterQueue';
import { EnqueueForm } from './EnqueueForm';
import { JobTable } from './JobTable';
import { StatsPanel } from './StatsPanel';
import { usePolling } from './usePolling';

export function App() {
  // The API refreshes its stats snapshot every 5 s, so polling faster than 2 s gains nothing.
  const { data: stats, error, refresh } = usePolling(api.stats, 2000);

  return (
    <div className="page">
      <header className="page-header">
        <h1>Job Queue</h1>
        <span className={`connection ${error ? 'connection-down' : 'connection-up'}`}>
          <span aria-hidden="true">{error ? '✕' : '●'}</span> {error ? 'API unreachable' : 'Live'}
        </span>
      </header>

      <main>
        <StatsPanel stats={stats} error={error} />
        <EnqueueForm onEnqueued={refresh} />
        <JobTable onChange={refresh} />
        <DeadLetterQueue onChange={refresh} />
      </main>
    </div>
  );
}
