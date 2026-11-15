-- For GET /stats: jobs that finished in the last N minutes (throughput and latency percentiles).
CREATE INDEX jobs_finished_idx ON jobs (status, updated_at) WHERE status IN ('SUCCEEDED', 'DEAD');
