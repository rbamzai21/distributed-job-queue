-- A fresh random id per claim, used as a fencing token. Every heartbeat and finishing update must
-- present the lease_id it claimed with. Once a lease is reaped or released, a worker still holding
-- the old id (slow, paused for GC, or partitioned) can no longer extend or finish the job, even if
-- the same job has since been reclaimed, including by that same worker.
ALTER TABLE jobs ADD COLUMN lease_id UUID;

-- The reaper's index: only RUNNING rows, ordered by when their lease runs out.
CREATE INDEX jobs_lease_expiry_idx ON jobs (lease_expires_at) WHERE status = 'RUNNING';
