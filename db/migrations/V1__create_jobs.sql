CREATE TABLE jobs (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    type             TEXT        NOT NULL,                       -- which handler runs it
    payload          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    priority         INT         NOT NULL DEFAULT 0,             -- higher runs first
    -- TEXT + CHECK instead of a Postgres ENUM: same safety, but adding a status later is a
    -- one-line constraint change instead of ALTER TYPE, and JDBC needs no casts.
    status           TEXT        NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'DEAD', 'CANCELLED')),
    attempts         INT         NOT NULL DEFAULT 0,             -- incremented on every claim
    max_attempts     INT         NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),         -- delayed jobs and retry backoff
    lease_expires_at TIMESTAMPTZ,                                -- when a RUNNING job counts as abandoned
    worker_id        TEXT,
    idempotency_key  TEXT        UNIQUE,                         -- prevents duplicate enqueues
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The claim query's index. Partial, so it only holds claimable rows: finished jobs pile up
-- over time but never bloat it, and the scan already returns rows in claim order.
CREATE INDEX jobs_claim_idx ON jobs (priority DESC, run_at) WHERE status = 'PENDING';

-- Listing by status, e.g. GET /jobs?status=DEAD, newest first.
CREATE INDEX jobs_status_created_idx ON jobs (status, created_at DESC);
