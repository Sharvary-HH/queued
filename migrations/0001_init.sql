-- Core schema: jobs, their attempt history, and cron definitions.

CREATE TYPE job_state AS ENUM (
    'pending',
    'claimed',
    'succeeded',
    'failed',
    'dead'
);

CREATE TABLE jobs (
    id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue  text      NOT NULL DEFAULT 'default',
    kind   text      NOT NULL,
    payload jsonb    NOT NULL DEFAULT '{}'::jsonb,
    state  job_state NOT NULL DEFAULT 'pending',

    -- Lower runs first. Left as a plain int rather than an enum so callers can
    -- slot new levels in between existing ones without a migration.
    priority int NOT NULL DEFAULT 100,

    -- The job is invisible to the claim query until now() >= run_at. This is
    -- what gives us both delayed jobs and retry backoff for free: failing a job
    -- just pushes run_at into the future.
    run_at timestamptz NOT NULL DEFAULT now(),

    attempt      int NOT NULL DEFAULT 0,
    max_attempts int NOT NULL DEFAULT 5,

    claimed_at timestamptz,
    claimed_by text,

    -- How long a claim is honoured before the reaper may hand the job to
    -- somebody else. Per-job rather than global because a 200ms webhook and a
    -- 10 minute video transcode do not want the same number.
    visibility_timeout_seconds int NOT NULL DEFAULT 60,

    last_error text,

    -- Enqueue-side dedupe. NULL means "no dedupe"; Postgres allows any number
    -- of NULLs in a unique index, so the common case costs nothing.
    idempotency_key text UNIQUE,

    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT jobs_attempt_nonneg CHECK (attempt >= 0),
    CONSTRAINT jobs_max_attempts_positive CHECK (max_attempts >= 1),
    CONSTRAINT jobs_visibility_positive CHECK (visibility_timeout_seconds >= 1),
    -- A claimed job must say who holds it and since when; anything else and the
    -- reaper has no way to decide whether the claim has expired.
    CONSTRAINT jobs_claim_fields CHECK (
        (state <> 'claimed') OR (claimed_at IS NOT NULL AND claimed_by IS NOT NULL)
    )
);

-- The claim query's index. Partial, not full, and the distinction matters:
--
--   * Only pending rows are ever candidates for claiming. Succeeded jobs are
--     the overwhelming majority of a healthy queue's table - a busy queue is
--     mostly history - and indexing them buys nothing while making the index
--     large enough to fall out of cache.
--   * The index therefore stays roughly the size of the backlog rather than the
--     size of the table, so it tends to stay resident in shared_buffers even
--     when the table is tens of gigabytes.
--   * Writes get cheaper too. A job's life is pending -> claimed -> succeeded;
--     with a full index every one of those transitions rewrites an index entry.
--     With the partial index, leaving 'pending' deletes the entry and the row
--     never comes back, so completed jobs stop costing anything on write.
--
-- Column order follows the query: equality on queue first, then the two ORDER BY
-- columns in order, so the planner can walk the index and stop at LIMIT instead
-- of sorting.
CREATE INDEX jobs_claim_idx ON jobs (queue, priority, run_at)
    WHERE state = 'pending';

-- The reaper's index: expired claims only. Same reasoning - claimed rows are a
-- tiny slice of the table at any moment.
CREATE INDEX jobs_reap_idx ON jobs (claimed_at)
    WHERE state = 'claimed';

-- Dashboard listing: newest first, filtered by state and queue.
CREATE INDEX jobs_listing_idx ON jobs (state, queue, id DESC);

-- Append-only execution history. One row per (job, attempt) try.
--
-- Deliberately NOT unique on (job_id, attempt): the double-execution test reads
-- this table to find out what really happened, and a constraint that made the
-- bad state unrepresentable would hide the very thing being measured.
CREATE TABLE job_attempts (
    id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id   bigint NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt  int    NOT NULL,
    worker_id text  NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    error       text
);

CREATE INDEX job_attempts_job_idx ON job_attempts (job_id, attempt);

CREATE TABLE recurring_jobs (
    id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL UNIQUE,
    cron text NOT NULL,

    queue   text  NOT NULL DEFAULT 'default',
    kind    text  NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,

    enabled bool NOT NULL DEFAULT true,

    last_run_at timestamptz,
    next_run_at timestamptz NOT NULL,

    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX recurring_jobs_due_idx ON recurring_jobs (next_run_at)
    WHERE enabled;

-- updated_at is maintained here rather than in every UPDATE in the store. The
-- dashboard shows it, and one forgotten SET in one query would make it quietly
-- wrong.
CREATE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_touch_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

CREATE TRIGGER recurring_jobs_touch_updated_at
    BEFORE UPDATE ON recurring_jobs
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
