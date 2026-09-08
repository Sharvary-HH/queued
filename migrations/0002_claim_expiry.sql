-- The reaper looks for claims that have outlived their visibility timeout. The
-- obvious way to write that is
--
--     WHERE state = 'claimed'
--       AND claimed_at + make_interval(secs => visibility_timeout_seconds) < now()
--
-- which cannot use an index on claimed_at: the timeout is per-row, so the
-- comparison touches two columns of the same row and the planner has to
-- sequential-scan to evaluate it. That is fine on a small table and quietly
-- becomes the most expensive thing in the system on a large one, and the reaper
-- runs every few seconds forever.
--
-- Materialising the deadline at claim time turns it into a plain range scan.

ALTER TABLE jobs ADD COLUMN claim_expires_at timestamptz;

ALTER TABLE jobs ADD CONSTRAINT jobs_claim_expiry CHECK (
    (state <> 'claimed') OR (claim_expires_at IS NOT NULL)
);

DROP INDEX jobs_reap_idx;

CREATE INDEX jobs_reap_idx ON jobs (claim_expires_at)
    WHERE state = 'claimed';
