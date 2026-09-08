-- Wake-on-enqueue.
--
-- Polling alone means a job sits in the table for up to one poll interval
-- before anyone looks at it. Dropping the interval to fix that turns idle
-- workers into a constant stream of queries against the database that exists to
-- serve the busy ones. NOTIFY closes the gap without the queries: an idle
-- worker blocks on its connection instead of asking every 100ms.
--
-- Statement-level, not row-level, and that matters here: EnqueueMany goes over
-- COPY, and a FOR EACH ROW trigger would emit 50,000 notifications for one
-- 50,000-row load. The workers only need to be told "there is something to
-- look at", not what it was, so one notification per statement is exactly
-- enough.
--
-- No payload for the same reason. Sending the queue name would let a worker
-- ignore wakeups for queues it does not serve, but a statement can touch rows
-- across several queues, so the payload would have to be a list, and the honest
-- version of that is "wake up and run your normal claim query". A spurious
-- wakeup costs one indexed query that returns nothing.
--
-- NOTIFY is delivered on transaction commit, so a worker never wakes up to find
-- the row it was told about is not visible yet.

CREATE FUNCTION notify_jobs_pending() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('jobs_pending', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_notify_pending
    AFTER INSERT ON jobs
    FOR EACH STATEMENT EXECUTE FUNCTION notify_jobs_pending();

-- Retries and reclaims put existing rows back into 'pending' rather than
-- inserting, so they need their own trigger. This one is row-level because a
-- statement-level trigger cannot see whether the state actually changed, and
-- these statements touch a handful of rows at a time, not fifty thousand.
CREATE FUNCTION notify_jobs_repending() RETURNS trigger AS $$
BEGIN
    IF NEW.state = 'pending' AND OLD.state <> 'pending' AND NEW.run_at <= now() THEN
        PERFORM pg_notify('jobs_pending', '');
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_notify_repending
    AFTER UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION notify_jobs_repending();
