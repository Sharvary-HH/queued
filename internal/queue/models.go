package queue

import "time"

// State is the job lifecycle. It mirrors the job_state enum in the database;
// the two must be kept in sync by hand (migrations/0001).
type State string

const (
	StatePending   State = "pending"
	StateClaimed   State = "claimed"
	StateSucceeded State = "succeeded"

	// StateFailed is defined by the enum but nothing in the state machine
	// writes it. A failed attempt with retries left goes back to 'pending' with
	// run_at pushed into the future, which is what makes the claim query a
	// single-value equality test on the hot path; a failed attempt with no
	// retries left goes to 'dead'. There is no moment in between for a job to
	// sit in. It is kept so that reading the enum does not require also reading
	// this comment to know the value never appears.
	StateFailed State = "failed"

	StateDead State = "dead"
)

func (s State) Valid() bool {
	switch s {
	case StatePending, StateClaimed, StateSucceeded, StateFailed, StateDead:
		return true
	}
	return false
}

type Job struct {
	ID       int64
	Queue    string
	Kind     string
	Payload  []byte
	State    State
	Priority int

	RunAt       time.Time
	Attempt     int
	MaxAttempts int

	ClaimedAt *time.Time
	ClaimedBy *string
	// VisibilityTimeout is how long a claim is honoured before the reaper is
	// allowed to hand the job to somebody else.
	VisibilityTimeout time.Duration
	// ClaimExpiresAt is claimed_at + VisibilityTimeout, materialised at claim
	// time so the reaper's scan is a plain range over an index.
	ClaimExpiresAt *time.Time

	LastError      *string
	IdempotencyKey *string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Attempt is one row of the append-only job_attempts history.
type Attempt struct {
	ID         int64
	JobID      int64
	Attempt    int
	WorkerID   string
	StartedAt  time.Time
	FinishedAt *time.Time
	Error      *string
}

type RecurringJob struct {
	ID        int64
	Name      string
	Cron      string
	Queue     string
	Kind      string
	Payload   []byte
	Enabled   bool
	LastRunAt *time.Time
	NextRunAt time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}
