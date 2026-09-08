package queue

import "time"

// State is the job lifecycle. It mirrors the job_state enum in the database;
// the two must be kept in sync by hand (migrations/0001).
type State string

const (
	StatePending   State = "pending"
	StateClaimed   State = "claimed"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateDead      State = "dead"
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
