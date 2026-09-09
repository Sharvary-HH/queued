// Package metrics holds the Prometheus collectors.
//
// They live in one package rather than next to the code that updates them so
// that the whole surface an operator can graph is readable in one file, and so
// that no package has to import another just to bump a counter.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Buckets chosen for what these actually measure rather than from the default
// set, which starts at 5ms and is far too coarse at the bottom for a query that
// should take under a millisecond.
var (
	claimBuckets = []float64{
		0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1,
	}
	jobBuckets = []float64{
		0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300,
	}
)

var (
	JobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_enqueued_total",
		Help: "Jobs added to the queue.",
	}, []string{"queue", "kind"})

	// Status is succeeded / failed / dead, not just success and failure: the
	// difference between "failed and will retry" and "failed and gave up" is
	// the difference between noise and an alert.
	JobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_completed_total",
		Help: "Jobs that finished an attempt, by outcome.",
	}, []string{"kind", "status"})

	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "job_duration_seconds",
		Help:    "Handler execution time.",
		Buckets: jobBuckets,
	}, []string{"kind"})

	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "job_queue_depth",
		Help: "Jobs in each state, refreshed periodically.",
	}, []string{"queue", "state"})

	// The claim query is the hot path and the thing that stops scaling first,
	// so it gets its own histogram rather than being averaged into the rest.
	ClaimLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "job_claim_latency_seconds",
		Help:    "Latency of the claim query itself.",
		Buckets: claimBuckets,
	})

	ClaimedJobs = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jobs_claimed_total",
		Help: "Jobs handed to a worker by the claim query.",
	})

	// JobsReclaimed is the health signal. Everything else here can be busy for
	// good reasons; this one going up means workers are dying or handlers are
	// outrunning their visibility timeouts, and it should be flat at zero on a
	// system that is well.
	JobsReclaimed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_reclaimed_total",
		Help: "Jobs taken back from an expired claim. Non-zero means workers are dying or timing out.",
	}, []string{"returned_to"})

	WorkerPoolActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "worker_pool_active",
		Help: "Handlers currently executing.",
	}, []string{"worker_id", "queue"})

	SchedulerLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scheduler_is_leader",
		Help: "1 when this instance holds the scheduler advisory lock.",
	})
)

// ObserveJob records one finished attempt.
func ObserveJob(kind, status string, seconds float64) {
	JobsCompleted.WithLabelValues(kind, status).Inc()
	JobDuration.WithLabelValues(kind).Observe(seconds)
}
