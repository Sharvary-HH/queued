// Package api serves the JSON API, the dashboard and the metrics endpoint.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Sharvary-HH/queued/internal/metrics"
	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/internal/scheduler"
)

// Server is the API and dashboard.
type Server struct {
	store *queue.Store
	log   *slog.Logger

	// stats is shared by the dashboard and the metrics exporter. It is the one
	// genuinely expensive query in the project — a full aggregate over the jobs
	// table — so it is computed on a timer rather than per request. A dashboard
	// that a few people leave open on a wall must not be able to add load to the
	// database proportional to how many people are looking at it.
	stats *statsCache
}

func NewServer(store *queue.Store, log *slog.Logger) *Server {
	return &Server{
		store: store,
		log:   log,
		stats: &statsCache{store: store, ttl: 5 * time.Second},
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// Go 1.22 patterns: method and wildcards in the pattern itself, so there is
	// no router dependency and no hand-rolled method switch in every handler.
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("POST /api/jobs", s.apiEnqueue)
	mux.HandleFunc("GET /api/jobs", s.apiListJobs)
	mux.HandleFunc("GET /api/jobs/{id}", s.apiGetJob)
	mux.HandleFunc("POST /api/jobs/{id}/requeue", s.apiRequeue)
	mux.HandleFunc("POST /api/jobs/{id}/cancel", s.apiCancel)
	mux.HandleFunc("GET /api/stats", s.apiStats)
	mux.HandleFunc("GET /api/recurring", s.apiListRecurring)
	mux.HandleFunc("POST /api/recurring", s.apiUpsertRecurring)
	mux.HandleFunc("POST /api/recurring/{id}/enabled", s.apiSetRecurringEnabled)

	mux.HandleFunc("GET /{$}", s.pageOverview)
	mux.HandleFunc("GET /jobs", s.pageJobs)
	mux.HandleFunc("GET /jobs/{id}", s.pageJob)
	mux.HandleFunc("GET /dead", s.pageDead)
	mux.HandleFunc("GET /recurring", s.pageRecurring)
	mux.HandleFunc("POST /jobs/{id}/requeue", s.formRequeue)
	mux.HandleFunc("POST /jobs/{id}/cancel", s.formCancel)
	mux.HandleFunc("POST /recurring/{id}/toggle", s.formToggleRecurring)
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(staticFS)))

	return s.withLogging(mux)
}

// RefreshStats keeps the queue-depth gauges current. Run as a goroutine.
func (s *Server) RefreshStats(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = 10 * time.Second
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		if stats, err := s.stats.refresh(ctx); err == nil {
			// Gauges are set, not incremented, and a (queue, state) pair that
			// drops to zero rows stops being reported at all — so reset first,
			// or a queue that drains keeps exporting its last non-zero depth
			// forever and every alert on it stays lit.
			metrics.QueueDepth.Reset()
			for _, st := range stats {
				metrics.QueueDepth.WithLabelValues(st.Queue, string(st.State)).Set(float64(st.Count))
			}
		} else if ctx.Err() == nil {
			s.log.Error("could not refresh queue stats", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ---- probes -------------------------------------------------------------

// healthz is liveness: the process is up. It deliberately does not touch the
// database, otherwise a database blip would get every instance restarted —
// which is precisely the wrong response to the database being unwell.
func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz is readiness: we can actually serve, which means Postgres answers.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := s.store.Pool().Ping(ctx); err != nil {
		s.log.Warn("readiness check failed", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unavailable", "error": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// ---- JSON API -----------------------------------------------------------

type enqueueRequest struct {
	Queue             string          `json:"queue"`
	Kind              string          `json:"kind"`
	Payload           json.RawMessage `json:"payload"`
	Priority          *int            `json:"priority"`
	MaxAttempts       *int            `json:"max_attempts"`
	DelaySeconds      int             `json:"delay_seconds"`
	VisibilityTimeout int             `json:"visibility_timeout_seconds"`
	IdempotencyKey    string          `json:"idempotency_key"`
}

func (s *Server) apiEnqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	params := queue.EnqueueParams{
		Queue:       req.Queue,
		Kind:        req.Kind,
		Payload:     req.Payload,
		Priority:    req.Priority,
		MaxAttempts: req.MaxAttempts,
	}
	if req.DelaySeconds > 0 {
		params.RunAt = time.Now().Add(time.Duration(req.DelaySeconds) * time.Second)
	}
	if req.VisibilityTimeout > 0 {
		params.VisibilityTimeout = time.Duration(req.VisibilityTimeout) * time.Second
	}
	if req.IdempotencyKey != "" {
		params.IdempotencyKey = &req.IdempotencyKey
	}

	job, inserted, err := s.store.Enqueue(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// 201 for a new job, 200 when an idempotency key matched: the caller can
	// tell whether it was the one that created the work without a second
	// request.
	status := http.StatusOK
	if inserted {
		status = http.StatusCreated
	}
	writeJSON(w, status, jobResponse(job))
}

func (s *Server) apiListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := queue.JobFilter{
		State:  queue.State(q.Get("state")),
		Queue:  q.Get("queue"),
		Kind:   q.Get("kind"),
		Before: atoi64(q.Get("before")),
		Limit:  int(atoi64(q.Get("limit"))),
	}

	jobs, next, err := s.store.ListJobs(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	out := make([]map[string]any, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, jobResponse(job))
	}
	body := map[string]any{"jobs": out}
	if next > 0 {
		body["next_cursor"] = next
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) apiGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	job, err := s.store.JobByID(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	attempts, err := s.store.AttemptsByJobID(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	body := jobResponse(job)
	history := make([]map[string]any, 0, len(attempts))
	for _, a := range attempts {
		history = append(history, map[string]any{
			"attempt":     a.Attempt,
			"worker_id":   a.WorkerID,
			"started_at":  a.StartedAt,
			"finished_at": a.FinishedAt,
			"error":       a.Error,
		})
	}
	body["attempts"] = history
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) apiRequeue(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	job, err := s.store.Requeue(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.log.Info("job requeued from the dead-letter queue", "job_id", id, "kind", job.Kind)
	writeJSON(w, http.StatusOK, jobResponse(job))
}

func (s *Server) apiCancel(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	job, err := s.store.Cancel(r.Context(), id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	s.log.Info("job cancelled", "job_id", id, "kind", job.Kind)
	writeJSON(w, http.StatusOK, jobResponse(job))
}

func (s *Server) apiStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.stats.get(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	out := make([]map[string]any, 0, len(stats))
	for _, st := range stats {
		out = append(out, map[string]any{"queue": st.Queue, "state": st.State, "count": st.Count})
	}
	writeJSON(w, http.StatusOK, map[string]any{"stats": out})
}

func (s *Server) apiListRecurring(w http.ResponseWriter, r *http.Request) {
	entries, err := s.store.ListRecurring(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, map[string]any{
			"id": e.ID, "name": e.Name, "cron": e.Cron, "queue": e.Queue,
			"kind": e.Kind, "enabled": e.Enabled,
			"last_run_at": e.LastRunAt, "next_run_at": e.NextRunAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"recurring": out})
}

type recurringRequest struct {
	Name    string          `json:"name"`
	Cron    string          `json:"cron"`
	Queue   string          `json:"queue"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
	Enabled *bool           `json:"enabled"`
}

func (s *Server) apiUpsertRecurring(w http.ResponseWriter, r *http.Request) {
	var req recurringRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// Validated here, at the moment somebody types it, rather than left for the
	// scheduler to discover and then log about once a second forever.
	next, err := scheduler.NextRun(req.Cron, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	entry, err := s.store.UpsertRecurring(r.Context(), queue.RecurringParams{
		Name: req.Name, Cron: req.Cron, Queue: req.Queue, Kind: req.Kind,
		Payload: req.Payload, Enabled: enabled, NextRunAt: next,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": entry.ID, "name": entry.Name, "cron": entry.Cron,
		"kind": entry.Kind, "enabled": entry.Enabled, "next_run_at": entry.NextRunAt,
	})
}

func (s *Server) apiSetRecurringEnabled(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	entry, err := s.setEnabled(r.Context(), id, req.Enabled)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": entry.ID, "name": entry.Name, "enabled": entry.Enabled, "next_run_at": entry.NextRunAt,
	})
}

func (s *Server) setEnabled(ctx context.Context, id int64, enabled bool) (queue.RecurringJob, error) {
	// Re-enabling needs a next_run_at computed from now, which needs the entry's
	// cron expression, which means reading it first.
	entries, err := s.store.ListRecurring(ctx)
	if err != nil {
		return queue.RecurringJob{}, err
	}
	next := time.Now()
	for _, e := range entries {
		if e.ID == id {
			if n, err := scheduler.NextRun(e.Cron, time.Now().UTC()); err == nil {
				next = n
			}
			break
		}
	}
	return s.store.SetRecurringEnabled(ctx, id, enabled, next)
}

// ---- helpers ------------------------------------------------------------

func jobResponse(job queue.Job) map[string]any {
	return map[string]any{
		"id":                         job.ID,
		"queue":                      job.Queue,
		"kind":                       job.Kind,
		"payload":                    json.RawMessage(job.Payload),
		"state":                      job.State,
		"priority":                   job.Priority,
		"run_at":                     job.RunAt,
		"attempt":                    job.Attempt,
		"max_attempts":               job.MaxAttempts,
		"claimed_at":                 job.ClaimedAt,
		"claimed_by":                 job.ClaimedBy,
		"visibility_timeout_seconds": int(job.VisibilityTimeout.Seconds()),
		"last_error":                 job.LastError,
		"idempotency_key":            job.IdempotencyKey,
		"created_at":                 job.CreatedAt,
		"updated_at":                 job.UpdatedAt,
	}
}

// statsCache computes the expensive aggregate at most once per ttl, and shares
// one in-flight computation between concurrent callers.
type statsCache struct {
	store *queue.Store
	ttl   time.Duration

	mu      sync.Mutex
	value   []queue.QueueStat
	fetched time.Time
}

func (c *statsCache) get(ctx context.Context) ([]queue.QueueStat, error) {
	c.mu.Lock()
	if time.Since(c.fetched) < c.ttl && c.value != nil {
		defer c.mu.Unlock()
		return c.value, nil
	}
	c.mu.Unlock()
	return c.refresh(ctx)
}

func (c *statsCache) refresh(ctx context.Context) ([]queue.QueueStat, error) {
	stats, err := c.store.Stats(ctx)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.value, c.fetched = stats, time.Now()
	c.mu.Unlock()
	return stats, nil
}

func decodeJSON(_ http.ResponseWriter, r *http.Request, dst any) error {
	// A cap on the body, because an unbounded json.Decode on a public endpoint
	// is a way to be handed a gigabyte.
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	return nil
}

func pathID(r *http.Request) (int64, error) {
	raw := r.PathValue("id")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid job id %q", raw)
	}
	return id, nil
}

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return n
}

// writeStoreError maps the store's sentinel errors onto status codes, so the
// handlers do not each repeat the same three checks.
func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrJobNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, queue.ErrNotDead), errors.Is(err, queue.ErrNotCancellable):
		// 409: the request is well formed and the job exists, it is just not in
		// a state where this makes sense.
		writeError(w, http.StatusConflict, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The metrics scrape is every few seconds forever and says nothing; it
		// would be most of the log volume on an idle system.
		if r.URL.Path == "/metrics" || r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		s.log.Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
