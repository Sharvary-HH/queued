package api

import (
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/Sharvary-HH/queued/internal/queue"
	"github.com/Sharvary-HH/queued/web"
)

// The dashboard is server-rendered html/template with no build step and no
// JavaScript framework. It is a few hundred rows of a database table with
// buttons on some of them; anything more elaborate would be a second project
// with its own toolchain to keep working, bolted onto this one.

// staticFS is the dashboard's static files, mounted at /static/.
var staticFS = web.Static

// Each page gets its own template set, rather than one set parsed from all the
// files at once.
//
// Parsing them together does not work and fails silently: every page file
// defines a block called "content", and in a single set the last definition
// parsed wins for all of them. The result is that every page renders whichever
// file sorted last alphabetically, with a 200 and no error anywhere. Parsing
// layout.html plus exactly one page per set keeps the names unambiguous.
//
// Done once at startup, because a broken template is a bug in a file that ships
// inside the binary: it should stop the process, not surface as a 500 the first
// time somebody opens that page.
var templates = parsePages(
	"overview.html", "jobs.html", "job.html", "dead.html", "recurring.html", "error.html",
)

func parsePages(names ...string) map[string]*template.Template {
	out := make(map[string]*template.Template, len(names))
	for _, name := range names {
		out[name] = template.Must(
			template.New(name).Funcs(templateFuncs).
				ParseFS(web.Templates, "templates/layout.html", "templates/"+name),
		)
	}
	return out
}

var templateFuncs = template.FuncMap{
	"since": func(t time.Time) string { return humanDuration(time.Since(t)) },
	"until": func(t time.Time) string { return humanDuration(time.Until(t)) },
	"time": func(t time.Time) string {
		return t.Local().Format("2006-01-02 15:04:05")
	},
	"timep": func(t *time.Time) string {
		if t == nil {
			return "—"
		}
		return t.Local().Format("2006-01-02 15:04:05")
	},
	"str": func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	},
	"json": func(b []byte) string {
		if len(b) == 0 {
			return "{}"
		}
		return string(b)
	},
	"truncate": func(n int, s string) string {
		if len(s) <= n {
			return s
		}
		return s[:n] + "…"
	},
	// pct scales a bar to the tallest one on the chart. Guards the empty case,
	// because dividing by a max of zero renders every bar as NaN% and the whole
	// chart silently disappears.
	"pct": func(v, max int64) int {
		if max <= 0 {
			return 0
		}
		return int(v * 100 / max)
	},
	"duration": func(start time.Time, end *time.Time) string {
		if end == nil {
			return "running"
		}
		return humanDuration(end.Sub(start))
	},
}

func humanDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}
	switch {
	case d < time.Second:
		return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
	case d < time.Minute:
		return strconv.FormatInt(int64(d.Seconds()), 10) + "s"
	case d < time.Hour:
		return strconv.FormatInt(int64(d.Minutes()), 10) + "m"
	case d < 24*time.Hour:
		return strconv.FormatInt(int64(d.Hours()), 10) + "h"
	default:
		return strconv.FormatInt(int64(d.Hours()/24), 10) + "d"
	}
}

type pageData struct {
	Title    string
	Nav      string
	Stats    []queue.QueueStat
	Totals   map[queue.State]int64
	Jobs     []queue.Job
	Job      *queue.Job
	Attempts []queue.Attempt
	Kinds    []string
	Queues   []string
	States   []queue.State
	Filter   queue.JobFilter
	Next     int64
	Points   []queue.ThroughputPoint
	MaxPoint int64
	Cron     []queue.RecurringJob
	Flash    string
	Error    string
}

func (s *Server) render(w http.ResponseWriter, name string, data pageData) {
	tpl, ok := templates[name]
	if !ok {
		s.log.Error("no such template", "template", name)
		http.Error(w, "template not found", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tpl.ExecuteTemplate(w, "layout", data); err != nil {
		// The status line is long gone by the time a template fails halfway
		// through, so there is nothing useful left to tell the browser.
		s.log.Error("template render failed", "template", name, "error", err)
	}
}

func (s *Server) pageOverview(w http.ResponseWriter, r *http.Request) {
	stats, err := s.stats.get(r.Context())
	if err != nil {
		s.render(w, "error.html", pageData{Title: "Error", Error: err.Error()})
		return
	}
	points, err := s.store.Throughput(r.Context(), 60)
	if err != nil {
		s.log.Warn("throughput query failed", "error", err)
	}

	var maxPoint int64
	for _, p := range points {
		if total := p.Succeeded + p.Failed; total > maxPoint {
			maxPoint = total
		}
	}

	s.render(w, "overview.html", pageData{
		Title:    "Overview",
		Nav:      "overview",
		Stats:    stats,
		Totals:   totalsByState(stats),
		Points:   points,
		MaxPoint: maxPoint,
	})
}

func (s *Server) pageJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := queue.JobFilter{
		State:  queue.State(q.Get("state")),
		Queue:  q.Get("queue"),
		Kind:   q.Get("kind"),
		Before: atoi64(q.Get("before")),
		Limit:  50,
	}

	jobs, next, err := s.store.ListJobs(r.Context(), filter)
	if err != nil {
		s.render(w, "error.html", pageData{Title: "Error", Error: err.Error()})
		return
	}
	kinds, _ := s.store.Kinds(r.Context())
	stats, _ := s.stats.get(r.Context())

	s.render(w, "jobs.html", pageData{
		Title:  "Jobs",
		Nav:    "jobs",
		Jobs:   jobs,
		Next:   next,
		Kinds:  kinds,
		Queues: queueNames(stats),
		States: allStates,
		Filter: filter,
		Flash:  q.Get("flash"),
	})
}

func (s *Server) pageJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		s.render(w, "error.html", pageData{Title: "Error", Error: err.Error()})
		return
	}

	job, err := s.store.JobByID(r.Context(), id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		s.render(w, "error.html", pageData{Title: "Not found", Error: err.Error()})
		return
	}
	attempts, err := s.store.AttemptsByJobID(r.Context(), id)
	if err != nil {
		s.log.Warn("attempt history query failed", "job_id", id, "error", err)
	}

	s.render(w, "job.html", pageData{
		Title:    "Job " + strconv.FormatInt(job.ID, 10),
		Nav:      "jobs",
		Job:      &job,
		Attempts: attempts,
		Flash:    r.URL.Query().Get("flash"),
	})
}

func (s *Server) pageDead(w http.ResponseWriter, r *http.Request) {
	filter := queue.JobFilter{
		State:  queue.StateDead,
		Before: atoi64(r.URL.Query().Get("before")),
		Limit:  50,
	}
	jobs, next, err := s.store.ListJobs(r.Context(), filter)
	if err != nil {
		s.render(w, "error.html", pageData{Title: "Error", Error: err.Error()})
		return
	}

	s.render(w, "dead.html", pageData{
		Title: "Dead letter queue",
		Nav:   "dead",
		Jobs:  jobs,
		Next:  next,
		Flash: r.URL.Query().Get("flash"),
	})
}

func (s *Server) pageRecurring(w http.ResponseWriter, r *http.Request) {
	entries, err := s.store.ListRecurring(r.Context())
	if err != nil {
		s.render(w, "error.html", pageData{Title: "Error", Error: err.Error()})
		return
	}
	s.render(w, "recurring.html", pageData{
		Title: "Recurring jobs",
		Nav:   "recurring",
		Cron:  entries,
		Flash: r.URL.Query().Get("flash"),
	})
}

// The form handlers redirect rather than rendering, so a refresh after clicking
// "requeue" does not submit it a second time.
func (s *Server) formRequeue(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := s.store.Requeue(r.Context(), id); err != nil {
		redirectWithFlash(w, r, "/dead", "could not requeue job: "+err.Error())
		return
	}
	s.log.Info("job requeued from the dead-letter queue", "job_id", id)
	redirectWithFlash(w, r, "/dead", "job "+strconv.FormatInt(id, 10)+" requeued")
}

func (s *Server) formCancel(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	target := "/jobs/" + strconv.FormatInt(id, 10)
	if _, err := s.store.Cancel(r.Context(), id); err != nil {
		redirectWithFlash(w, r, target, "could not cancel job: "+err.Error())
		return
	}
	s.log.Info("job cancelled", "job_id", id)
	redirectWithFlash(w, r, target, "job cancelled")
}

func (s *Server) formToggleRecurring(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	enabled := r.FormValue("enabled") == "true"

	if _, err := s.setEnabled(r.Context(), id, enabled); err != nil {
		redirectWithFlash(w, r, "/recurring", "could not update schedule: "+err.Error())
		return
	}
	s.log.Info("recurring job toggled", "recurring_id", id, "enabled", enabled)
	redirectWithFlash(w, r, "/recurring", "schedule updated")
}

func redirectWithFlash(w http.ResponseWriter, r *http.Request, path, message string) {
	http.Redirect(w, r, path+"?flash="+url.QueryEscape(message), http.StatusSeeOther)
}

var allStates = []queue.State{
	queue.StatePending, queue.StateClaimed, queue.StateSucceeded,
	queue.StateDead, queue.StateCancelled,
}

func totalsByState(stats []queue.QueueStat) map[queue.State]int64 {
	out := make(map[queue.State]int64, len(allStates))
	for _, st := range stats {
		out[st.State] += st.Count
	}
	for _, st := range allStates {
		if _, ok := out[st]; !ok {
			out[st] = 0
		}
	}
	return out
}

func queueNames(stats []queue.QueueStat) []string {
	seen := map[string]bool{}
	var out []string
	for _, st := range stats {
		if !seen[st.Queue] {
			seen[st.Queue] = true
			out = append(out, st.Queue)
		}
	}
	return out
}
