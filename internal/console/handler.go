// Package console implements the read-only job/task history HTTP console: an
// embedded single-page UI plus a small JSON API for listing jobs/tasks,
// retrying, and terminating.
//
// It depends on internal/domain, internal/jobconfig, and internal/repository
// (for ErrJobAlreadyTerminal) plus internal/executor to actually perform a
// retry, but never on internal/scheduler's *Service: NewHandler takes a
// narrow Scheduler interface instead, so scheduler can import console to
// build its ConsoleHandler method without an import cycle.
package console

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgtype"
	"gorm.io/gorm"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/executor"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/repository"
)

//go:embed assets/index.html
var consoleIndexHTML string

var consoleIndexTmpl = template.Must(template.New("index").Parse(consoleIndexHTML))

// Scheduler is the subset of *scheduler.Service the console needs: enough to
// list registered jobs, read job/task history, and act on a job (retry or
// terminate).
type Scheduler interface {
	DB() *gorm.DB
	GetRegisteredJobs() map[string]jobconfig.JobConfig
	AsynqRedisOpt() asynq.RedisClientOpt
	RetryJob(ctx context.Context, jobID uuid.UUID, fullRetry bool, refID string) (*domain.Job, error)
	RetryTriggerJob(ctx context.Context, jobID uuid.UUID, refID string) (*domain.Job, error)
	RetryAsynqTask(ctx context.Context, queue, taskID string) error
	FindJobByID(ctx context.Context, jobID uuid.UUID) (*domain.Job, error)
	TerminateJob(ctx context.Context, jobID uuid.UUID) error
}

// consoleAPI wires the read-only history queries and the retry action behind
// an http.Handler. It holds the live Scheduler so retry/terminate can act on
// real jobs.
type consoleAPI struct {
	svc   Scheduler
	store *consoleStore
}

// NewHandler returns an http.Handler serving the job-history console: an
// embedded single-page UI at "/" plus a small JSON API under "/api". Mount it
// wherever you like, or use it as-is behind your own server.
//
// The console has no built-in authentication — serve it on localhost or
// behind your own auth/network controls.
func NewHandler(svc Scheduler) http.Handler {
	api := &consoleAPI{svc: svc, store: &consoleStore{db: svc.DB()}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/jobs/meta", api.handleMeta)
	mux.HandleFunc("GET /api/jobs", api.handleListJobs)
	mux.HandleFunc("GET /api/jobs/{id}/tasks", api.handleListJobTasks)
	mux.HandleFunc("GET /api/jobs/{id}", api.handleJobDetail)
	mux.HandleFunc("POST /api/jobs/{id}/retry", api.handleRetry)
	mux.HandleFunc("POST /api/jobs/{id}/terminate", api.handleTerminate)
	mux.HandleFunc("GET /api/config", api.handleConfig)
	mux.HandleFunc("GET /api/upcoming", api.handleUpcoming)
	mux.HandleFunc("GET /api/stats/jobs", api.handleStatsJobs)
	mux.HandleFunc("GET /api/stats/timeseries", api.handleStatsTimeseries)
	mux.HandleFunc("GET /api/stats/job-metrics", api.handleStatsJobMetrics)
	mux.HandleFunc("GET /", api.handleIndex)
	return mux
}

func (api *consoleAPI) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = consoleIndexTmpl.Execute(w, nil)
}

func (api *consoleAPI) handleMeta(w http.ResponseWriter, r *http.Request) {
	names := make([]string, 0)
	for name := range api.svc.GetRegisteredJobs() {
		names = append(names, name)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"job_names":    names,
		"job_statuses": domain.TerminalJobStates,
		"all_statuses": []string{
			domain.JobStatusPending, domain.JobStatusRunning, domain.JobStatusCompleted,
			domain.JobStatusFailed, domain.JobStatusPartialFailed, domain.JobStatusRejected, domain.JobStatusCancelled,
		},
	})
}

func (api *consoleAPI) handleListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := JobFilter{
		Name:   q.Get("name"),
		Status: q.Get("status"),
		RefID:  strings.TrimSpace(q.Get("ref_id")),
		From:   parseTimeParam(q.Get("from")),
		To:     parseTimeParam(q.Get("to")),
		Limit:  atoiOr(q.Get("limit"), defaultConsolePageSize),
		Offset: atoiOr(q.Get("offset"), 0),
	}

	pgFetch := filter
	pgFetch.Offset = 0
	pgFetch.Limit = filter.Offset + filter.Limit + maxAsynqOrphans
	if pgFetch.Limit > maxConsolePageSize {
		pgFetch.Limit = maxConsolePageSize
	}

	asynqCandidates, err := listAsynqTriggers(r.Context(), api.svc.AsynqRedisOpt(), api.svc.GetRegisteredJobs(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	asynqRefIDs := make([]string, 0, len(asynqCandidates))
	for i := range asynqCandidates {
		if asynqCandidates[i].UniqueReferenceID != nil {
			asynqRefIDs = append(asynqRefIDs, *asynqCandidates[i].UniqueReferenceID)
		}
	}
	existingRefs, err := api.store.ListExistingRefIDs(r.Context(), asynqRefIDs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	asynqSummaries := make([]JobSummary, 0, len(asynqCandidates))
	for i := range asynqCandidates {
		if asynqCandidates[i].UniqueReferenceID != nil {
			if _, exists := existingRefs[*asynqCandidates[i].UniqueReferenceID]; exists {
				continue
			}
		}
		asynqSummaries = append(asynqSummaries, asynqCandidates[i])
	}

	pgSummaries, err := api.store.ListJobs(r.Context(), pgFetch)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	for i := range pgSummaries {
		pgSummaries[i].Source = "postgres"
	}

	summaries := mergeJobSummaries(pgSummaries, asynqSummaries, filter.Offset, filter.Limit)

	jobs := make([]jobView, 0, len(summaries))
	for i := range summaries {
		jobs = append(jobs, summaryToView(summaries[i]))
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"jobs":   jobs,
		"limit":  filter.Limit,
		"offset": filter.Offset,
	})
}

func (api *consoleAPI) handleJobDetail(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("id")
	if queue, taskID, ok := parseAsynqJobID(rawID); ok {
		summary, err := getAsynqTrigger(r.Context(), api.svc.AsynqRedisOpt(), api.svc.GetRegisteredJobs(), queue, taskID)
		if err != nil {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"job":     summaryToView(*summary),
			"lineage": []jobView{},
		})
		return
	}

	jobID, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	detail, err := api.store.GetJobDetail(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}

	lineage := make([]jobView, 0, len(detail.Lineage))
	for i := range detail.Lineage {
		lineage = append(lineage, summaryToView(detail.Lineage[i]))
	}
	// Tasks are intentionally omitted: the console fetches them via the
	// paginated GET /api/jobs/{id}/tasks endpoint (handleListJobTasks).
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"job":     jobToView(detail.Job),
		"lineage": lineage,
	})
}

func (api *consoleAPI) handleListJobTasks(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("id")
	if _, _, ok := parseAsynqJobID(rawID); ok {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"tasks":  []taskView{},
			"total":  0,
			"limit":  atoiOr(r.URL.Query().Get("limit"), defaultConsolePageSize),
			"offset": atoiOr(r.URL.Query().Get("offset"), 0),
		})
		return
	}

	jobID, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	q := r.URL.Query()
	filter := TaskFilter{
		JobID:  jobID,
		Status: q.Get("status"),
		Limit:  atoiOr(q.Get("limit"), defaultConsolePageSize),
		Offset: atoiOr(q.Get("offset"), 0),
	}

	page, err := api.store.ListTasksForJob(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	tasks := make([]taskView, 0, len(page.Tasks))
	for i := range page.Tasks {
		tasks = append(tasks, taskToView(page.Tasks[i]))
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tasks":  tasks,
		"total":  page.Total,
		"limit":  page.Limit,
		"offset": page.Offset,
	})
}

func (api *consoleAPI) handleRetry(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("id")
	full := r.URL.Query().Get("full") == "true"

	if queue, taskID, ok := parseAsynqJobID(rawID); ok {
		if err := api.svc.RetryAsynqTask(r.Context(), queue, taskID); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"retried":       true,
			"retry_method":  "asynq",
			"asynq_task_id": taskID,
		})
		return
	}

	jobID, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	refID := uuid.New().String()

	job, err := api.svc.FindJobByID(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}

	if executor.IsTriggerFailureJob(job) {
		taskCount, err := api.store.CountTasksForJob(r.Context(), jobID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if taskCount == 0 {
			cloned, err := api.svc.RetryTriggerJob(r.Context(), jobID, refID)
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{
				"retried":       true,
				"retry_method":  "trigger",
				"cloned_job_id": cloned.ID.String(),
				"job":           jobToView(cloned),
			})
			return
		}
	}

	cloned, err := api.svc.RetryJob(r.Context(), jobID, full, refID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if cloned == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{"retried": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"retried":       true,
		"retry_method":  "job",
		"cloned_job_id": cloned.ID.String(),
		"job":           jobToView(cloned),
	})
}

func (api *consoleAPI) handleTerminate(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("id")
	if _, _, ok := parseAsynqJobID(rawID); ok {
		writeError(w, http.StatusBadRequest, errors.New("cannot terminate an asynq-only trigger; retry it instead"))
		return
	}

	jobID, err := uuid.Parse(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if err := api.svc.TerminateJob(r.Context(), jobID); err != nil {
		if errors.Is(err, repository.ErrJobAlreadyTerminal) {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"terminated": true, "job_id": jobID.String()})
}

// --- view DTOs (plain JSON-friendly shapes; decode pgtype.JSONB fields) ---

type progressView struct {
	Total     int `json:"total"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
	Running   int `json:"running"`
	Pending   int `json:"pending"`
}

type jobView struct {
	ID                string       `json:"id"`
	ParentJobID       *string      `json:"parent_job_id,omitempty"`
	Name              string       `json:"name"`
	Status            string       `json:"status"`
	UniqueReferenceID *string      `json:"unique_reference_id,omitempty"`
	CreatedAt         time.Time    `json:"created_at"`
	StartedAt         *time.Time   `json:"started_at,omitempty"`
	FinishedAt        *time.Time   `json:"finished_at,omitempty"`
	DurationMs        *int64       `json:"duration_ms,omitempty"`
	Metadata          interface{}  `json:"metadata,omitempty"`
	Source            string       `json:"source,omitempty"`
	Progress          progressView `json:"progress"`
}

type taskView struct {
	ID             string      `json:"id"`
	Name           string      `json:"name"`
	Status         string      `json:"status"`
	Attempt        int         `json:"attempt"`
	MaxRetries     int         `json:"max_retries"`
	SequenceNumber int         `json:"sequence_number"`
	Params         interface{} `json:"params,omitempty"`
	Result         interface{} `json:"result,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
	StartedAt      *time.Time  `json:"started_at,omitempty"`
	FinishedAt     *time.Time  `json:"finished_at,omitempty"`
	DurationMs     *int64      `json:"duration_ms,omitempty"`
}

func summaryToView(s JobSummary) jobView {
	id := s.ID.String()
	source := s.Source
	if source == "" {
		source = "postgres"
	}
	if source == "asynq" {
		id = asynqJobID(s.AsynqTaskID)
	}

	var metadata interface{}
	if s.Metadata != nil {
		metadata = s.Metadata
	}

	return jobView{
		ID:                id,
		ParentJobID:       uuidPtrToString(s.ParentJobID),
		Name:              s.Name,
		Status:            s.Status,
		UniqueReferenceID: s.UniqueReferenceID,
		CreatedAt:         s.CreatedAt,
		StartedAt:         s.StartedAt,
		FinishedAt:        s.FinishedAt,
		DurationMs:        s.DurationMs,
		Metadata:          metadata,
		Source:            source,
		Progress: progressView{
			Total: s.Total, Completed: s.Completed, Failed: s.Failed,
			Running: s.Running, Pending: s.Pending,
		},
	}
}

func jobToView(j *domain.Job) jobView {
	return jobView{
		ID:                j.ID.String(),
		ParentJobID:       uuidPtrToString(j.ParentJobID),
		Name:              j.Name,
		Status:            j.Status,
		UniqueReferenceID: j.UniqueReferenceID,
		CreatedAt:         j.CreatedAt,
		StartedAt:         j.StartedAt,
		FinishedAt:        j.FinishedAt,
		DurationMs:        j.DurationMs,
		Metadata:          decodeJSONB(j.Metadata),
		Source:            "postgres",
	}
}

func taskToView(t domain.Task) taskView {
	return taskView{
		ID:             t.ID.String(),
		Name:           t.Name,
		Status:         t.Status,
		Attempt:        t.Attempt,
		MaxRetries:     t.MaxRetries,
		SequenceNumber: t.SequenceNumber,
		Params:         decodeJSONB(t.Params),
		Result:         decodeJSONB(t.Result),
		CreatedAt:      t.CreatedAt,
		StartedAt:      t.StartedAt,
		FinishedAt:     t.FinishedAt,
		DurationMs:     t.DurationMs,
	}
}

// --- helpers ---

func decodeJSONB(j pgtype.JSONB) interface{} {
	if j.Status != pgtype.Present || len(j.Bytes) == 0 {
		return nil
	}
	var v interface{}
	if err := json.Unmarshal(j.Bytes, &v); err != nil {
		return string(j.Bytes)
	}
	return v
}

func uuidPtrToString(id *uuid.UUID) *string {
	if id == nil {
		return nil
	}
	s := id.String()
	return &s
}

func parseTimeParam(v string) *time.Time {
	if v == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return nil
	}
	return &t
}

func atoiOr(v string, fallback int) int {
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
