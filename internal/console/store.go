package console

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/zepto-labs/scherry/internal/domain"
)

// consoleStore holds the read-only queries backing the history console. It
// reads the scherry_jobs / scherry_tasks tables directly and is kept
// separate from the core Repository so the write path stays small.
type consoleStore struct {
	db *gorm.DB
}

// JobFilter narrows and paginates the job history list.
type JobFilter struct {
	Name   string
	Status string
	// RefID filters to jobs whose UniqueReferenceID exactly matches (the
	// caller-supplied refID used for dedup). Since that column has no unique
	// constraint, this can match more than one job.
	RefID  string
	From   *time.Time
	To     *time.Time
	Limit  int
	Offset int
}

// JobSummary is a job row plus aggregated task-status counts for the list view.
type JobSummary struct {
	ID                uuid.UUID              `json:"id"`
	ParentJobID       *uuid.UUID             `json:"parent_job_id"`
	Name              string                 `json:"name"`
	Status            string                 `json:"status"`
	UniqueReferenceID *string                `json:"unique_reference_id"`
	CreatedAt         time.Time              `json:"created_at"`
	UpdatedAt         time.Time              `json:"updated_at"`
	StartedAt         *time.Time             `json:"started_at"`
	FinishedAt        *time.Time             `json:"finished_at"`
	DurationMs        *int64                 `json:"duration_ms"`
	Total             int                    `json:"total"`
	Completed         int                    `json:"completed"`
	Failed            int                    `json:"failed"`
	Running           int                    `json:"running"`
	Pending           int                    `json:"pending"`
	Source            string                 `json:"source,omitempty"` // "postgres" (default) or "asynq"
	AsynqQueue        string                 `json:"asynq_queue,omitempty"`
	AsynqTaskID       string                 `json:"asynq_task_id,omitempty"`
	Metadata          map[string]interface{} `json:"metadata,omitempty" gorm:"-"`
}

const defaultConsolePageSize = 50
const maxConsolePageSize = 200

// TaskFilter narrows and paginates the task list for a given job.
type TaskFilter struct {
	JobID  uuid.UUID
	Status string
	Limit  int
	Offset int
}

// TaskPage is the paginated result of ListTasksForJob.
type TaskPage struct {
	Tasks  []domain.Task
	Total  int
	Limit  int
	Offset int
}

// ListJobs returns jobs newest-first with per-job task-status counts, applying
// the optional name/status/time filters and pagination in a single query.
func (c *consoleStore) ListJobs(ctx context.Context, f JobFilter) ([]JobSummary, error) {
	conds := []string{"1 = 1"}
	args := []interface{}{}
	next := func() string { return fmt.Sprintf("$%d", len(args)+1) }

	if f.Name != "" {
		conds = append(conds, "j.name = "+next())
		args = append(args, f.Name)
	}
	if f.Status != "" {
		conds = append(conds, "j.status = "+next())
		args = append(args, f.Status)
	}
	if f.RefID != "" {
		conds = append(conds, "j.unique_reference_id = "+next())
		args = append(args, f.RefID)
	}
	if f.From != nil {
		conds = append(conds, "j.created_at >= "+next())
		args = append(args, *f.From)
	}
	if f.To != nil {
		conds = append(conds, "j.created_at <= "+next())
		args = append(args, *f.To)
	}

	limit := f.Limit
	if limit <= 0 {
		limit = defaultConsolePageSize
	}
	if limit > maxConsolePageSize {
		limit = maxConsolePageSize
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}
	limitPlaceholder := next()
	args = append(args, limit)
	offsetPlaceholder := next()
	args = append(args, offset)

	query := fmt.Sprintf(`
		SELECT
			j.id, j.parent_job_id, j.name, j.status, j.unique_reference_id,
			j.created_at, j.updated_at, j.started_at, j.finished_at, j.duration_ms,
			count(t.id) AS total,
			count(t.id) FILTER (WHERE t.status = 'COMPLETED') AS completed,
			count(t.id) FILTER (WHERE t.status IN ('FAILED', 'MAX_RETRIES_EXHAUSTED')) AS failed,
			count(t.id) FILTER (WHERE t.status = 'RUNNING') AS running,
			count(t.id) FILTER (WHERE t.status = 'PENDING') AS pending
		FROM scherry_jobs j
		LEFT JOIN scherry_tasks t ON j.id = t.job_id
		WHERE %s
		GROUP BY j.id, j.created_at
		ORDER BY j.created_at DESC
		LIMIT %s OFFSET %s`,
		strings.Join(conds, " AND "), limitPlaceholder, offsetPlaceholder)

	var out []JobSummary
	if err := c.db.WithContext(ctx).Raw(query, args...).Scan(&out).Error; err != nil {
		return nil, err
	}
	return out, nil
}

// ListExistingRefIDs returns the subset of refIDs that already have a
// scherry_jobs row (used to exclude Asynq orphans already persisted).
func (c *consoleStore) ListExistingRefIDs(ctx context.Context, refIDs []string) (map[string]struct{}, error) {
	out := make(map[string]struct{})
	if len(refIDs) == 0 {
		return out, nil
	}
	var found []string
	if err := c.db.WithContext(ctx).
		Table("scherry_jobs").
		Where("unique_reference_id IN ?", refIDs).
		Distinct("unique_reference_id").
		Pluck("unique_reference_id", &found).Error; err != nil {
		return nil, err
	}
	for _, id := range found {
		out[id] = struct{}{}
	}
	return out, nil
}

// CountTasksForJob returns the number of tasks for a job.
func (c *consoleStore) CountTasksForJob(ctx context.Context, jobID uuid.UUID) (int, error) {
	var count int64
	err := c.db.WithContext(ctx).Table("scherry_tasks").Where("job_id = ?", jobID).Count(&count).Error
	return int(count), err
}

// ListTasksForJob returns a paginated slice of tasks for jobID, backed by db
// directly. Exported so callers with programmatic (non-HTTP) access — e.g.
// *scheduler.Service.GetJobTasks — can reuse the same paginated query the
// console's GET /api/jobs/{id}/tasks endpoint uses, without reaching into the
// unexported consoleStore type.
func ListTasksForJob(ctx context.Context, db *gorm.DB, f TaskFilter) (*TaskPage, error) {
	return (&consoleStore{db: db}).ListTasksForJob(ctx, f)
}

func (c *consoleStore) ListTasksForJob(ctx context.Context, f TaskFilter) (*TaskPage, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = defaultConsolePageSize
	}
	if limit > maxConsolePageSize {
		limit = maxConsolePageSize
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}

	conds := []string{"job_id = $1"}
	args := []interface{}{f.JobID}
	next := func() string { return fmt.Sprintf("$%d", len(args)+1) }

	if f.Status != "" {
		conds = append(conds, "status = "+next())
		args = append(args, f.Status)
	}

	where := strings.Join(conds, " AND ")

	// Count total matching tasks so the caller can compute total pages.
	countQuery := fmt.Sprintf(`SELECT count(*) FROM scherry_tasks WHERE %s`, where)
	var total int
	if err := c.db.WithContext(ctx).Raw(countQuery, args...).Scan(&total).Error; err != nil {
		return nil, err
	}

	limitPlaceholder := next()
	args = append(args, limit)
	offsetPlaceholder := next()
	args = append(args, offset)

	dataQuery := fmt.Sprintf(`
		SELECT id, job_id, name, status, attempt, max_retries, distribution_key,
		       params, result, created_at, updated_at, started_at, finished_at, duration_ms,
		       sequence_number
		FROM scherry_tasks
		WHERE %s
		ORDER BY sequence_number ASC
		LIMIT %s OFFSET %s`,
		where, limitPlaceholder, offsetPlaceholder)

	var tasks []domain.Task
	if err := c.db.WithContext(ctx).Raw(dataQuery, args...).Scan(&tasks).Error; err != nil {
		return nil, err
	}

	return &TaskPage{
		Tasks:  tasks,
		Total:  total,
		Limit:  limit,
		Offset: offset,
	}, nil
}

// JobDetail is a single job with its retry lineage (parent plus any retry-clone
// children, discoverable via parent_job_id). Tasks are intentionally omitted
// here: the console fetches them separately via the paginated ListTasksForJob
// endpoint so that opening a job detail never loads all T task rows at once.
type JobDetail struct {
	Job     *domain.Job
	Lineage []JobSummary
}

// GetJobDetail loads one job and its retry-chain lineage. Tasks are excluded:
// the console fetches them via the paginated ListTasksForJob endpoint so that
// opening a detail view never loads all T task rows into memory.
func (c *consoleStore) GetJobDetail(ctx context.Context, jobID uuid.UUID) (*JobDetail, error) {
	var job domain.Job
	if err := c.db.WithContext(ctx).Where("id = ?", jobID).First(&job).Error; err != nil {
		return nil, err
	}

	lineage, err := c.jobLineage(ctx, &job)
	if err != nil {
		return nil, err
	}

	return &JobDetail{Job: &job, Lineage: lineage}, nil
}

// jobLineage returns the retry-chain neighbours of a job: its parent (if any)
// and its retry-clone children. Counts are omitted (lightweight summaries).
func (c *consoleStore) jobLineage(ctx context.Context, job *domain.Job) ([]JobSummary, error) {
	ids := []uuid.UUID{}
	if job.ParentJobID != nil {
		ids = append(ids, *job.ParentJobID)
	}

	query := c.db.WithContext(ctx).
		Table("scherry_jobs").
		Select("id, parent_job_id, name, status, unique_reference_id, created_at, updated_at, started_at, finished_at, duration_ms").
		Order("created_at ASC")

	var out []JobSummary
	if len(ids) > 0 {
		if err := query.Where("parent_job_id = ? OR id IN ?", job.ID, ids).Scan(&out).Error; err != nil {
			return nil, err
		}
		return out, nil
	}
	if err := query.Where("parent_job_id = ?", job.ID).Scan(&out).Error; err != nil {
		return nil, err
	}
	return out, nil
}
