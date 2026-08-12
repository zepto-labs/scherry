package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/zepto-labs/scherry/internal/domain"
)

// TaskStatusSummary holds aggregate task counts for a job, computed by a single
// COUNT(*) query instead of loading every row. Used by the job finalizer to avoid
// the O(T) per-task heap fetch that previously scaled as O(T²) per job.
type TaskStatusSummary struct {
	Total       int
	Completed   int
	NonTerminal int // tasks that are still PENDING or RUNNING
}

type Repository interface {
	FindJobByID(ctx context.Context, jobID uuid.UUID) (*domain.Job, error)
	FindTaskWithJob(ctx context.Context, taskID uuid.UUID) (*domain.Task, *domain.Job, error)
	FindPendingJobByParentID(ctx context.Context, parentJobID uuid.UUID) (*domain.Job, error)
	FindJobWithTasksByUniqueReferenceID(ctx context.Context, uniqueReferenceID string) (*domain.Job, []domain.Task, error)
	FindTasksByJobIDAndStatuses(ctx context.Context, jobID uuid.UUID, statuses []string) ([]domain.Task, error)
	// FindTasksByJobIDAndStatusesBatch returns up to limit tasks for jobID with
	// sequence_number > afterSeq, ordered by sequence_number ASC. Pass afterSeq = -1
	// to start from the first task. An empty statuses slice matches all statuses.
	// Used by the streaming retry path to page through tasks without loading all
	// T rows into memory at once.
	FindTasksByJobIDAndStatusesBatch(ctx context.Context, jobID uuid.UUID, statuses []string, afterSeq, limit int) ([]domain.Task, error)
	GetTaskStatusSummary(ctx context.Context, jobID uuid.UUID) (TaskStatusSummary, error)
	// CreateTasks inserts tasks in batches. Used by the streaming retry path where
	// the parent job was pre-created and tasks are inserted one batch at a time.
	CreateTasks(ctx context.Context, tasks []domain.Task) error
	UpdateTasksStatus(ctx context.Context, taskIDs []uuid.UUID, status string) error
	// UpdateTasksStatusByJobID bulk-updates every task for a job to newStatus.
	// Prefer this over UpdateTasksStatus when operating on all tasks of a job:
	// it issues a single WHERE job_id = ? update instead of WHERE id IN (N UUIDs),
	// avoiding a potentially huge bind-parameter list at large T.
	UpdateTasksStatusByJobID(ctx context.Context, jobID uuid.UUID, newStatus string) error
	CancelPreviousJobs(ctx context.Context, jobName string, currentJobID uuid.UUID) error
	// TerminateJob cancels a non-terminal job and all of its pending tasks.
	// Tasks that are already in a terminal state are left unchanged.
	// Returns ErrJobAlreadyTerminal when the job is not in a cancellable state.
	TerminateJob(ctx context.Context, jobID uuid.UUID) error
	UpdateJobStatus(ctx context.Context, jobID uuid.UUID, status string) error
	UpdateJob(ctx context.Context, jobID uuid.UUID, updates map[string]interface{}) error
	UpdateTask(ctx context.Context, taskID uuid.UUID, updates map[string]interface{}) error
	CreateJobAndTasks(ctx context.Context, job *domain.Job, tasks []domain.Task) (*domain.Job, []domain.Task, error)
	// DB returns the underlying *gorm.DB connection, or nil if this
	// Repository implementation is not backed by one (e.g. a test double).
	DB() *gorm.DB
	// FindCompletableJobIDs returns up to limit RUNNING job IDs for jobName
	// whose tasks have all already reached a terminal state (or that have no
	// tasks at all) — i.e. jobs ready to finalize. Used by
	// executor.CheckJobCompletions, the handler for a job's
	// JobCompletionCron.
	FindCompletableJobIDs(ctx context.Context, jobName string, limit int) ([]uuid.UUID, error)
}

type repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) Repository {
	return &repository{db: db}
}

func (r *repository) DB() *gorm.DB {
	return r.db
}

func (r *repository) FindJobByID(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	var job domain.Job
	if err := r.db.WithContext(ctx).Where("id = ?", jobID).First(&job).Error; err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *repository) FindTaskWithJob(ctx context.Context, taskID uuid.UUID) (*domain.Task, *domain.Job, error) {
	var task domain.Task
	if err := r.db.WithContext(ctx).Preload("Job").Where("id = ?", taskID).First(&task).Error; err != nil {
		return nil, nil, err
	}
	// Preload does not error when the association is missing — it silently
	// leaves task.Job nil. Return an explicit error so callers never receive
	// a nil job without knowing about it.
	if task.Job == nil {
		return nil, nil, fmt.Errorf("job not found for task %s (job_id %s)", task.ID, task.JobID)
	}
	return &task, task.Job, nil
}

func (r *repository) FindJobWithTasksByUniqueReferenceID(ctx context.Context, uniqueReferenceID string) (*domain.Job, []domain.Task, error) {
	// Two-phase lookup: fetch the job row first (hits the unique_reference_id index),
	// then fetch its tasks only when a duplicate is found. The previous single LEFT
	// JOIN returned all T task rows unconditionally, which at large T (e.g. 100k
	// tasks) loaded the full task set just for a dedup check that in the common case
	// (no duplicate) returns nothing.
	//
	// unique_reference_id has no unique constraint (see migrations/001), so several
	// rows can share one value. Resolve that by returning the most recent.
	var job domain.Job
	err := r.db.WithContext(ctx).
		Where("unique_reference_id = ?", uniqueReferenceID).
		Order("created_at DESC").
		First(&job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	tasks, err := r.FindTasksByJobIDAndStatuses(ctx, job.ID, nil)
	if err != nil {
		return nil, nil, err
	}
	return &job, tasks, nil
}

func (r *repository) FindTasksByJobIDAndStatuses(ctx context.Context, jobID uuid.UUID, statuses []string) ([]domain.Task, error) {
	var tasks []domain.Task
	query := r.db.WithContext(ctx).Where("job_id = ?", jobID)
	if len(statuses) > 0 {
		query = query.Where("status IN ?", statuses)
	}
	return tasks, query.Find(&tasks).Error
}

func (r *repository) FindTasksByJobIDAndStatusesBatch(ctx context.Context, jobID uuid.UUID, statuses []string, afterSeq, limit int) ([]domain.Task, error) {
	var tasks []domain.Task
	query := r.db.WithContext(ctx).
		Where("job_id = ? AND sequence_number > ?", jobID, afterSeq).
		Order("sequence_number ASC").
		Limit(limit)
	if len(statuses) > 0 {
		query = query.Where("status IN ?", statuses)
	}
	return tasks, query.Find(&tasks).Error
}

func (r *repository) CreateTasks(ctx context.Context, tasks []domain.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).CreateInBatches(&tasks, TaskInsertBatchSize).Error
}

func (r *repository) UpdateTasksStatus(ctx context.Context, taskIDs []uuid.UUID, status string) error {
	return r.db.WithContext(ctx).Model(&domain.Task{}).Where("id IN ?", taskIDs).Updates(map[string]interface{}{
		"status": status, "updated_at": time.Now(),
	}).Error
}

func (r *repository) UpdateTasksStatusByJobID(ctx context.Context, jobID uuid.UUID, newStatus string) error {
	return r.db.WithContext(ctx).Model(&domain.Task{}).Where("job_id = ?", jobID).Updates(map[string]interface{}{
		"status": newStatus, "updated_at": time.Now(),
	}).Error
}

// ErrJobAlreadyTerminal is returned by TerminateJob when the target job is
// already in a terminal state and cannot be cancelled.
var ErrJobAlreadyTerminal = errors.New("job is already in a terminal state")

func (r *repository) TerminateJob(ctx context.Context, jobID uuid.UUID) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		jobQuery := `
			UPDATE scherry_jobs SET status = $1, updated_at = NOW()
			WHERE id = $2 AND status IN ($3, $4)
			RETURNING id`
		var updated []uuid.UUID
		if err := tx.Raw(jobQuery, domain.JobStatusCancelled, jobID, domain.JobStatusPending, domain.JobStatusRunning).
			Scan(&updated).Error; err != nil {
			return err
		}
		if len(updated) == 0 {
			return ErrJobAlreadyTerminal
		}
		taskQuery := `
			UPDATE scherry_tasks SET status = $1, updated_at = NOW()
			WHERE job_id = $2 AND status = $3`
		return tx.Exec(taskQuery, domain.TaskStatusCancelled, jobID, domain.TaskStatusPending).Error
	})
}

func (r *repository) CancelPreviousJobs(ctx context.Context, jobName string, currentJobID uuid.UUID) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var cancelledJobIDs []uuid.UUID
		jobQuery := `
			UPDATE scherry_jobs SET status = $1, updated_at = NOW()
			WHERE name = $2 AND id != $3 AND status IN ($4, $5)
			RETURNING id`
		if err := tx.Raw(jobQuery, domain.JobStatusCancelled, jobName, currentJobID, domain.JobStatusPending, domain.JobStatusRunning).
			Scan(&cancelledJobIDs).Error; err != nil {
			return err
		}
		if len(cancelledJobIDs) == 0 {
			return nil
		}
		taskQuery := `
			UPDATE scherry_tasks SET status = $1, updated_at = NOW()
			WHERE job_id = ANY($2) AND status IN ($3, $4, $5)`
		return tx.Exec(taskQuery, domain.TaskStatusCancelled, cancelledJobIDs,
			domain.TaskStatusPending, domain.TaskStatusRunning, domain.TaskStatusFailed).Error
	})
}

func (r *repository) UpdateJobStatus(ctx context.Context, jobID uuid.UUID, status string) error {
	return r.db.WithContext(ctx).Model(&domain.Job{}).Where("id = ?", jobID).
		Updates(map[string]interface{}{"status": status, "updated_at": time.Now()}).Error
}

func (r *repository) UpdateJob(ctx context.Context, jobID uuid.UUID, updates map[string]interface{}) error {
	if len(updates) == 0 {
		return nil
	}
	updates["updated_at"] = time.Now()
	return r.db.WithContext(ctx).Model(&domain.Job{}).Where("id = ?", jobID).Updates(updates).Error
}

// defaultCompletionCheckBatchSize is the fallback limit used when a caller
// passes limit <= 0. In practice executor.CheckJobCompletions always passes
// its own explicit batch size, so this only guards direct callers.
const defaultCompletionCheckBatchSize = 200

func (r *repository) FindCompletableJobIDs(ctx context.Context, jobName string, limit int) ([]uuid.UUID, error) {
	if limit <= 0 {
		limit = defaultCompletionCheckBatchSize
	}
	var ids []uuid.UUID
	err := r.db.WithContext(ctx).Raw(`
		SELECT j.id
		FROM scherry_jobs j
		WHERE j.name = $1 AND j.status = $2
		AND NOT EXISTS (
			SELECT 1 FROM scherry_tasks t
			WHERE t.job_id = j.id
			AND t.status NOT IN ($3, $4, $5, $6, $7)
		)
		ORDER BY j.updated_at ASC
		LIMIT $8`,
		jobName, domain.JobStatusRunning,
		domain.TaskStatusCompleted, domain.TaskStatusFailed, domain.TaskStatusMaxRetriesExhausted, domain.TaskStatusRejected, domain.TaskStatusCancelled,
		limit,
	).Scan(&ids).Error
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (r *repository) UpdateTask(ctx context.Context, taskID uuid.UUID, updates map[string]interface{}) error {
	if len(updates) == 0 {
		return nil
	}
	updates["updated_at"] = time.Now()
	return r.db.WithContext(ctx).Model(&domain.Task{}).Where("id = ?", taskID).Updates(updates).Error
}

func (r *repository) FindPendingJobByParentID(ctx context.Context, parentJobID uuid.UUID) (*domain.Job, error) {
	var job domain.Job
	err := r.db.WithContext(ctx).
		Where("parent_job_id = ? AND status IN ?", parentJobID, domain.NonTerminalJobStates).
		Order("created_at DESC").First(&job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *repository) GetTaskStatusSummary(ctx context.Context, jobID uuid.UUID) (TaskStatusSummary, error) {
	var row struct {
		Total       int `gorm:"column:total"`
		Completed   int `gorm:"column:completed"`
		NonTerminal int `gorm:"column:non_terminal"`
	}
	err := r.db.WithContext(ctx).Raw(`
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE status = $2) AS completed,
			COUNT(*) FILTER (WHERE status NOT IN ($3, $4, $5, $6, $7)) AS non_terminal
		FROM scherry_tasks
		WHERE job_id = $1`,
		jobID,
		domain.TaskStatusCompleted,
		domain.TaskStatusCompleted, domain.TaskStatusFailed, domain.TaskStatusMaxRetriesExhausted, domain.TaskStatusRejected, domain.TaskStatusCancelled,
	).Scan(&row).Error
	if err != nil {
		return TaskStatusSummary{}, err
	}
	return TaskStatusSummary{Total: row.Total, Completed: row.Completed, NonTerminal: row.NonTerminal}, nil
}

// TaskInsertBatchSize caps the number of rows per INSERT statement.
// PostgreSQL allows at most 65535 bind parameters per query; the Task struct
// has 15 columns, so the safe upper bound is 65535/15 = 4369 rows. We use
// 1000 to leave headroom and keep individual statement parse times low.
// Exported because retry.go (root package) reuses it to size retry batches.
const TaskInsertBatchSize = 1000

func (r *repository) CreateJobAndTasks(ctx context.Context, job *domain.Job, tasks []domain.Task) (*domain.Job, []domain.Task, error) {
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(job).Error; err != nil {
			return fmt.Errorf("create job: %w", err)
		}
		if len(tasks) > 0 {
			if err := tx.CreateInBatches(&tasks, TaskInsertBatchSize).Error; err != nil {
				return fmt.Errorf("create tasks: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return job, tasks, nil
}
