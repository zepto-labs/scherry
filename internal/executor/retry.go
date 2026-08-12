package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
)

type RetryJobPayload struct {
	JobID     string `json:"job_id"`
	FullRetry bool   `json:"full_retry"`
}

// retryBatchSize is the number of tasks read, cloned, and published per
// iteration of the streaming retry loop. Kept equal to
// repository.TaskInsertBatchSize so each create batch stays within the
// PostgreSQL 65535 parameter limit and peak memory per iteration is bounded
// to ~1000 Task structs regardless of total T.
const retryBatchSize = repository.TaskInsertBatchSize

// RetryJob re-publishes the tasks of a terminal job as a new cloned job,
// streaming source tasks in batches so peak memory stays bounded regardless
// of the original job's task count. fullRetry selects every task; otherwise
// only tasks that exhausted their retries are retried.
func RetryJob(ctx context.Context, repo repository.Repository, logger logging.Logger, jobs map[string]jobconfig.JobConfig, jobID uuid.UUID, fullRetry bool, refID string) (*domain.Job, error) {
	d := &deps{repo: repo, logger: logger, jobs: jobs}
	return d.retryJob(ctx, jobID, fullRetry, refID)
}

func (d *deps) retryJob(ctx context.Context, jobID uuid.UUID, fullRetry bool, refID string) (*domain.Job, error) {
	originalJob, err := d.repo.FindJobByID(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("find original job %s: %w", jobID, err)
	}
	if originalJob.IsExecutionPending() {
		return nil, fmt.Errorf("job %s is not in terminal state", jobID)
	}

	clonedJob, taskCount, err := d.prepareAndStreamRetry(ctx, originalJob, fullRetry, refID)
	if err != nil {
		return nil, fmt.Errorf("stream retry: %w", err)
	}
	if clonedJob == nil {
		return nil, nil
	}
	if clonedJob.IsRunning() {
		d.logger.Info("cloned job already running", "job_id", clonedJob.ID)
		return nil, nil
	}
	if clonedJob.IsPending() {
		d.startClonedJob(ctx, clonedJob)
		if taskCount == 0 {
			// A cloned job with no tasks has nothing to ever trigger a future
			// completion check, so finalize it immediately — same as executeJobFlow.
			if err := d.maybeFinalizeJob(ctx, clonedJob.ID); err != nil {
				d.logger.Error("finalize empty cloned job failed", "job_id", clonedJob.ID, "error", err)
			}
		}
		d.logger.Info("retried job", "original_job_id", jobID, "cloned_job_id", clonedJob.ID)
	}
	return clonedJob, nil
}

// prepareAndStreamRetry either reuses an existing pending clone or creates a new
// one and fills it by streaming tasks from the original job in batches.
// Each batch is: read retryBatchSize source tasks → insert cloned rows → publish
// to Kafka. Peak memory is bounded to retryBatchSize Task structs at a time,
// making the retry path safe regardless of total task count.
func (d *deps) prepareAndStreamRetry(ctx context.Context, originalJob *domain.Job, fullRetry bool, refID string) (*domain.Job, int, error) {
	jobCfg, ok := d.jobs[originalJob.Name]
	if !ok {
		return nil, 0, fmt.Errorf("no job config for %s", originalJob.Name)
	}

	// Reuse a pending clone from a previous (possibly incomplete) attempt rather
	// than cloning twice, and re-publish its tasks in batches in case publishing
	// was interrupted mid-way. Like the refID check in executeJob this is a
	// best-effort read-then-insert with no unique constraint behind it, so
	// concurrent retries of the same job can still each create a clone.
	existingJob, err := d.repo.FindPendingJobByParentID(ctx, originalJob.ID)
	if err != nil {
		return nil, 0, fmt.Errorf("check pending clone: %w", err)
	}
	if existingJob != nil {
		d.logger.Info("reusing pending cloned job", "job_id", existingJob.ID, "parent_job_id", originalJob.ID)
		if err := d.streamPublishTasks(ctx, existingJob.ID, jobCfg); err != nil {
			return nil, 0, fmt.Errorf("re-publish existing clone tasks: %w", err)
		}
		summary, err := d.repo.GetTaskStatusSummary(ctx, existingJob.ID)
		if err != nil {
			return nil, 0, fmt.Errorf("count existing clone tasks: %w", err)
		}
		return existingJob, summary.Total, nil
	}

	// Pre-create the cloned job row (no tasks yet) so it exists in DB before
	// any tasks are inserted. If the process crashes mid-stream, the next call
	// finds the PENDING clone via FindPendingJobByParentID and re-publishes.
	clonedJob := &domain.Job{
		ID: uuid.New(), ParentJobID: &originalJob.ID, Name: originalJob.Name,
		Status: domain.JobStatusPending, MaxRetries: originalJob.MaxRetries,
		Metadata: originalJob.Metadata, UniqueReferenceID: &refID,
	}
	if _, _, err := d.repo.CreateJobAndTasks(ctx, clonedJob, nil); err != nil {
		return nil, 0, fmt.Errorf("create cloned job: %w", err)
	}

	statuses := retryStatuses(fullRetry)
	total, err := d.streamCloneAndPublish(ctx, originalJob.ID, clonedJob, statuses, jobCfg)
	if err != nil {
		return nil, total, err
	}
	if total == 0 {
		d.logger.Info("retry produced no tasks", "original_job_id", originalJob.ID, "cloned_job_id", clonedJob.ID)
	}
	return clonedJob, total, nil
}

// retryStatuses returns the task status filter for a retry operation.
// Full retry retries every task; partial retry targets only exhausted ones.
func retryStatuses(fullRetry bool) []string {
	if fullRetry {
		return nil
	}
	return []string{domain.TaskStatusMaxRetriesExhausted}
}

// streamCloneAndPublish iterates source tasks in pages of retryBatchSize,
// cloning and publishing each page before moving to the next.
func (d *deps) streamCloneAndPublish(ctx context.Context, srcJobID uuid.UUID, clonedJob *domain.Job, statuses []string, jobCfg jobconfig.JobConfig) (int, error) {
	afterSeq := -1
	total := 0
	for {
		srcBatch, err := d.repo.FindTasksByJobIDAndStatusesBatch(ctx, srcJobID, statuses, afterSeq, retryBatchSize)
		if err != nil {
			return total, fmt.Errorf("read source tasks (afterSeq=%d): %w", afterSeq, err)
		}
		if len(srcBatch) == 0 {
			break
		}

		clonedBatch := make([]domain.Task, len(srcBatch))
		for i, t := range srcBatch {
			clonedBatch[i] = domain.Task{
				ID: uuid.New(), JobID: clonedJob.ID, Name: t.Name,
				Status: domain.TaskStatusPending, Attempt: 1, MaxRetries: t.MaxRetries,
				SequenceNumber: t.SequenceNumber,
				Params:         t.Params, Result: t.Result,
				DistributionKey: t.DistributionKey,
				// CreatedAt must equal the job's CreatedAt to satisfy the composite
				// FK (JobID, CreatedAt) used for partition-pruned job lookups.
				// The streaming retry creates the job and tasks in separate
				// transactions, so without this explicit assignment GORM would let
				// the DB assign NOW() to each batch — producing task.created_at
				// values that diverge from job.created_at and cause FindTaskWithJob
				// to miss the job row entirely.
				CreatedAt: clonedJob.CreatedAt,
			}
			afterSeq = t.SequenceNumber
		}

		if err := d.repo.CreateTasks(ctx, clonedBatch); err != nil {
			return total, fmt.Errorf("create cloned tasks batch: %w", err)
		}
		if err := jobCfg.Publisher.PublishTasks(ctx, clonedBatch, jobCfg.Kafka.TaskTopic); err != nil {
			return total, fmt.Errorf("publish cloned tasks batch: %w", err)
		}
		total += len(clonedBatch)

		if len(srcBatch) < retryBatchSize {
			break
		}
	}
	return total, nil
}

// streamPublishTasks publishes tasks for a job in batches of retryBatchSize.
// Used when resuming a previously interrupted retry to re-enqueue tasks that
// were created in DB but may not have reached Kafka.
func (d *deps) streamPublishTasks(ctx context.Context, jobID uuid.UUID, jobCfg jobconfig.JobConfig) error {
	afterSeq := -1
	for {
		batch, err := d.repo.FindTasksByJobIDAndStatusesBatch(ctx, jobID, nil, afterSeq, retryBatchSize)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		if err := jobCfg.Publisher.PublishTasks(ctx, batch, jobCfg.Kafka.TaskTopic); err != nil {
			return err
		}
		afterSeq = batch[len(batch)-1].SequenceNumber
		if len(batch) < retryBatchSize {
			break
		}
	}
	return nil
}

func (d *deps) startClonedJob(ctx context.Context, job *domain.Job) {
	if err := d.transitionJob(ctx, job, domain.JobActionStart); err != nil {
		d.logger.Error("start cloned job failed", "job_id", job.ID, "error", err)
	}
}

// RegisterRetryHandler registers an Asynq handler for retryJobType that
// invokes RetryJob using the Asynq task ID as the deduplication ref.
func RegisterRetryHandler(mux *asynq.ServeMux, retryJobType string, repo repository.Repository, logger logging.Logger, jobs map[string]jobconfig.JobConfig) {
	mux.HandleFunc(retryJobType, func(ctx context.Context, task *asynq.Task) error {
		start := time.Now()
		var payload RetryJobPayload
		if err := json.Unmarshal(task.Payload(), &payload); err != nil {
			return err
		}
		jobID, err := uuid.Parse(payload.JobID)
		if err != nil {
			return err
		}
		taskID, _ := asynq.GetTaskID(ctx)
		_, retryErr := RetryJob(ctx, repo, logger, jobs, jobID, payload.FullRetry, taskID)
		if retryErr != nil {
			logger.Error("retry job handler failed", "job_id", payload.JobID, "error", retryErr, "duration", time.Since(start))
			return retryErr
		}
		logger.Info("retry job handler completed", "job_id", payload.JobID, "duration", time.Since(start))
		return nil
	})
}
