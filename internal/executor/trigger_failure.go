package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
)

const (
	metadataTriggerFailure = "trigger_failure"
	metadataFailurePhase   = "failure_phase"
	metadataFailureError   = "error"
	metadataAsynqTaskID    = "asynq_task_id"
	metadataAsynqRetryCnt  = "asynq_retry_count"
	metadataAsynqMaxRetry  = "asynq_max_retry"
)

// IsTriggerFailureJob reports whether job was persisted after a trigger-time
// failure (JobExecutor.Execute or task build) before any tasks were created.
func IsTriggerFailureJob(job *domain.Job) bool {
	if job == nil {
		return false
	}
	meta, err := job.MetadataMap()
	if err != nil || meta == nil {
		return false
	}
	v, ok := meta[metadataTriggerFailure].(bool)
	return ok && v
}

// shouldPersistTriggerFailure decides whether a failed trigger attempt should
// be written to Postgres. Asynq retries share the same refID; intermediate
// failures stay visible via the Asynq inspector merge until the final attempt.
func shouldPersistTriggerFailure(metadata map[string]interface{}) bool {
	maxRetry, hasMax := metadataInt(metadata, metadataAsynqMaxRetry)
	if !hasMax {
		return true
	}
	retryCount, _ := metadataInt(metadata, metadataAsynqRetryCnt)
	return retryCount >= maxRetry
}

func metadataInt(metadata map[string]interface{}, key string) (int, bool) {
	if metadata == nil {
		return 0, false
	}
	v, ok := metadata[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}

func (d *deps) persistTriggerFailure(ctx context.Context, job *domain.Job, jobCfg jobconfig.JobConfig, refID string, metadata map[string]interface{}, phase string, execErr error) error {
	existing, _, err := d.repo.FindJobWithTasksByUniqueReferenceID(ctx, refID)
	if err != nil {
		return fmt.Errorf("check existing trigger failure: %w", err)
	}
	if existing != nil {
		return nil
	}

	meta := make(map[string]interface{}, len(metadata)+4)
	for k, v := range metadata {
		meta[k] = v
	}
	meta[metadataTriggerFailure] = true
	meta[metadataFailurePhase] = phase
	meta[metadataFailureError] = execErr.Error()
	if _, ok := meta[metadataAsynqTaskID]; !ok {
		meta[metadataAsynqTaskID] = refID
	}

	if err := job.SetMetadata(meta); err != nil {
		return fmt.Errorf("set trigger failure metadata: %w", err)
	}

	now := time.Now()
	job.StartedAt = &now
	job.FinishedAt = &now
	zero := int64(0)
	job.DurationMs = &zero
	job.Status = domain.JobStatusFailed

	_, _, err = d.repo.CreateJobAndTasks(ctx, job, nil)
	if err != nil {
		return fmt.Errorf("create trigger failure job: %w", err)
	}
	d.logger.Info("persisted trigger failure", "job_id", job.ID, "name", jobCfg.Name, "phase", phase)
	return nil
}

// RetryTriggerJob re-runs a trigger-failure job (zero tasks) via ExecuteJob.
func RetryTriggerJob(ctx context.Context, repo repository.Repository, logger logging.Logger, jobs map[string]jobconfig.JobConfig, jobID uuid.UUID, refID string) (*domain.Job, error) {
	d := &deps{repo: repo, logger: logger, jobs: jobs}
	return d.retryTriggerJob(ctx, jobID, refID)
}

func (d *deps) retryTriggerJob(ctx context.Context, jobID uuid.UUID, refID string) (*domain.Job, error) {
	original, err := d.repo.FindJobByID(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("find original job %s: %w", jobID, err)
	}
	if !IsTriggerFailureJob(original) {
		return nil, fmt.Errorf("job %s is not a trigger failure job", jobID)
	}
	if original.IsExecutionPending() {
		return nil, fmt.Errorf("job %s is not in terminal state", jobID)
	}

	meta := map[string]interface{}{
		"retry_of_job_id": original.ID.String(),
	}
	if original.UniqueReferenceID != nil {
		meta["retry_of_ref_id"] = *original.UniqueReferenceID
	}

	if err := d.executeJob(ctx, original.Name, refID, meta); err != nil {
		return nil, err
	}
	job, _, err := d.repo.FindJobWithTasksByUniqueReferenceID(ctx, refID)
	if err != nil {
		return nil, fmt.Errorf("find retried job: %w", err)
	}
	if job == nil {
		return nil, fmt.Errorf("retried job not found for ref %s", refID)
	}
	return job, nil
}
