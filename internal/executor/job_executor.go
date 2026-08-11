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

// ExecuteJob runs jobName's JobExecutor, persists the resulting job and
// tasks, and publishes them to Kafka. jobs is the caller's full registered-job
// map; refID is the idempotency key used to detect an already-executed job.
func ExecuteJob(ctx context.Context, repo repository.Repository, logger logging.Logger, jobs map[string]jobconfig.JobConfig, jobName, refID string, metadata map[string]interface{}) error {
	d := &deps{repo: repo, logger: logger, jobs: jobs}
	return d.executeJob(ctx, jobName, refID, metadata)
}

func (d *deps) executeJob(ctx context.Context, jobName, refID string, metadata map[string]interface{}) error {
	start := time.Now()
	jobCfg, ok := d.jobs[jobName]
	if !ok {
		return fmt.Errorf("no job config registered for job type: %s", jobName)
	}

	job, tasks, err := d.prepareJobAndTasks(ctx, jobCfg, metadata, refID)
	if err != nil {
		jobCfg.Hooks.OnJobFinished(ctx, jobName, refID, domain.JobStatusFailed, time.Since(start))
		return err
	}

	err = d.executeJobFlow(ctx, job, tasks, jobCfg)
	status := domain.JobStatusRunning
	if err != nil {
		status = domain.JobStatusFailed
	} else if !job.IsExecutionPending() {
		status = job.Status
	}
	jobCfg.Hooks.OnJobFinished(ctx, jobName, refID, status, time.Since(start))
	return err
}

func (d *deps) prepareJobAndTasks(ctx context.Context, jobCfg jobconfig.JobConfig, metadata map[string]interface{}, refID string) (*domain.Job, []domain.Task, error) {
	existingJob, existingTasks, err := d.repo.FindJobWithTasksByUniqueReferenceID(ctx, refID)
	if err != nil {
		return nil, nil, fmt.Errorf("check existing job: %w", err)
	}
	if existingJob != nil {
		d.logger.Info("found existing job", "job_id", existingJob.ID, "ref_id", refID, "status", existingJob.Status, "tasks", len(existingTasks))
		return existingJob, existingTasks, nil
	}

	job := &domain.Job{
		ID:                uuid.New(),
		Name:              jobCfg.Name,
		MaxRetries:        jobCfg.Retry.MaxRetries,
		UniqueReferenceID: &refID,
	}
	setInitialJobStatus(job, jobconfig.IsEnabled(jobCfg))
	if err := job.SetMetadata(metadata); err != nil {
		return nil, nil, fmt.Errorf("set job metadata: %w", err)
	}

	d.logger.Info("executing job", "name", jobCfg.Name, "job_id", job.ID)
	taskData, err := jobCfg.JobExecutor.Execute(ctx, metadata)
	if err != nil {
		d.logger.Error("job executor failed", "job_id", job.ID, "error", err)
		if shouldPersistTriggerFailure(metadata) {
			if persistErr := d.persistTriggerFailure(ctx, job, jobCfg, refID, metadata, "job_executor", err); persistErr != nil {
				d.logger.Error("persist trigger failure", "job_id", job.ID, "error", persistErr)
			}
		}
		return nil, nil, err
	}

	tasks, err := d.buildTasks(job, taskData)
	if err != nil {
		if shouldPersistTriggerFailure(metadata) {
			if persistErr := d.persistTriggerFailure(ctx, job, jobCfg, refID, metadata, "build_tasks", err); persistErr != nil {
				d.logger.Error("persist trigger failure", "job_id", job.ID, "error", persistErr)
			}
		}
		return nil, nil, err
	}

	createdJob, createdTasks, err := d.repo.CreateJobAndTasks(ctx, job, tasks)
	if err != nil {
		return nil, nil, fmt.Errorf("create job and tasks: %w", err)
	}
	d.logger.Info("created job with tasks", "job_id", createdJob.ID, "task_count", len(createdTasks))
	return createdJob, createdTasks, nil
}

func (d *deps) buildTasks(job *domain.Job, taskData []jobconfig.TaskData) ([]domain.Task, error) {
	tasks := make([]domain.Task, 0, len(taskData))
	for i, data := range taskData {
		task := domain.Task{
			ID: uuid.New(), JobID: job.ID, Name: data.Name, Attempt: 1, MaxRetries: job.MaxRetries,
			SequenceNumber: i,
		}
		if data.DistributionKey != "" {
			task.DistributionKey = &data.DistributionKey
		}
		setInitialTaskStatus(&task, job)
		if err := task.SetParams(data.Params); err != nil {
			return nil, fmt.Errorf("set task params: %w", err)
		}
		if err := task.SetResult(map[string]interface{}{}); err != nil {
			return nil, fmt.Errorf("set task result: %w", err)
		}
		tasks = append(tasks, task)
	}
	if len(tasks) == 0 {
		d.logger.Info("job produced no tasks", "job_id", job.ID)
	}
	return tasks, nil
}

func setInitialJobStatus(job *domain.Job, enabled bool) {
	job.Status = domain.JobStatusPending
	if !enabled {
		job.Status = domain.JobStatusRejected
	}
}

func setInitialTaskStatus(task *domain.Task, job *domain.Job) {
	switch {
	case job.IsExecutionPending():
		task.Status = domain.TaskStatusPending
	case job.IsRejected():
		task.Status = domain.TaskStatusRejected
	case job.IsCancelled():
		task.Status = domain.TaskStatusCancelled
	}
}

func (d *deps) executeJobFlow(ctx context.Context, job *domain.Job, tasks []domain.Task, jobCfg jobconfig.JobConfig) error {
	if !job.IsExecutionPending() {
		// Use a job_id-based bulk UPDATE rather than WHERE id IN (N UUIDs) to avoid
		// a bind-parameter explosion at large T (e.g. 100k tasks × 1 id each).
		if err := d.repo.UpdateTasksStatusByJobID(ctx, job.ID, job.Status); err != nil {
			d.logger.Error("update tasks status failed", "status", job.Status, "error", err)
		}
		d.logger.Info("job marked non-pending", "job_id", job.ID, "status", job.Status)
		return nil
	}
	if job.IsRunning() {
		d.logger.Info("job already running", "job_id", job.ID)
		return nil
	}
	if err := d.publishTasks(ctx, job, tasks, jobCfg); err != nil {
		if transErr := d.transitionJob(ctx, job, domain.JobActionFail); transErr != nil {
			d.logger.Error("mark job failed", "job_id", job.ID, "error", transErr)
		}
		return err
	}
	if jobCfg.CancelPrevious {
		if err := d.repo.CancelPreviousJobs(ctx, job.Name, job.ID); err != nil {
			d.logger.Error("cancel previous jobs failed", "job", job.Name, "error", err)
		}
	}
	if err := d.transitionJob(ctx, job, domain.JobActionStart); err != nil {
		return fmt.Errorf("start job: %w", err)
	}
	d.logger.Info("job tasks published", "job_id", job.ID, "tasks", len(tasks))
	if len(tasks) == 0 {
		// A job with no tasks has nothing to ever trigger a future check: no
		// task will complete to wake it, so it would otherwise just sit
		// RUNNING until the next JobCompletionCron tick. Finalize it
		// immediately here instead — this is a one-time "there was never
		// anything to wait for" case, not the per-task-completion re-check
		// this design intentionally removed.
		if err := d.maybeFinalizeJob(ctx, job.ID); err != nil {
			d.logger.Error("finalize job failed", "job_id", job.ID, "error", err)
		}
	}
	return nil
}

func (d *deps) publishTasks(ctx context.Context, job *domain.Job, tasks []domain.Task, jobCfg jobconfig.JobConfig) error {
	if len(tasks) == 0 {
		return nil
	}
	// Publish in batches so kafkago.Message slice allocation is bounded to
	// repository.TaskInsertBatchSize per call rather than T messages allocated at once.
	for i := 0; i < len(tasks); i += repository.TaskInsertBatchSize {
		end := i + repository.TaskInsertBatchSize
		if end > len(tasks) {
			end = len(tasks)
		}
		if err := jobCfg.Publisher.PublishTasks(ctx, tasks[i:end], jobCfg.Kafka.TaskTopic); err != nil {
			d.logger.Error("publish tasks failed", "job_id", job.ID, "error", err)
			return err
		}
	}
	jobCfg.Hooks.OnTasksPublished(ctx, jobCfg.Name, len(tasks))
	return nil
}

func (d *deps) updateTasksStatus(ctx context.Context, tasks []domain.Task, status string) {
	if len(tasks) == 0 {
		return
	}
	ids := make([]uuid.UUID, len(tasks))
	for i, task := range tasks {
		ids[i] = task.ID
	}
	if err := d.repo.UpdateTasksStatus(ctx, ids, status); err != nil {
		d.logger.Error("update tasks status failed", "status", status, "error", err)
	}
}
