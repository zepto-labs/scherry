package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/looplab/fsm"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
)

// statusField is the GORM column name shared by the update-map helpers below.
const statusField = "status"

// ExecuteTask runs a single task message end-to-end: loads the task and its
// job, executes it via the registered TaskExecutor, and persists the
// outcome (including retry/DLQ routing on failure).
func ExecuteTask(ctx context.Context, repo repository.Repository, logger logging.Logger, jobs map[string]jobconfig.JobConfig, message []byte) error {
	d := &deps{repo: repo, logger: logger, jobs: jobs}
	return d.executeTask(ctx, message)
}

func (d *deps) executeTask(ctx context.Context, message []byte) error {
	start := time.Now()
	task, err := parseTaskMessage(message)
	if err != nil {
		return err
	}

	job, err := d.loadJobForTask(ctx, task)
	if err != nil {
		return err
	}

	jobCfg, ok := d.jobs[job.Name]
	if !ok {
		return fmt.Errorf("no job config registered for job type: %s", job.Name)
	}

	if !job.IsExecutionPending() {
		d.logger.Info("skip task for non-pending job", "job_id", job.ID, "task_id", task.ID, "job_status", job.Status)
		d.updateTasksStatus(ctx, []domain.Task{*task}, job.Status)
		jobCfg.Hooks.OnTaskFinished(ctx, job.Name, task.ID.String(), "skipped", time.Since(start))
		return nil
	}

	if err := d.startTaskExecution(ctx, task); err != nil {
		jobCfg.Hooks.OnTaskFinished(ctx, job.Name, task.ID.String(), "error", time.Since(start))
		return err
	}
	jobCfg.Hooks.OnTaskStarted(ctx, job.Name, task.ID.String(), task.Attempt)

	params, err := task.GetParams()
	if err != nil {
		wrappedErr := fmt.Errorf("get task params: %w", err)
		d.failTaskWithResult(ctx, task, wrappedErr)
		return wrappedErr
	}

	output, err := jobCfg.TaskExecutor.Execute(ctx, params)
	if err != nil {
		d.handleTaskFailure(ctx, task, err, jobCfg)
		jobCfg.Hooks.OnTaskFinished(ctx, job.Name, task.ID.String(), domain.TaskStatusFailed, time.Since(start))
		return nil
	}

	if err := d.completeTask(ctx, task, output); err != nil {
		jobCfg.Hooks.OnTaskFinished(ctx, job.Name, task.ID.String(), "error", time.Since(start))
		return err
	}
	jobCfg.Hooks.OnTaskFinished(ctx, job.Name, task.ID.String(), domain.TaskStatusCompleted, time.Since(start))
	return nil
}

func parseTaskMessage(message []byte) (*domain.Task, error) {
	var task domain.Task
	if err := json.Unmarshal(message, &task); err != nil {
		return nil, fmt.Errorf("unmarshal task: %w", err)
	}
	return &task, nil
}

func (d *deps) loadJobForTask(ctx context.Context, task *domain.Task) (*domain.Job, error) {
	reloaded, job, err := d.repo.FindTaskWithJob(ctx, task.ID)
	if err != nil {
		return nil, fmt.Errorf("load task and job: %w", err)
	}
	if job == nil {
		return nil, fmt.Errorf("job not found for task %s (job_id %s)", task.ID, task.JobID)
	}
	*task = *reloaded
	return job, nil
}

func (d *deps) startTaskExecution(ctx context.Context, task *domain.Task) error {
	d.logger.Info("executing task", "task_id", task.ID, "job_id", task.JobID, "attempt", task.Attempt)
	if err := task.Start(ctx); err != nil && !isNoTransition(err) {
		return err
	}
	now := time.Now()
	task.StartedAt = &now
	return d.repo.UpdateTask(ctx, task.ID, map[string]interface{}{statusField: task.Status, "started_at": now})
}

// taskFinishUpdates adds finished_at and duration_ms (measured from the task's
// started_at for the latest attempt) to a task update map on a terminal outcome.
func taskFinishUpdates(task *domain.Task, updates map[string]interface{}) map[string]interface{} {
	now := time.Now()
	task.FinishedAt = &now
	updates["finished_at"] = now
	if task.StartedAt != nil {
		d := now.Sub(*task.StartedAt).Milliseconds()
		task.DurationMs = &d
		updates["duration_ms"] = d
	}
	return updates
}

func isNoTransition(err error) bool {
	var nt fsm.NoTransitionError
	return errors.As(err, &nt) && nt.Err == nil
}

func (d *deps) completeTask(ctx context.Context, task *domain.Task, output map[string]interface{}) error {
	if err := task.Complete(ctx); err != nil {
		return fmt.Errorf("complete task: %w", err)
	}
	result := map[string]interface{}{"error": nil, "response": output}
	if err := task.SetResult(result); err != nil {
		d.logger.Warn("set task result failed", "task_id", task.ID, "error", err)
	}
	return d.repo.UpdateTask(ctx, task.ID, taskFinishUpdates(task, map[string]interface{}{statusField: task.Status, "result": task.Result}))
}

func (d *deps) handleTaskFailure(ctx context.Context, task *domain.Task, execErr error, jobCfg jobconfig.JobConfig) {
	d.updateTaskStatus(ctx, task, domain.TaskStatusFailed)
	d.logger.Warn("task failed", "task_id", task.ID, "error", execErr)

	if task.Attempt < task.MaxRetries {
		d.retryTask(ctx, task, execErr, jobCfg)
		return
	}
	d.sendTaskToDLQ(ctx, task, execErr, jobCfg)
}

func (d *deps) retryTask(ctx context.Context, task *domain.Task, execErr error, jobCfg jobconfig.JobConfig) {
	result := map[string]interface{}{"error": execErr.Error(), "response": nil}
	_ = task.SetResult(result)
	if err := task.Retry(ctx); err != nil {
		d.logger.Error("mark task retrying failed", "task_id", task.ID, "error", err)
		return
	}
	task.Attempt++
	if err := d.repo.UpdateTask(ctx, task.ID, map[string]interface{}{
		"attempt": task.Attempt, "result": task.Result, statusField: task.Status,
	}); err != nil {
		d.logger.Error("update task for retry failed", "task_id", task.ID, "error", err)
	}
	if jobCfg.Kafka.TaskRetryTopic == "" {
		d.logger.Warn("retry topic not configured", "task_id", task.ID)
		return
	}
	if err := jobCfg.Publisher.PublishRetryTasks(ctx, []domain.Task{*task}, jobCfg.Kafka.TaskRetryTopic, jobCfg.Retry.RetryDelay); err != nil {
		d.logger.Error("publish retry task failed", "task_id", task.ID, "error", err)
	}
}

func (d *deps) sendTaskToDLQ(ctx context.Context, task *domain.Task, execErr error, jobCfg jobconfig.JobConfig) {
	d.logger.Error("max retries reached, sending to DLQ", "task_id", task.ID, "attempt", task.Attempt)
	_ = task.ExhaustRetries(ctx)
	result := map[string]interface{}{"error": execErr.Error(), "response": nil}
	_ = task.SetResult(result)
	_ = d.repo.UpdateTask(ctx, task.ID, taskFinishUpdates(task, map[string]interface{}{"result": task.Result, statusField: task.Status}))

	if jobCfg.Kafka.TaskDLQTopic == "" {
		d.logger.Warn("DLQ topic not configured", "task_id", task.ID)
		return
	}
	if err := jobCfg.Publisher.PublishTasks(ctx, []domain.Task{*task}, jobCfg.Kafka.TaskDLQTopic); err != nil {
		d.logger.Error("publish DLQ task failed", "task_id", task.ID, "error", err)
	}
}

func (d *deps) updateTaskStatus(ctx context.Context, task *domain.Task, status string) {
	task.Status = status
	if err := d.repo.UpdateTask(ctx, task.ID, map[string]interface{}{statusField: status}); err != nil {
		d.logger.Error("update task status failed", "task_id", task.ID, "error", err)
	}
}

// failTaskWithResult marks a task as failed and persists the error message in the result column.
func (d *deps) failTaskWithResult(ctx context.Context, task *domain.Task, execErr error) {
	task.Status = domain.TaskStatusFailed
	result := map[string]interface{}{"error": execErr.Error(), "response": nil}
	if err := task.SetResult(result); err != nil {
		d.logger.Warn("set task result failed", "task_id", task.ID, "error", err)
	}
	updates := taskFinishUpdates(task, map[string]interface{}{statusField: task.Status, "result": task.Result})
	if err := d.repo.UpdateTask(ctx, task.ID, updates); err != nil {
		d.logger.Error("update task status failed", "task_id", task.ID, "error", err)
	}
}
