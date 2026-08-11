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

func isTaskTerminal(status string) bool {
	switch status {
	case domain.TaskStatusCompleted, domain.TaskStatusFailed, domain.TaskStatusMaxRetriesExhausted, domain.TaskStatusRejected, domain.TaskStatusCancelled:
		return true
	default:
		return false
	}
}

func resolveJobTerminalStatus(tasks []domain.Task) (status string, ready bool) {
	if len(tasks) == 0 {
		return domain.JobStatusCompleted, true
	}

	completed, failed := 0, 0
	for _, task := range tasks {
		if !isTaskTerminal(task.Status) {
			return "", false
		}
		switch task.Status {
		case domain.TaskStatusCompleted:
			completed++
		default:
			failed++
		}
	}

	switch {
	case failed == 0:
		return domain.JobStatusCompleted, true
	case completed == 0:
		return domain.JobStatusFailed, true
	default:
		return domain.JobStatusPartialFailed, true
	}
}

func jobStatusToAction(status string) string {
	switch status {
	case domain.JobStatusCompleted:
		return domain.JobActionComplete
	case domain.JobStatusFailed:
		return domain.JobActionFail
	case domain.JobStatusPartialFailed:
		return domain.JobActionPartialFail
	default:
		return ""
	}
}

func (d *deps) transitionJob(ctx context.Context, job *domain.Job, action string) error {
	switch action {
	case domain.JobActionStart:
		if err := job.Start(ctx); err != nil {
			return fmt.Errorf("job %s transition %s: %w", job.ID, action, err)
		}
	case domain.JobActionComplete:
		if err := job.Complete(ctx); err != nil {
			return fmt.Errorf("job %s transition %s: %w", job.ID, action, err)
		}
	case domain.JobActionFail:
		if err := job.Fail(ctx); err != nil {
			return fmt.Errorf("job %s transition %s: %w", job.ID, action, err)
		}
	case domain.JobActionPartialFail:
		if err := job.PartialFail(ctx); err != nil {
			return fmt.Errorf("job %s transition %s: %w", job.ID, action, err)
		}
	case domain.JobActionCancel:
		if err := job.Cancel(ctx); err != nil {
			return fmt.Errorf("job %s transition %s: %w", job.ID, action, err)
		}
	default:
		return fmt.Errorf("unknown job action: %s", action)
	}
	return d.repo.UpdateJob(ctx, job.ID, jobTimingUpdates(job, action))
}

// jobTimingUpdates builds the persisted column set for a job transition:
// always the new status, plus started_at when the job begins running and
// finished_at/duration_ms when it reaches a terminal state.
func jobTimingUpdates(job *domain.Job, action string) map[string]interface{} {
	updates := map[string]interface{}{statusField: job.Status}
	now := time.Now()
	switch action {
	case domain.JobActionStart:
		job.StartedAt = &now
		updates["started_at"] = now
	case domain.JobActionComplete, domain.JobActionFail, domain.JobActionPartialFail, domain.JobActionCancel:
		job.FinishedAt = &now
		updates["finished_at"] = now
		if job.StartedAt != nil {
			d := now.Sub(*job.StartedAt).Milliseconds()
			job.DurationMs = &d
			updates["duration_ms"] = d
		}
	}
	return updates
}

// resolveJobTerminalStatusFromSummary determines whether a job has reached a
// terminal state using aggregate counts rather than a full task row-set. This
// avoids the O(T) heap fetch that resolveJobTerminalStatus requires, reducing
// the per-task finalization cost from O(T) rows to a single scalar query.
func resolveJobTerminalStatusFromSummary(s repository.TaskStatusSummary) (status string, ready bool) {
	if s.Total == 0 {
		return domain.JobStatusCompleted, true
	}
	if s.NonTerminal > 0 {
		return "", false
	}
	failed := s.Total - s.Completed
	switch {
	case failed == 0:
		return domain.JobStatusCompleted, true
	case s.Completed == 0:
		return domain.JobStatusFailed, true
	default:
		return domain.JobStatusPartialFailed, true
	}
}

// defaultCompletionCheckBatchSize caps how many of a job's RUNNING instances
// a single CheckJobCompletions call will process, so a large backlog is
// drained incrementally across cron ticks rather than in one unbounded query.
const defaultCompletionCheckBatchSize = 200

// completionCheckTaskSuffix distinguishes a job's periodic completion-check
// asynq task from the job's own scheduled-execution task (which uses the
// job's name directly as its type).
const completionCheckTaskSuffix = ":completion-check"

// CompletionCheckTaskType returns the asynq task type used for jobName's
// JobCompletionCron entry.
func CompletionCheckTaskType(jobName string) string {
	return jobName + completionCheckTaskSuffix
}

// CheckJobCompletions is the handler for a job's JobCompletionCron: instead
// of re-checking a job's readiness after every single task completes, jobs
// configured with JobCompletionCron are checked in a single periodic batch
// here instead. It finds up to defaultCompletionCheckBatchSize of jobName's
// RUNNING instances whose tasks have all reached a terminal state (or which
// have no tasks at all) and finalizes each of them exactly like
// maybeFinalizeJob always has: the terminal FSM transition is persisted and
// OnJobFinished fires.
//
// A per-job error is logged and skipped rather than aborting the batch, so
// one bad row can't block every other job's completion check on the same
// tick.
func CheckJobCompletions(ctx context.Context, repo repository.Repository, logger logging.Logger, jobs map[string]jobconfig.JobConfig, jobName string) error {
	d := &deps{repo: repo, logger: logger, jobs: jobs}
	return d.checkJobCompletions(ctx, jobName)
}

func (d *deps) checkJobCompletions(ctx context.Context, jobName string) error {
	jobIDs, err := d.repo.FindCompletableJobIDs(ctx, jobName, defaultCompletionCheckBatchSize)
	if err != nil {
		return fmt.Errorf("find completable jobs for %s: %w", jobName, err)
	}
	for _, jobID := range jobIDs {
		if err := d.maybeFinalizeJob(ctx, jobID); err != nil {
			d.logger.Error("check job completion failed", "job_id", jobID, "job_name", jobName, "error", err)
		}
	}
	return nil
}

func (d *deps) maybeFinalizeJob(ctx context.Context, jobID uuid.UUID) error {
	job, err := d.repo.FindJobByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("find job for finalize: %w", err)
	}
	if !job.IsRunning() {
		return nil
	}

	summary, err := d.repo.GetTaskStatusSummary(ctx, jobID)
	if err != nil {
		return fmt.Errorf("get task summary for finalize: %w", err)
	}

	status, ready := resolveJobTerminalStatusFromSummary(summary)
	if !ready {
		return nil
	}

	action := jobStatusToAction(status)
	if action == "" {
		return fmt.Errorf("unsupported terminal job status: %s", status)
	}
	if err := d.transitionJob(ctx, job, action); err != nil {
		return err
	}

	refID := ""
	if job.UniqueReferenceID != nil {
		refID = *job.UniqueReferenceID
	}
	d.logger.Info("job finalized", "job_id", job.ID, "status", job.Status, "tasks", summary.Total)
	if jobCfg, ok := d.jobs[job.Name]; ok {
		jobCfg.Hooks.OnJobFinished(ctx, job.Name, refID, job.Status, 0)
	}
	return nil
}
