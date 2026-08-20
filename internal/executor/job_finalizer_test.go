package executor

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
	"github.com/zepto-labs/scherry/internal/testutil"
)

func TestResolveJobTerminalStatus(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()

	tests := []struct {
		name   string
		tasks  []domain.Task
		status string
		ready  bool
	}{
		{
			name:   "no tasks",
			tasks:  nil,
			status: domain.JobStatusCompleted,
			ready:  true,
		},
		{
			name: "pending task",
			tasks: []domain.Task{
				{JobID: jobID, Status: domain.TaskStatusCompleted},
				{JobID: jobID, Status: domain.TaskStatusPending},
			},
			ready: false,
		},
		{
			name: "running task",
			tasks: []domain.Task{
				{JobID: jobID, Status: domain.TaskStatusRunning},
			},
			ready: false,
		},
		{
			name: "all completed",
			tasks: []domain.Task{
				{JobID: jobID, Status: domain.TaskStatusCompleted},
				{JobID: jobID, Status: domain.TaskStatusCompleted},
			},
			status: domain.JobStatusCompleted,
			ready:  true,
		},
		{
			name: "all failed",
			tasks: []domain.Task{
				{JobID: jobID, Status: domain.TaskStatusMaxRetriesExhausted},
				{JobID: jobID, Status: domain.TaskStatusFailed},
			},
			status: domain.JobStatusFailed,
			ready:  true,
		},
		{
			name: "partial failure",
			tasks: []domain.Task{
				{JobID: jobID, Status: domain.TaskStatusCompleted},
				{JobID: jobID, Status: domain.TaskStatusMaxRetriesExhausted},
			},
			status: domain.JobStatusPartialFailed,
			ready:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, ready := resolveJobTerminalStatus(tt.tasks)
			assert.Equal(t, tt.ready, ready)
			if ready {
				assert.Equal(t, tt.status, status)
			}
		})
	}
}

func TestResolveJobTerminalStatusFromSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		summary repository.TaskStatusSummary
		status  string
		ready   bool
	}{
		{
			name:    "no tasks",
			summary: repository.TaskStatusSummary{Total: 0},
			status:  domain.JobStatusCompleted,
			ready:   true,
		},
		{
			name:    "tasks still pending or running",
			summary: repository.TaskStatusSummary{Total: 3, Completed: 1, NonTerminal: 2},
			ready:   false,
		},
		{
			name:    "all completed",
			summary: repository.TaskStatusSummary{Total: 5, Completed: 5, NonTerminal: 0},
			status:  domain.JobStatusCompleted,
			ready:   true,
		},
		{
			name:    "all failed",
			summary: repository.TaskStatusSummary{Total: 4, Completed: 0, NonTerminal: 0},
			status:  domain.JobStatusFailed,
			ready:   true,
		},
		{
			name:    "partial failure",
			summary: repository.TaskStatusSummary{Total: 4, Completed: 2, NonTerminal: 0},
			status:  domain.JobStatusPartialFailed,
			ready:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, ready := resolveJobTerminalStatusFromSummary(tt.summary)
			assert.Equal(t, tt.ready, ready)
			if ready {
				assert.Equal(t, tt.status, status)
			}
		})
	}
}

func TestMaybeFinalizeJob(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()
	refID := "ref-1"
	job := &domain.Job{
		ID: jobID, Name: "test-job", Status: domain.JobStatusRunning,
		UniqueReferenceID: testutil.StringPtr(refID),
	}

	t.Run("finalizes when all tasks completed", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)

		repo.On("FindJobByID", mock.Anything, jobID).Return(job, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, jobID).Return(
			repository.TaskStatusSummary{Total: 1, Completed: 1, NonTerminal: 0}, nil)
		repo.On("UpdateJobIfStatus", mock.Anything, jobID, domain.JobStatusRunning, mock.Anything).Return(true, nil)

		assert.NoError(t, d.maybeFinalizeJob(context.Background(), jobID))
		assert.Equal(t, domain.JobStatusCompleted, job.Status)
	})

	t.Run("skips when tasks still running", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)
		runningJob := &domain.Job{ID: jobID, Name: "test-job", Status: domain.JobStatusRunning}

		repo.On("FindJobByID", mock.Anything, jobID).Return(runningJob, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, jobID).Return(
			repository.TaskStatusSummary{Total: 1, Completed: 0, NonTerminal: 1}, nil)

		assert.NoError(t, d.maybeFinalizeJob(context.Background(), jobID))
		assert.Equal(t, domain.JobStatusRunning, runningJob.Status)
	})

	t.Run("finalizes as failed when all tasks failed", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)
		failedJob := &domain.Job{ID: jobID, Name: "test-job", Status: domain.JobStatusRunning}

		repo.On("FindJobByID", mock.Anything, jobID).Return(failedJob, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, jobID).Return(
			repository.TaskStatusSummary{Total: 2, Completed: 0, NonTerminal: 0}, nil)
		repo.On("UpdateJobIfStatus", mock.Anything, jobID, domain.JobStatusRunning, mock.Anything).Return(true, nil)

		assert.NoError(t, d.maybeFinalizeJob(context.Background(), jobID))
		assert.Equal(t, domain.JobStatusFailed, failedJob.Status)
	})

	t.Run("finalizes as partial failed on mixed outcomes", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)
		partialJob := &domain.Job{ID: jobID, Name: "test-job", Status: domain.JobStatusRunning}

		repo.On("FindJobByID", mock.Anything, jobID).Return(partialJob, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, jobID).Return(
			repository.TaskStatusSummary{Total: 4, Completed: 2, NonTerminal: 0}, nil)
		repo.On("UpdateJobIfStatus", mock.Anything, jobID, domain.JobStatusRunning, mock.Anything).Return(true, nil)

		assert.NoError(t, d.maybeFinalizeJob(context.Background(), jobID))
		assert.Equal(t, domain.JobStatusPartialFailed, partialJob.Status)
	})
}

func TestCheckJobCompletions(t *testing.T) {
	t.Run("finalizes every completable job it finds", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobA := &domain.Job{ID: uuid.New(), Name: "test-job", Status: domain.JobStatusRunning}
		jobB := &domain.Job{ID: uuid.New(), Name: "test-job", Status: domain.JobStatusRunning}

		repo.On("FindCompletableJobIDs", mock.Anything, "test-job", defaultCompletionCheckBatchSize).
			Return([]uuid.UUID{jobA.ID, jobB.ID}, nil)
		repo.On("FindJobByID", mock.Anything, jobA.ID).Return(jobA, nil)
		repo.On("FindJobByID", mock.Anything, jobB.ID).Return(jobB, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, jobA.ID).Return(
			repository.TaskStatusSummary{Total: 1, Completed: 1, NonTerminal: 0}, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, jobB.ID).Return(
			repository.TaskStatusSummary{Total: 1, Completed: 0, NonTerminal: 0}, nil)
		repo.On("UpdateJobIfStatus", mock.Anything, jobA.ID, domain.JobStatusRunning, mock.Anything).Return(true, nil)
		repo.On("UpdateJobIfStatus", mock.Anything, jobB.ID, domain.JobStatusRunning, mock.Anything).Return(true, nil)

		assert.NoError(t, CheckJobCompletions(context.Background(), repo, logging.NopLogger{}, nil, "test-job"))
		assert.Equal(t, domain.JobStatusCompleted, jobA.Status)
		assert.Equal(t, domain.JobStatusFailed, jobB.Status)
	})

	t.Run("nothing completable is a no-op", func(t *testing.T) {
		repo := &testutil.MockRepository{}

		repo.On("FindCompletableJobIDs", mock.Anything, "test-job", defaultCompletionCheckBatchSize).
			Return([]uuid.UUID{}, nil)

		assert.NoError(t, CheckJobCompletions(context.Background(), repo, logging.NopLogger{}, nil, "test-job"))
		repo.AssertNotCalled(t, "FindJobByID", mock.Anything, mock.Anything)
	})

	t.Run("propagates the scan error", func(t *testing.T) {
		repo := &testutil.MockRepository{}

		repo.On("FindCompletableJobIDs", mock.Anything, "test-job", defaultCompletionCheckBatchSize).
			Return(nil, assert.AnError)

		err := CheckJobCompletions(context.Background(), repo, logging.NopLogger{}, nil, "test-job")
		assert.Error(t, err)
	})

	t.Run("logs but does not fail the batch when one job errors", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		badJobID := uuid.New()

		repo.On("FindCompletableJobIDs", mock.Anything, "test-job", defaultCompletionCheckBatchSize).
			Return([]uuid.UUID{badJobID}, nil)
		repo.On("FindJobByID", mock.Anything, badJobID).Return(nil, assert.AnError)

		assert.NoError(t, CheckJobCompletions(context.Background(), repo, logging.NopLogger{}, nil, "test-job"))
	})
}

func TestCompletionCheckTaskType(t *testing.T) {
	assert.Equal(t, "my-job:completion-check", CompletionCheckTaskType("my-job"))
}

func TestIsTaskTerminal(t *testing.T) {
	terminal := []string{domain.TaskStatusCompleted, domain.TaskStatusFailed, domain.TaskStatusMaxRetriesExhausted, domain.TaskStatusRejected, domain.TaskStatusCancelled}
	for _, status := range terminal {
		assert.True(t, isTaskTerminal(status), status)
	}
	assert.False(t, isTaskTerminal(domain.TaskStatusPending))
	assert.False(t, isTaskTerminal(domain.TaskStatusRunning))
}

func TestJobStatusToAction(t *testing.T) {
	assert.Equal(t, domain.JobActionComplete, jobStatusToAction(domain.JobStatusCompleted))
	assert.Equal(t, domain.JobActionFail, jobStatusToAction(domain.JobStatusFailed))
	assert.Equal(t, domain.JobActionPartialFail, jobStatusToAction(domain.JobStatusPartialFailed))
	assert.Equal(t, "", jobStatusToAction(domain.JobStatusPending))
	assert.Equal(t, "", jobStatusToAction(domain.JobStatusRunning))
}

func TestTransitionJob(t *testing.T) {
	t.Run("start persists status and started_at", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)
		job := &domain.Job{ID: uuid.New(), Status: domain.JobStatusPending}

		repo.On("UpdateJob", mock.Anything, job.ID, mock.MatchedBy(func(u map[string]interface{}) bool {
			_, hasStarted := u["started_at"]
			return u[statusField] == domain.JobStatusRunning && hasStarted
		})).Return(nil)

		assert.NoError(t, d.transitionJob(context.Background(), job, domain.JobActionStart))
		assert.Equal(t, domain.JobStatusRunning, job.Status)
	})

	t.Run("complete persists status and finished_at", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)
		job := &domain.Job{ID: uuid.New(), Status: domain.JobStatusRunning}

		repo.On("UpdateJob", mock.Anything, job.ID, mock.MatchedBy(func(u map[string]interface{}) bool {
			_, hasFinished := u["finished_at"]
			return u[statusField] == domain.JobStatusCompleted && hasFinished
		})).Return(nil)

		assert.NoError(t, d.transitionJob(context.Background(), job, domain.JobActionComplete))
		assert.Equal(t, domain.JobStatusCompleted, job.Status)
	})

	t.Run("partial_fail transition", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)
		job := &domain.Job{ID: uuid.New(), Status: domain.JobStatusRunning}

		repo.On("UpdateJob", mock.Anything, job.ID, mock.Anything).Return(nil)

		assert.NoError(t, d.transitionJob(context.Background(), job, domain.JobActionPartialFail))
		assert.Equal(t, domain.JobStatusPartialFailed, job.Status)
	})

	t.Run("cancel transition computes duration when started_at is set", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)
		started := time.Now().Add(-time.Minute)
		job := &domain.Job{ID: uuid.New(), Status: domain.JobStatusPending, StartedAt: &started}

		repo.On("UpdateJob", mock.Anything, job.ID, mock.MatchedBy(func(u map[string]interface{}) bool {
			_, hasDuration := u["duration_ms"]
			return hasDuration
		})).Return(nil)

		assert.NoError(t, d.transitionJob(context.Background(), job, domain.JobActionCancel))
		assert.Equal(t, domain.JobStatusCancelled, job.Status)
		assert.NotNil(t, job.DurationMs)
	})

	t.Run("fail transition surfaces invalid state errors", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)
		job := &domain.Job{ID: uuid.New(), Status: domain.JobStatusCompleted} // Fail is not valid from Completed

		err := d.transitionJob(context.Background(), job, domain.JobActionFail)
		assert.Error(t, err)
		repo.AssertNotCalled(t, "UpdateJob", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("unknown action returns error", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)
		job := &domain.Job{ID: uuid.New(), Status: domain.JobStatusPending}

		err := d.transitionJob(context.Background(), job, "not-a-real-action")
		assert.ErrorContains(t, err, "unknown job action")
	})
}

// TestMaybeFinalizeJobExactlyOnce guards the exactly-once completion guarantee:
// the RUNNING check and the task-summary read in maybeFinalizeJob are separate,
// so two finalizers (an overlapping completion-check tick and the empty-job
// path, say) can both reach the terminal transition for the same job. Only the
// one whose compare-and-set actually moves the row out of RUNNING may fire
// OnJobFinished.
func TestMaybeFinalizeJobExactlyOnce(t *testing.T) {
	jobID := uuid.New()
	allDone := repository.TaskStatusSummary{Total: 1, Completed: 1, NonTerminal: 0}

	setup := func(casWon bool, calls *int) *deps {
		jobs := map[string]jobconfig.JobConfig{"test-job": {Name: "test-job", Hooks: jobconfig.Hooks{
			OnJobFinished: func(context.Context, string, string, string, time.Duration) { *calls++ },
		}}}
		repo := &testutil.MockRepository{}
		job := &domain.Job{ID: jobID, Name: "test-job", Status: domain.JobStatusRunning}
		repo.On("FindJobByID", mock.Anything, jobID).Return(job, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, jobID).Return(allDone, nil)
		repo.On("UpdateJobIfStatus", mock.Anything, jobID, domain.JobStatusRunning, mock.Anything).Return(casWon, nil)
		return newDeps(repo, jobs)
	}

	t.Run("the finalizer that wins the compare-and-set fires the hook once", func(t *testing.T) {
		calls := 0
		assert.NoError(t, setup(true, &calls).maybeFinalizeJob(context.Background(), jobID))
		assert.Equal(t, 1, calls)
	})

	t.Run("a finalizer that loses the compare-and-set does not fire the hook", func(t *testing.T) {
		calls := 0
		assert.NoError(t, setup(false, &calls).maybeFinalizeJob(context.Background(), jobID))
		assert.Equal(t, 0, calls, "OnJobFinished must not fire when another finalizer already transitioned the job")
	})
}
