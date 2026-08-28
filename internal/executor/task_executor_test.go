package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/testutil"
)

func TestExecuteTaskRetryAndDLQ(t *testing.T) {
	t.Run("retry on failure", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		taskExec := &testutil.MockTaskExecutor{}
		pub := &testutil.MockPublisher{}
		d := newDeps(repo, map[string]jobconfig.JobConfig{
			"test-job": {
				Name: "test-job", TaskExecutor: taskExec, Publisher: pub,
				Retry: jobconfig.RetryConfig{RetryDelay: time.Minute},
				Kafka: jobconfig.KafkaConfig{TaskRetryTopic: "retry"},
			},
		})

		task, job := testutil.NewTestTaskPair()
		task.Attempt = 1
		payload := testutil.MustMarshal(task)

		repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
		repo.On("UpdateTask", mock.Anything, task.ID, mock.Anything).Return(nil)
		taskExec.On("Execute", mock.Anything, mock.Anything).Return(nil, errors.New("boom"))
		pub.On("PublishRetryTasks", mock.Anything, mock.Anything, "retry", time.Minute).Return(nil)

		assert.NoError(t, d.executeTask(context.Background(), payload))
	})

	t.Run("dlq on max retries", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		taskExec := &testutil.MockTaskExecutor{}
		pub := &testutil.MockPublisher{}
		d := newDeps(repo, map[string]jobconfig.JobConfig{
			"test-job": {
				Name: "test-job", TaskExecutor: taskExec, Publisher: pub,
				Kafka: jobconfig.KafkaConfig{TaskDLQTopic: "dlq"},
			},
		})

		task, job := testutil.NewTestTaskPair()
		task.Attempt = task.MaxRetries
		payload := testutil.MustMarshal(task)

		repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
		repo.On("UpdateTask", mock.Anything, task.ID, mock.Anything).Return(nil)
		taskExec.On("Execute", mock.Anything, mock.Anything).Return(nil, errors.New("boom"))
		pub.On("PublishTasks", mock.Anything, mock.Anything, "dlq").Return(nil)

		assert.NoError(t, d.executeTask(context.Background(), payload))
		// The task itself reaches a terminal status (verified via UpdateTask
		// below), but the job is no longer re-checked reactively after every
		// task completion — it stays RUNNING until JobCompletionCron (or a
		// manual CheckJobCompletions call) picks it up. See
		// TestCheckJobCompletions for that half of the flow.
		assert.Equal(t, domain.JobStatusRunning, job.Status)
		repo.AssertCalled(t, "UpdateTask", mock.Anything, task.ID, mock.MatchedBy(func(u map[string]interface{}) bool {
			return u[statusField] == domain.TaskStatusMaxRetriesExhausted
		}))
	})
}

func TestExecuteTaskIdempotentOnRedelivery(t *testing.T) {
	// Kafka delivers at least once, so a task message can be redelivered after the
	// task has already settled. Re-running it would attempt an invalid transition
	// ("start" on a COMPLETED task) and fail the message; the executor must instead
	// treat an already-terminal task as an idempotent no-op.
	for _, status := range []string{
		domain.TaskStatusCompleted,
		domain.TaskStatusCancelled,
		domain.TaskStatusRejected,
		domain.TaskStatusMaxRetriesExhausted,
	} {
		t.Run("skips redelivered "+status+" task", func(t *testing.T) {
			repo := &testutil.MockRepository{}
			taskExec := &testutil.MockTaskExecutor{}
			d := newDeps(repo, map[string]jobconfig.JobConfig{
				"test-job": {
					Name: "test-job", TaskExecutor: taskExec, Hooks: jobconfig.NopHooks(),
				},
			})

			task, job := testutil.NewTestTaskPair()
			task.Status = status                 // this task already finished
			job.Status = domain.JobStatusRunning // but the job is still executing
			payload := testutil.MustMarshal(task)

			repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)

			assert.NoError(t, d.executeTask(context.Background(), payload))
			// No re-execution and no state writes for an already-terminal task.
			taskExec.AssertNotCalled(t, "Execute", mock.Anything, mock.Anything)
			repo.AssertNotCalled(t, "UpdateTask", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

func TestExecuteTaskIdempotentWhenJobAlsoFinalized(t *testing.T) {
	// Kafka delivers at least once, so a task message can be redelivered after
	// BOTH the task and its job have settled — e.g. the job was finalized by the
	// JobCompletionCron between the task completing and the duplicate arriving.
	// An already-terminal task must stay an idempotent no-op regardless of the
	// job's state: it must never be re-executed and its status must never be
	// overwritten with the job's status (which, for PARTIAL_FAILED, is not even a
	// valid task status).
	terminalJobStatuses := []string{
		domain.JobStatusCompleted,
		domain.JobStatusPartialFailed,
		domain.JobStatusFailed,
		domain.JobStatusCancelled,
		domain.JobStatusRejected,
	}
	for _, jobStatus := range terminalJobStatuses {
		t.Run("skips redelivered COMPLETED task under "+jobStatus+" job", func(t *testing.T) {
			repo := &testutil.MockRepository{}
			taskExec := &testutil.MockTaskExecutor{}
			d := newDeps(repo, map[string]jobconfig.JobConfig{
				"test-job": {
					Name: "test-job", TaskExecutor: taskExec, Hooks: jobconfig.NopHooks(),
				},
			})

			task, job := testutil.NewTestTaskPair()
			task.Status = domain.TaskStatusCompleted // this task already finished
			job.Status = jobStatus                   // and the job has since finalized
			payload := testutil.MustMarshal(task)

			repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
			// Allow (but do not require) the buggy write so the failure is a clean
			// AssertNotCalled rather than an unexpected-call panic.
			repo.On("UpdateTasksStatus", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			assert.NoError(t, d.executeTask(context.Background(), payload))

			// A settled task must not be re-executed nor have its status rewritten.
			taskExec.AssertNotCalled(t, "Execute", mock.Anything, mock.Anything)
			repo.AssertNotCalled(t, "UpdateTasksStatus", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

func TestIsNoTransition(t *testing.T) {
	t.Run("true for a same-state self transition", func(t *testing.T) {
		task := &domain.Task{Status: domain.TaskStatusRunning}
		// Start's Src includes Running with Dst Running too, so re-starting an
		// already-running task is a no-op transition, not a real error.
		err := task.Start(context.Background())
		assert.True(t, isNoTransition(err))
	})

	t.Run("false for an invalid transition error", func(t *testing.T) {
		task := &domain.Task{Status: domain.TaskStatusCompleted}
		err := task.Start(context.Background())
		assert.Error(t, err)
		assert.False(t, isNoTransition(err))
	})

	t.Run("false for a plain error", func(t *testing.T) {
		assert.False(t, isNoTransition(errors.New("boom")))
	})

	t.Run("false for nil", func(t *testing.T) {
		assert.False(t, isNoTransition(nil))
	})
}

func TestFailTaskWithResult(t *testing.T) {
	repo := &testutil.MockRepository{}
	d := newDeps(repo, nil)
	task, _ := testutil.NewTestTaskPair()

	repo.On("UpdateTask", mock.Anything, task.ID, mock.MatchedBy(func(updates map[string]interface{}) bool {
		return updates[statusField] == domain.TaskStatusFailed
	})).Return(nil)

	d.failTaskWithResult(context.Background(), task, errors.New("boom"))

	assert.Equal(t, domain.TaskStatusFailed, task.Status)
	result, err := task.GetParams() // sanity: Params untouched, Result was set separately
	assert.NoError(t, err)
	assert.NotNil(t, result)
	repo.AssertExpectations(t)
}

func TestTaskFinishUpdates(t *testing.T) {
	task, _ := testutil.NewTestTaskPair()
	now := time.Now()
	task.StartedAt = &now
	time.Sleep(time.Millisecond)

	updates := taskFinishUpdates(task, map[string]interface{}{})

	assert.NotNil(t, task.FinishedAt)
	assert.NotNil(t, task.DurationMs)
	assert.Contains(t, updates, "finished_at")
	assert.Contains(t, updates, "duration_ms")
}

func TestTaskFinishUpdatesWithoutStartedAt(t *testing.T) {
	task, _ := testutil.NewTestTaskPair()
	task.StartedAt = nil

	updates := taskFinishUpdates(task, map[string]interface{}{})

	assert.NotNil(t, task.FinishedAt)
	assert.Nil(t, task.DurationMs)
	assert.NotContains(t, updates, "duration_ms")
}

func TestExecuteTaskEdgePaths(t *testing.T) {
	t.Run("retry without retry topic configured", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		taskExec := &testutil.MockTaskExecutor{}
		d := newDeps(repo, map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", TaskExecutor: taskExec, Publisher: &testutil.MockPublisher{}},
		})

		task, job := testutil.NewTestTaskPair()
		task.Attempt = 1
		payload := testutil.MustMarshal(task)

		repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
		repo.On("UpdateTask", mock.Anything, task.ID, mock.Anything).Return(nil)
		taskExec.On("Execute", mock.Anything, mock.Anything).Return(nil, errors.New("boom"))

		assert.NoError(t, d.executeTask(context.Background(), payload))
	})

	t.Run("dlq without topic configured", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		taskExec := &testutil.MockTaskExecutor{}
		d := newDeps(repo, map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", TaskExecutor: taskExec, Publisher: &testutil.MockPublisher{}},
		})

		task, job := testutil.NewTestTaskPair()
		task.Attempt = task.MaxRetries
		payload := testutil.MustMarshal(task)

		repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
		repo.On("UpdateTask", mock.Anything, task.ID, mock.Anything).Return(nil)
		taskExec.On("Execute", mock.Anything, mock.Anything).Return(nil, errors.New("boom"))

		assert.NoError(t, d.executeTask(context.Background(), payload))
	})

	t.Run("successful task completion", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		taskExec := &testutil.MockTaskExecutor{}
		d := newDeps(repo, map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", TaskExecutor: taskExec},
		})

		task, job := testutil.NewTestTaskPair()
		payload := testutil.MustMarshal(task)

		repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
		repo.On("UpdateTask", mock.Anything, task.ID, mock.Anything).Return(nil)
		taskExec.On("Execute", mock.Anything, mock.Anything).Return(map[string]interface{}{"ok": true}, nil)

		assert.NoError(t, d.executeTask(context.Background(), payload))
	})
}
