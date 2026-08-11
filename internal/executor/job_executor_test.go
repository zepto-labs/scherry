package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/testutil"
)

func TestServiceUpdateTasksStatus(t *testing.T) {
	t.Run("no-op for empty task slice", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)

		d.updateTasksStatus(context.Background(), nil, domain.TaskStatusCancelled)

		repo.AssertNotCalled(t, "UpdateTasksStatus", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("updates every task id to the given status", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)

		tasks := []domain.Task{{ID: uuid.New()}, {ID: uuid.New()}}
		ids := []uuid.UUID{tasks[0].ID, tasks[1].ID}

		repo.On("UpdateTasksStatus", mock.Anything, ids, domain.TaskStatusCancelled).Return(nil)

		d.updateTasksStatus(context.Background(), tasks, domain.TaskStatusCancelled)
		repo.AssertExpectations(t)
	})

	t.Run("logs but does not panic when repository fails", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, nil)

		tasks := []domain.Task{{ID: uuid.New()}}
		repo.On("UpdateTasksStatus", mock.Anything, mock.Anything, domain.TaskStatusRejected).
			Return(errors.New("db down"))

		d.updateTasksStatus(context.Background(), tasks, domain.TaskStatusRejected)
		repo.AssertExpectations(t)
	})
}

func TestSetInitialTaskStatus(t *testing.T) {
	t.Run("pending job yields pending task", func(t *testing.T) {
		task := &domain.Task{}
		setInitialTaskStatus(task, &domain.Job{Status: domain.JobStatusPending})
		assert.Equal(t, domain.TaskStatusPending, task.Status)
	})

	t.Run("rejected job yields rejected task", func(t *testing.T) {
		task := &domain.Task{}
		setInitialTaskStatus(task, &domain.Job{Status: domain.JobStatusRejected})
		assert.Equal(t, domain.TaskStatusRejected, task.Status)
	})

	t.Run("cancelled job yields cancelled task", func(t *testing.T) {
		task := &domain.Task{}
		setInitialTaskStatus(task, &domain.Job{Status: domain.JobStatusCancelled})
		assert.Equal(t, domain.TaskStatusCancelled, task.Status)
	})

	t.Run("other terminal statuses leave task status unset", func(t *testing.T) {
		task := &domain.Task{}
		setInitialTaskStatus(task, &domain.Job{Status: domain.JobStatusFailed})
		assert.Equal(t, "", task.Status)
	})
}

func TestSetInitialJobStatus(t *testing.T) {
	job := &domain.Job{}
	setInitialJobStatus(job, true)
	assert.Equal(t, domain.JobStatusPending, job.Status)

	setInitialJobStatus(job, false)
	assert.Equal(t, domain.JobStatusRejected, job.Status)
}

func TestExecuteJobFlowBranches(t *testing.T) {
	t.Run("non-pending job bulk-updates tasks and returns immediately", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", Publisher: &testutil.MockPublisher{}, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}},
		}
		d := newDeps(repo, jobs)
		job := &domain.Job{ID: uuid.New(), Name: "job-a", Status: domain.JobStatusRejected}

		repo.On("UpdateTasksStatusByJobID", mock.Anything, job.ID, domain.JobStatusRejected).Return(nil)

		err := d.executeJobFlow(context.Background(), job, nil, jobs["job-a"])
		assert.NoError(t, err)
		repo.AssertNotCalled(t, "UpdateJob", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("already running job is a no-op", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", Publisher: &testutil.MockPublisher{}, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}},
		}
		d := newDeps(repo, jobs)
		job := &domain.Job{ID: uuid.New(), Name: "job-a", Status: domain.JobStatusRunning}

		err := d.executeJobFlow(context.Background(), job, []domain.Task{{ID: uuid.New()}}, jobs["job-a"])
		assert.NoError(t, err)
		repo.AssertNotCalled(t, "UpdateJob", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("publish failure transitions job to failed", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		pub := &testutil.MockPublisher{}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", Publisher: pub, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}},
		}
		d := newDeps(repo, jobs)
		job := &domain.Job{ID: uuid.New(), Name: "job-a", Status: domain.JobStatusPending}
		tasks := []domain.Task{{ID: uuid.New()}}

		pub.On("PublishTasks", mock.Anything, tasks, "topic").Return(errors.New("kafka down"))
		repo.On("UpdateJob", mock.Anything, job.ID, mock.Anything).Return(nil)

		err := d.executeJobFlow(context.Background(), job, tasks, jobs["job-a"])
		assert.Error(t, err)
		assert.Equal(t, domain.JobStatusFailed, job.Status)
	})

	t.Run("cancels previous jobs when configured", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		pub := &testutil.MockPublisher{}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {
				Name: "job-a", Publisher: pub, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"},
				CancelPrevious: true,
			},
		}
		d := newDeps(repo, jobs)
		job := &domain.Job{ID: uuid.New(), Name: "job-a", Status: domain.JobStatusPending}
		tasks := []domain.Task{{ID: uuid.New()}}

		pub.On("PublishTasks", mock.Anything, tasks, "topic").Return(nil)
		repo.On("CancelPreviousJobs", mock.Anything, "job-a", job.ID).Return(nil)
		repo.On("UpdateJob", mock.Anything, job.ID, mock.Anything).Return(nil)

		err := d.executeJobFlow(context.Background(), job, tasks, jobs["job-a"])
		assert.NoError(t, err)
		repo.AssertCalled(t, "CancelPreviousJobs", mock.Anything, "job-a", job.ID)
	})
}
