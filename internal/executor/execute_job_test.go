package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/testutil"
)

func TestExecuteJob(t *testing.T) {
	t.Run("success creates job and publishes tasks", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobExec := &testutil.MockJobExecutor{}
		pub := &testutil.MockPublisher{}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {
				Name: "job-a", JobExecutor: jobExec, Publisher: pub, Hooks: jobconfig.NopHooks(),
				Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}, Enabled: func() bool { return true },
			},
		}

		taskData := []jobconfig.TaskData{
			{Name: "t1", Params: map[string]interface{}{"k": "v"}, DistributionKey: "shard-1"},
		}
		job := &domain.Job{ID: uuid.New(), Name: "job-a", Status: domain.JobStatusPending}
		tasks := []domain.Task{{ID: uuid.New(), Name: "t1"}}

		jobExec.On("Execute", mock.Anything, mock.Anything).Return(taskData, nil)
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref").Return((*domain.Job)(nil), []domain.Task{}, nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, mock.Anything).Return(job, tasks, nil)
		repo.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		pub.On("PublishTasks", mock.Anything, tasks, "topic").Return(nil)

		err := ExecuteJob(context.Background(), repo, logging.NopLogger{}, jobs, "job-a", "ref", map[string]interface{}{"m": 1})
		assert.NoError(t, err)
	})

	t.Run("returns existing job without re-executing", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobExec := &testutil.MockJobExecutor{}
		existing := &domain.Job{ID: uuid.New(), Name: "job-a", Status: domain.JobStatusRunning}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", JobExecutor: jobExec, Publisher: &testutil.MockPublisher{}, Hooks: jobconfig.NopHooks(), Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}},
		}

		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref").Return(existing, []domain.Task{{ID: uuid.New()}}, nil)
		repo.On("UpdateTasksStatusByJobID", mock.Anything, existing.ID, domain.JobStatusRunning).Return(nil)

		err := ExecuteJob(context.Background(), repo, logging.NopLogger{}, jobs, "job-a", "ref", nil)
		assert.NoError(t, err)
		jobExec.AssertNotCalled(t, "Execute", mock.Anything, mock.Anything)
	})

	t.Run("unknown job name", func(t *testing.T) {
		err := ExecuteJob(context.Background(), &testutil.MockRepository{}, logging.NopLogger{}, map[string]jobconfig.JobConfig{}, "missing", "ref", nil)
		assert.ErrorContains(t, err, "no job config registered")
	})

	t.Run("job executor failure", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobExec := &testutil.MockJobExecutor{}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", JobExecutor: jobExec, Hooks: jobconfig.NopHooks(), Enabled: func() bool { return true }},
		}

		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref").Return((*domain.Job)(nil), []domain.Task{}, nil).Twice()
		jobExec.On("Execute", mock.Anything, mock.Anything).Return([]jobconfig.TaskData{}, errors.New("boom"))
		repo.On("CreateJobAndTasks", mock.Anything, mock.MatchedBy(func(j *domain.Job) bool {
			return j.Status == domain.JobStatusFailed
		}), []domain.Task(nil)).Return(&domain.Job{Status: domain.JobStatusFailed}, []domain.Task{}, nil)

		err := ExecuteJob(context.Background(), repo, logging.NopLogger{}, jobs, "job-a", "ref", nil)
		assert.Error(t, err)
	})

	t.Run("disabled job is rejected", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobExec := &testutil.MockJobExecutor{}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", JobExecutor: jobExec, Hooks: jobconfig.NopHooks(), Enabled: func() bool { return false }},
		}

		jobExec.On("Execute", mock.Anything, mock.Anything).Return([]jobconfig.TaskData{{Name: "t1"}}, nil)
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref").Return((*domain.Job)(nil), []domain.Task{}, nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.MatchedBy(func(j *domain.Job) bool {
			return j.Status == domain.JobStatusRejected
		}), mock.Anything).Return(&domain.Job{Status: domain.JobStatusRejected}, []domain.Task{{}}, nil)
		repo.On("UpdateTasksStatusByJobID", mock.Anything, mock.Anything, domain.JobStatusRejected).Return(nil)

		err := ExecuteJob(context.Background(), repo, logging.NopLogger{}, jobs, "job-a", "ref", nil)
		assert.NoError(t, err)
	})

	t.Run("build tasks failure persists trigger failure", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobExec := &testutil.MockJobExecutor{}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", JobExecutor: jobExec, Hooks: jobconfig.NopHooks(), Enabled: func() bool { return true }},
		}

		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref").Return((*domain.Job)(nil), []domain.Task{}, nil).Twice()
		jobExec.On("Execute", mock.Anything, mock.Anything).Return([]jobconfig.TaskData{{
			Name:   "t1",
			Params: map[string]interface{}{"bad": make(chan int)},
		}}, nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.MatchedBy(func(j *domain.Job) bool {
			return j.Status == domain.JobStatusFailed
		}), []domain.Task(nil)).Return(&domain.Job{Status: domain.JobStatusFailed}, []domain.Task{}, nil)

		err := ExecuteJob(context.Background(), repo, logging.NopLogger{}, jobs, "job-a", "ref", nil)
		assert.Error(t, err)
	})
}

func TestPrepareJobAndTasks(t *testing.T) {
	t.Run("create job and tasks error", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobExec := &testutil.MockJobExecutor{}
		d := newDeps(repo, map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", JobExecutor: jobExec, Enabled: func() bool { return true }},
		})

		jobExec.On("Execute", mock.Anything, mock.Anything).Return([]jobconfig.TaskData{{Name: "t1"}}, nil)
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref").Return((*domain.Job)(nil), []domain.Task{}, nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.New("db down"))

		_, _, err := d.prepareJobAndTasks(context.Background(), d.jobs["job-a"], nil, "ref")
		assert.ErrorContains(t, err, "create job and tasks")
	})
}

func TestBuildTasks(t *testing.T) {
	d := newDeps(&testutil.MockRepository{}, nil)
	job := &domain.Job{ID: uuid.New(), MaxRetries: 2, Status: domain.JobStatusPending}

	tasks, err := d.buildTasks(job, []jobconfig.TaskData{{Name: "t1", DistributionKey: "dk"}})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NotNil(t, tasks[0].DistributionKey)
	assert.Equal(t, "dk", *tasks[0].DistributionKey)
}

func TestExecuteTaskPublicEntrypoint(t *testing.T) {
	repo := &testutil.MockRepository{}
	taskExec := &testutil.MockTaskExecutor{}
	jobs := map[string]jobconfig.JobConfig{
		"test-job": {Name: "test-job", TaskExecutor: taskExec, Hooks: jobconfig.NopHooks()},
	}
	task, job := testutil.NewTestTaskPair()
	payload := testutil.MustMarshal(task)

	repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
	repo.On("UpdateTask", mock.Anything, task.ID, mock.Anything).Return(nil)
	taskExec.On("Execute", mock.Anything, mock.Anything).Return(map[string]interface{}{"ok": true}, nil)

	assert.NoError(t, ExecuteTask(context.Background(), repo, logging.NopLogger{}, jobs, payload))
}

func TestExecuteTaskSkipsNonPendingJob(t *testing.T) {
	repo := &testutil.MockRepository{}
	jobs := map[string]jobconfig.JobConfig{
		"test-job": {Name: "test-job", TaskExecutor: &testutil.MockTaskExecutor{}, Hooks: jobconfig.NopHooks()},
	}
	task, job := testutil.NewTestTaskPair()
	job.Status = domain.JobStatusCompleted
	payload := testutil.MustMarshal(task)

	repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
	repo.On("UpdateTasksStatus", mock.Anything, []uuid.UUID{task.ID}, domain.JobStatusCompleted).Return(nil)

	assert.NoError(t, ExecuteTask(context.Background(), repo, logging.NopLogger{}, jobs, payload))
}
