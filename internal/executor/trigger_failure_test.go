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
	"github.com/zepto-labs/scherry/internal/repository"
	"github.com/zepto-labs/scherry/internal/testutil"
)

func TestShouldPersistTriggerFailure(t *testing.T) {
	t.Run("manual trigger without asynq metadata always persists", func(t *testing.T) {
		assert.True(t, shouldPersistTriggerFailure(nil))
		assert.True(t, shouldPersistTriggerFailure(map[string]interface{}{"asynq_task_id": "x"}))
	})

	t.Run("asynq intermediate retry does not persist", func(t *testing.T) {
		meta := map[string]interface{}{
			"asynq_retry_count": 1,
			"asynq_max_retry":   3,
		}
		assert.False(t, shouldPersistTriggerFailure(meta))
	})

	t.Run("asynq final retry persists", func(t *testing.T) {
		meta := map[string]interface{}{
			"asynq_retry_count": 3,
			"asynq_max_retry":   3,
		}
		assert.True(t, shouldPersistTriggerFailure(meta))
	})
}

func TestIsTriggerFailureJob(t *testing.T) {
	assert.False(t, IsTriggerFailureJob(nil))

	job := &domain.Job{}
	assert.False(t, IsTriggerFailureJob(job))

	assert.NoError(t, job.SetMetadata(map[string]interface{}{metadataTriggerFailure: true}))
	assert.True(t, IsTriggerFailureJob(job))
}

func TestMetadataInt(t *testing.T) {
	meta := map[string]interface{}{
		"i":   2,
		"i64": int64(3),
		"f":   float64(4),
		"x":   "nope",
	}
	v, ok := metadataInt(meta, "i")
	assert.True(t, ok)
	assert.Equal(t, 2, v)
	v, ok = metadataInt(meta, "i64")
	assert.True(t, ok)
	assert.Equal(t, 3, v)
	v, ok = metadataInt(meta, "f")
	assert.True(t, ok)
	assert.Equal(t, 4, v)
	_, ok = metadataInt(meta, "x")
	assert.False(t, ok)
	_, ok = metadataInt(nil, "i")
	assert.False(t, ok)
}

func TestPersistTriggerFailure(t *testing.T) {
	t.Run("skips when job already exists for ref", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", Hooks: jobconfig.NopHooks()},
		})
		job := &domain.Job{ID: uuid.New(), Name: "job-a"}
		existing := &domain.Job{ID: uuid.New(), Name: "job-a"}
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref").Return(existing, []domain.Task{}, nil)

		err := d.persistTriggerFailure(context.Background(), job, d.jobs["job-a"], "ref", nil, "job_executor", errors.New("boom"))
		assert.NoError(t, err)
	})

	t.Run("creates failed trigger job", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", Hooks: jobconfig.NopHooks()},
		})
		job := &domain.Job{ID: uuid.New(), Name: "job-a"}
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref").Return((*domain.Job)(nil), []domain.Task{}, nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.MatchedBy(func(j *domain.Job) bool {
			return j.Status == domain.JobStatusFailed
		}), []domain.Task(nil)).Return(job, []domain.Task{}, nil)

		err := d.persistTriggerFailure(context.Background(), job, d.jobs["job-a"], "ref", map[string]interface{}{"asynq_task_id": "task-1"}, "job_executor", errors.New("boom"))
		assert.NoError(t, err)
		assert.True(t, IsTriggerFailureJob(job))
	})
}

func TestRetryTriggerJob(t *testing.T) {
	t.Run("success re-executes trigger failure job", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobExec := &testutil.MockJobExecutor{}
		pub := &testutil.MockPublisher{}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {
				Name: "job-a", JobExecutor: jobExec, Publisher: pub, Hooks: jobconfig.NopHooks(),
				Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}, Enabled: func() bool { return true },
			},
		}

		originalID := uuid.New()
		original := &domain.Job{
			ID: originalID, Name: "job-a", Status: domain.JobStatusFailed,
			UniqueReferenceID: testutil.StringPtr("orig-ref"),
		}
		require.NoError(t, original.SetMetadata(map[string]interface{}{metadataTriggerFailure: true}))

		retried := &domain.Job{ID: uuid.New(), Name: "job-a", Status: domain.JobStatusPending}
		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, mock.Anything).
			Return((*domain.Job)(nil), []domain.Task{}, nil).Once()
		jobExec.On("Execute", mock.Anything, mock.Anything).Return([]jobconfig.TaskData{}, nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, mock.Anything).Return(retried, []domain.Task{}, nil)
		repo.On("FindJobByID", mock.Anything, retried.ID).Return(retried, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, retried.ID).Return(repository.TaskStatusSummary{Total: 0}, nil)
		repo.On("UpdateJob", mock.Anything, retried.ID, mock.Anything).Return(nil)
		completed := &domain.Job{ID: retried.ID, Name: "job-a", Status: domain.JobStatusCompleted}
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, mock.Anything).
			Return(completed, []domain.Task{}, nil).Once()

		result, err := RetryTriggerJob(context.Background(), repo, logging.NopLogger{}, jobs, originalID, "new-ref")
		assert.NoError(t, err)
		assert.Equal(t, retried.ID, result.ID)
	})

	t.Run("rejects non trigger failure job", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		originalID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "job-a", Status: domain.JobStatusFailed}
		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)

		_, err := RetryTriggerJob(context.Background(), repo, logging.NopLogger{}, map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", Hooks: jobconfig.NopHooks(), Enabled: func() bool { return true }},
		}, originalID, "ref")
		assert.ErrorContains(t, err, "not a trigger failure job")
	})

	t.Run("rejects pending trigger failure job", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		originalID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "job-a", Status: domain.JobStatusPending}
		require.NoError(t, original.SetMetadata(map[string]interface{}{metadataTriggerFailure: true}))
		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)

		_, err := RetryTriggerJob(context.Background(), repo, logging.NopLogger{}, map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", Hooks: jobconfig.NopHooks(), Enabled: func() bool { return true }},
		}, originalID, "ref")
		assert.ErrorContains(t, err, "not in terminal state")
	})

	t.Run("fails when job executor fails", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobExec := &testutil.MockJobExecutor{}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", JobExecutor: jobExec, Hooks: jobconfig.NopHooks(), Enabled: func() bool { return true }},
		}
		originalID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "job-a", Status: domain.JobStatusFailed}
		require.NoError(t, original.SetMetadata(map[string]interface{}{metadataTriggerFailure: true}))

		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, mock.Anything).
			Return((*domain.Job)(nil), []domain.Task{}, nil).Twice()
		jobExec.On("Execute", mock.Anything, mock.Anything).Return([]jobconfig.TaskData{}, errors.New("still failing"))
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, []domain.Task(nil)).
			Return(&domain.Job{Status: domain.JobStatusFailed}, []domain.Task{}, nil)

		_, err := RetryTriggerJob(context.Background(), repo, logging.NopLogger{}, jobs, originalID, "ref")
		assert.Error(t, err)
	})

	t.Run("fails when retried job cannot be found", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobExec := &testutil.MockJobExecutor{}
		pub := &testutil.MockPublisher{}
		jobs := map[string]jobconfig.JobConfig{
			"job-a": {
				Name: "job-a", JobExecutor: jobExec, Publisher: pub, Hooks: jobconfig.NopHooks(),
				Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}, Enabled: func() bool { return true },
			},
		}
		originalID := uuid.New()
		retriedID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "job-a", Status: domain.JobStatusFailed}
		require.NoError(t, original.SetMetadata(map[string]interface{}{metadataTriggerFailure: true}))
		retried := &domain.Job{ID: retriedID, Name: "job-a", Status: domain.JobStatusPending}

		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, mock.Anything).
			Return((*domain.Job)(nil), []domain.Task{}, nil).Once()
		jobExec.On("Execute", mock.Anything, mock.Anything).Return([]jobconfig.TaskData{{Name: "t1"}}, nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, mock.Anything).Return(retried, []domain.Task{}, nil)
		repo.On("FindJobByID", mock.Anything, retriedID).Return(retried, nil)
		repo.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		pub.On("PublishTasks", mock.Anything, mock.Anything, "topic").Return(nil)
		repo.On("GetTaskStatusSummary", mock.Anything, retriedID).Return(repository.TaskStatusSummary{Total: 1, NonTerminal: 1}, nil)
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "new-ref").
			Return((*domain.Job)(nil), []domain.Task{}, nil)

		_, err := RetryTriggerJob(context.Background(), repo, logging.NopLogger{}, jobs, originalID, "new-ref")
		assert.ErrorContains(t, err, "retried job not found")
	})

	t.Run("propagates create trigger failure error", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		d := newDeps(repo, map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", Hooks: jobconfig.NopHooks()},
		})
		job := &domain.Job{ID: uuid.New(), Name: "job-a"}
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref").Return((*domain.Job)(nil), []domain.Task{}, nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, []domain.Task(nil)).
			Return((*domain.Job)(nil), []domain.Task{}, errors.New("db down"))

		err := d.persistTriggerFailure(context.Background(), job, d.jobs["job-a"], "ref", nil, "job_executor", errors.New("boom"))
		assert.ErrorContains(t, err, "create trigger failure job")
	})
}
