package executor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
	"github.com/zepto-labs/scherry/internal/testutil"
)

func TestRetryStatuses(t *testing.T) {
	assert.Nil(t, retryStatuses(true))
	assert.Equal(t, []string{domain.TaskStatusMaxRetriesExhausted}, retryStatuses(false))
}

func TestRegisterRetryHandler(t *testing.T) {
	t.Run("invalid payload returns error", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		mux := asynq.NewServeMux()
		RegisterRetryHandler(mux, "retry-job", repo, logging.NopLogger{}, nil)

		task := asynq.NewTask("retry-job", []byte("not-json"))
		err := mux.ProcessTask(context.Background(), task)
		assert.Error(t, err)
	})

	t.Run("invalid job id returns error", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		mux := asynq.NewServeMux()
		RegisterRetryHandler(mux, "retry-job", repo, logging.NopLogger{}, nil)

		payload, err := json.Marshal(RetryJobPayload{JobID: "not-a-uuid"})
		assert.NoError(t, err)
		task := asynq.NewTask("retry-job", payload)
		assert.Error(t, mux.ProcessTask(context.Background(), task))
	})

	t.Run("zero tasks completes cloned job immediately", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobs := map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", Publisher: &testutil.MockPublisher{}, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}, Hooks: jobconfig.NopHooks()},
		}

		originalID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "test-job", Status: domain.JobStatusCompleted}

		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil).Once()
		repo.On("FindPendingJobByParentID", mock.Anything, originalID).Return((*domain.Job)(nil), nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, []domain.Task(nil)).
			Run(func(args mock.Arguments) {
				clonedJob := args.Get(1).(*domain.Job)
				repo.On("FindJobByID", mock.Anything, clonedJob.ID).Return(clonedJob, nil).Once()
			}).
			Return(nil, nil, nil)
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, originalID,
			[]string{domain.TaskStatusMaxRetriesExhausted}, -1, retryBatchSize).Return([]domain.Task{}, nil)
		repo.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		repo.On("UpdateJobIfStatus", mock.Anything, mock.Anything, domain.JobStatusRunning, mock.Anything).Return(true, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, mock.Anything).Return(
			repository.TaskStatusSummary{Total: 0}, nil)

		result, err := RetryJob(context.Background(), repo, logging.NopLogger{}, jobs, originalID, false, "ref")
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, domain.JobStatusCompleted, result.Status)
	})

	t.Run("propagates retry failure", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobs := map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", Publisher: &testutil.MockPublisher{}, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}, Hooks: jobconfig.NopHooks()},
		}
		mux := asynq.NewServeMux()
		RegisterRetryHandler(mux, "retry-job", repo, logging.NopLogger{}, jobs)

		originalID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "test-job", Status: domain.JobStatusFailed}
		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
		repo.On("FindPendingJobByParentID", mock.Anything, originalID).Return((*domain.Job)(nil), nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, []domain.Task(nil)).
			Return(&domain.Job{Name: "test-job", Status: domain.JobStatusPending}, []domain.Task(nil), nil)
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, originalID,
			[]string(nil), -1, retryBatchSize).Return([]domain.Task{}, assert.AnError)

		payload, err := json.Marshal(RetryJobPayload{JobID: originalID.String(), FullRetry: true})
		assert.NoError(t, err)
		task := asynq.NewTask("retry-job", payload)

		err = mux.ProcessTask(context.Background(), task)
		assert.Error(t, err)
	})

	t.Run("successful retry completes without error", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		pub := &testutil.MockPublisher{}
		jobs := map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", Publisher: pub, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}},
		}
		mux := asynq.NewServeMux()
		RegisterRetryHandler(mux, "retry-job", repo, logging.NopLogger{}, jobs)

		originalID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "test-job", Status: domain.JobStatusCompleted}
		pendingClone := &domain.Job{Name: "test-job", Status: domain.JobStatusPending}
		srcTask := domain.Task{ID: uuid.New(), JobID: originalID, Status: domain.TaskStatusMaxRetriesExhausted, SequenceNumber: 0}

		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
		repo.On("FindPendingJobByParentID", mock.Anything, originalID).Return((*domain.Job)(nil), nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, []domain.Task(nil)).Return(pendingClone, []domain.Task(nil), nil)
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, originalID,
			[]string{domain.TaskStatusMaxRetriesExhausted}, -1, retryBatchSize).Return([]domain.Task{srcTask}, nil)
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, originalID,
			[]string{domain.TaskStatusMaxRetriesExhausted}, 0, retryBatchSize).Return([]domain.Task{}, nil)
		repo.On("CreateTasks", mock.Anything, mock.Anything).Return(nil)
		pub.On("PublishTasks", mock.Anything, mock.Anything, "topic").Return(nil)
		repo.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		payload, err := json.Marshal(RetryJobPayload{JobID: originalID.String(), FullRetry: false})
		assert.NoError(t, err)
		task := asynq.NewTask("retry-job", payload)

		assert.NoError(t, mux.ProcessTask(context.Background(), task))
	})
}

func TestRetryJob(t *testing.T) {
	t.Run("rejects non-terminal original job", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobs := map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", Hooks: jobconfig.NopHooks()},
		}
		originalID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "test-job", Status: domain.JobStatusRunning}
		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)

		_, err := RetryJob(context.Background(), repo, logging.NopLogger{}, jobs, originalID, false, "ref")
		assert.ErrorContains(t, err, "not in terminal state")
	})

	t.Run("reuses empty pending clone and completes", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobs := map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", Publisher: &testutil.MockPublisher{}, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}, Hooks: jobconfig.NopHooks()},
		}
		originalID := uuid.New()
		clonedID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "test-job", Status: domain.JobStatusCompleted}
		cloned := &domain.Job{ID: clonedID, Name: "test-job", Status: domain.JobStatusPending}

		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil).Once()
		repo.On("FindPendingJobByParentID", mock.Anything, originalID).Return(cloned, nil)
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, clonedID,
			[]string(nil), -1, retryBatchSize).Return([]domain.Task{}, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, clonedID).Return(repository.TaskStatusSummary{Total: 0}, nil).Twice()
		repo.On("FindJobByID", mock.Anything, clonedID).Return(cloned, nil).Once()
		repo.On("UpdateJob", mock.Anything, clonedID, mock.Anything).Return(nil)
		repo.On("UpdateJobIfStatus", mock.Anything, clonedID, domain.JobStatusRunning, mock.Anything).Return(true, nil)

		result, err := RetryJob(context.Background(), repo, logging.NopLogger{}, jobs, originalID, false, "ref")
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, domain.JobStatusCompleted, result.Status)
	})

	t.Run("returns nil when cloned job already running", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobs := map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", Publisher: &testutil.MockPublisher{}, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}, Hooks: jobconfig.NopHooks()},
		}
		originalID := uuid.New()
		clonedID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "test-job", Status: domain.JobStatusCompleted}
		running := &domain.Job{ID: clonedID, Name: "test-job", Status: domain.JobStatusRunning}

		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
		repo.On("FindPendingJobByParentID", mock.Anything, originalID).Return(running, nil)
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, clonedID,
			[]string(nil), -1, retryBatchSize).Return([]domain.Task{}, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, clonedID).Return(repository.TaskStatusSummary{Total: 0}, nil)

		result, err := RetryJob(context.Background(), repo, logging.NopLogger{}, jobs, originalID, false, "ref")
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("missing job config", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		originalID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "missing", Status: domain.JobStatusCompleted}
		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
		repo.On("FindPendingJobByParentID", mock.Anything, originalID).Return((*domain.Job)(nil), nil)

		_, err := RetryJob(context.Background(), repo, logging.NopLogger{}, map[string]jobconfig.JobConfig{}, originalID, false, "ref")
		assert.ErrorContains(t, err, "no job config")
	})

	t.Run("logs when cloned job cannot be started", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		pub := &testutil.MockPublisher{}
		jobs := map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", Publisher: pub, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}, Hooks: jobconfig.NopHooks()},
		}
		originalID := uuid.New()
		clonedID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "test-job", Status: domain.JobStatusCompleted}
		cloned := &domain.Job{ID: clonedID, Name: "test-job", Status: domain.JobStatusPending}
		srcTask := domain.Task{ID: uuid.New(), JobID: originalID, Status: domain.TaskStatusMaxRetriesExhausted, SequenceNumber: 0}

		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
		repo.On("FindPendingJobByParentID", mock.Anything, originalID).Return((*domain.Job)(nil), nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, []domain.Task(nil)).Return(cloned, []domain.Task(nil), nil)
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, originalID,
			[]string{domain.TaskStatusMaxRetriesExhausted}, -1, retryBatchSize).Return([]domain.Task{srcTask}, nil)
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, originalID,
			[]string{domain.TaskStatusMaxRetriesExhausted}, 0, retryBatchSize).Return([]domain.Task{}, nil)
		repo.On("CreateTasks", mock.Anything, mock.Anything).Return(nil)
		pub.On("PublishTasks", mock.Anything, mock.Anything, "topic").Return(nil)
		repo.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("start failed")).Once()
		repo.On("GetTaskStatusSummary", mock.Anything, mock.Anything).Return(repository.TaskStatusSummary{Total: 1, NonTerminal: 1}, nil)

		result, err := RetryJob(context.Background(), repo, logging.NopLogger{}, jobs, originalID, false, "ref")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}
