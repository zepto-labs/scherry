package scheduler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zepto-labs/scherry/internal/console"
	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
	"github.com/zepto-labs/scherry/internal/testutil"
)

func TestServiceTerminateJob(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		svc := testService(repo, nil)
		jobID := uuid.New()
		repo.On("TerminateJob", mock.Anything, jobID).Return(nil)

		assert.NoError(t, svc.TerminateJob(context.Background(), jobID))
	})

	t.Run("propagates repository error", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		svc := testService(repo, nil)
		jobID := uuid.New()
		repo.On("TerminateJob", mock.Anything, jobID).Return(repository.ErrJobAlreadyTerminal)

		err := svc.TerminateJob(context.Background(), jobID)
		assert.ErrorIs(t, err, repository.ErrJobAlreadyTerminal)
	})
}

func TestServiceGetJobTasks(t *testing.T) {
	t.Run("requires database", func(t *testing.T) {
		svc := testService(&testutil.MockRepository{}, nil)
		_, err := svc.GetJobTasks(context.Background(), console.TaskFilter{JobID: uuid.New()})
		assert.ErrorContains(t, err, "requires a database connection")
	})

	t.Run("delegates to console store", func(t *testing.T) {
		db := fakeGormDB(t)
		svc := &Service{logger: logging.NopLogger{}, repository: repository.NewRepository(db), jobs: map[string]jobconfig.JobConfig{}}
		_, err := svc.GetJobTasks(context.Background(), console.TaskFilter{JobID: uuid.New()})
		// fakeGormDB has no sqlmock expectations; query fails but proves delegation past the nil-db guard.
		assert.Error(t, err)
	})
}

func TestServiceConsoleHandler(t *testing.T) {
	svc := testService(&testutil.MockRepository{}, map[string]jobconfig.JobConfig{
		"job-a": {Name: "job-a"},
	})
	handler := svc.ConsoleHandler()
	require.NotNil(t, handler)

	req := httptest.NewRequest(http.MethodGet, "/api/jobs/meta", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "job-a")
}

func TestServiceCheckJobCompletions(t *testing.T) {
	repo := &testutil.MockRepository{}
	svc := testService(repo, map[string]jobconfig.JobConfig{"job-a": {Name: "job-a"}})
	repo.On("FindCompletableJobIDs", mock.Anything, "job-a", mock.Anything).Return([]uuid.UUID{}, nil)

	assert.NoError(t, svc.CheckJobCompletions(context.Background(), "job-a"))
}

func TestServiceRegisterRetryHandler(t *testing.T) {
	repo := &testutil.MockRepository{}
	jobs := map[string]jobconfig.JobConfig{
		"test-job": {Name: "test-job", Publisher: &testutil.MockPublisher{}, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}},
	}
	svc := testService(repo, jobs)
	mux := asynq.NewServeMux()
	svc.RegisterRetryHandler(mux, "retry-job")

	task := asynq.NewTask("retry-job", []byte("not-json"))
	assert.Error(t, mux.ProcessTask(context.Background(), task))
}

func TestServiceTaskRunner(t *testing.T) {
	repo := &testutil.MockRepository{}
	taskExec := &testutil.MockTaskExecutor{}
	svc := testService(repo, map[string]jobconfig.JobConfig{
		"test-job": {Name: "test-job", TaskExecutor: taskExec},
	})
	task, job := testutil.NewTestTaskPair()
	payload := testutil.MustMarshal(task)

	repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
	repo.On("UpdateTask", mock.Anything, task.ID, mock.Anything).Return(nil)
	taskExec.On("Execute", mock.Anything, mock.Anything).Return(nil, nil)

	assert.NoError(t, svc.taskRunner().ExecuteTask(context.Background(), payload))
}

func TestServiceRetryTriggerJob(t *testing.T) {
	repo := &testutil.MockRepository{}
	jobExec := &testutil.MockJobExecutor{}
	svc := testService(repo, map[string]jobconfig.JobConfig{
		"job-a": {
			Name: "job-a", JobExecutor: jobExec, Publisher: &testutil.MockPublisher{},
			Hooks: jobconfig.NopHooks(), Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"},
			Enabled: func() bool { return true },
		},
	})

	originalID := uuid.New()
	original := &domain.Job{ID: originalID, Name: "job-a", Status: domain.JobStatusFailed}
	require.NoError(t, original.SetMetadata(map[string]interface{}{"trigger_failure": true}))
	retried := &domain.Job{ID: uuid.New(), Name: "job-a", Status: domain.JobStatusPending}
	completed := &domain.Job{ID: retried.ID, Name: "job-a", Status: domain.JobStatusCompleted}

	repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
	repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, mock.Anything).
		Return((*domain.Job)(nil), []domain.Task{}, nil).Once()
	jobExec.On("Execute", mock.Anything, mock.Anything).Return([]jobconfig.TaskData{}, nil)
	repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, mock.Anything).Return(retried, []domain.Task{}, nil)
	repo.On("FindJobByID", mock.Anything, retried.ID).Return(retried, nil)
	repo.On("GetTaskStatusSummary", mock.Anything, retried.ID).Return(repository.TaskStatusSummary{Total: 0}, nil)
	repo.On("UpdateJob", mock.Anything, retried.ID, mock.Anything).Return(nil)
	repo.On("UpdateJobIfStatus", mock.Anything, retried.ID, domain.JobStatusRunning, mock.Anything).Return(true, nil)
	repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, mock.Anything).
		Return(completed, []domain.Task{}, nil).Once()

	result, err := svc.RetryTriggerJob(context.Background(), originalID, "new-ref")
	assert.NoError(t, err)
	assert.Equal(t, completed.ID, result.ID)
}

func TestServiceFindJobByID(t *testing.T) {
	repo := &testutil.MockRepository{}
	svc := testService(repo, nil)
	jobID := uuid.New()
	job := &domain.Job{ID: jobID, Name: "job-a"}
	repo.On("FindJobByID", mock.Anything, jobID).Return(job, nil)

	found, err := svc.FindJobByID(context.Background(), jobID)
	assert.NoError(t, err)
	assert.Equal(t, jobID, found.ID)
}

func TestServiceAsynqRedisOpt(t *testing.T) {
	svc := &Service{cfg: Config{Redis: RedisConfig{Addr: "127.0.0.1:6379"}}}
	opt := svc.AsynqRedisOpt()
	assert.Equal(t, "127.0.0.1:6379", opt.Addr)
}

func TestServiceRetryAsynqTask(t *testing.T) {
	mr := miniredis.RunT(t)
	opt := asynq.RedisClientOpt{Addr: mr.Addr()}
	taskID := archiveAsynqTaskForService(t, opt, "payroll")

	svc := &Service{
		cfg:    Config{Redis: RedisConfig{Addr: mr.Addr()}},
		logger: logging.NopLogger{},
	}
	assert.NoError(t, svc.RetryAsynqTask(context.Background(), "", taskID))

	err := svc.RetryAsynqTask(context.Background(), "default", "missing")
	assert.Error(t, err)
}

// archiveAsynqTaskForService enqueues a task of the given type, lets it fail
// once, and returns its ID after it reaches the archived state.
//
// The worker is stopped before returning so it cannot pick up work that the
// caller enqueues afterwards, such as a task put back on the queue by
// Service.RetryAsynqTask.
func archiveAsynqTaskForService(t *testing.T, opt asynq.RedisClientOpt, taskType string) string {
	t.Helper()
	processed := make(chan struct{}, 1)
	srv := asynq.NewServer(opt, asynq.Config{Concurrency: 1, LogLevel: asynq.FatalLevel})
	mux := asynq.NewServeMux()
	mux.HandleFunc(taskType, func(context.Context, *asynq.Task) error {
		processed <- struct{}{}
		return errors.New("trigger failed")
	})
	go func() { _ = srv.Run(mux) }()
	defer srv.Shutdown()

	client := asynq.NewClient(opt)
	defer func() { _ = client.Close() }()
	info, err := client.Enqueue(asynq.NewTask(taskType, nil), asynq.MaxRetry(0))
	require.NoError(t, err)

	select {
	case <-processed:
	case <-time.After(3 * time.Second):
		t.Fatal("task was not processed")
	}

	inspector := asynq.NewInspector(opt)
	defer func() { _ = inspector.Close() }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ti, err := inspector.GetTaskInfo("default", info.ID)
		if err == nil && ti.State == asynq.TaskStateArchived {
			return info.ID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("task never archived")
	return ""
}
