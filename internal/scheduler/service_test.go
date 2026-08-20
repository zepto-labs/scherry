package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" sql driver

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
	"github.com/zepto-labs/scherry/internal/testutil"
	"github.com/zepto-labs/scherry/internal/transport"
)

// testService builds a *Service around repo/jobs for tests, merging each
// job's Hooks the same way Register/RegisterManual would.
func testService(repo repository.Repository, jobs map[string]jobconfig.JobConfig) *Service {
	for name, cfg := range jobs {
		cfg.Hooks = jobconfig.MergeHooks(cfg.Hooks)
		jobs[name] = cfg
	}
	return &Service{
		logger:     logging.NopLogger{},
		repository: repo,
		jobs:       jobs,
	}
}

type noopReader struct{}

func (noopReader) FetchMessage(context.Context) (kafkago.Message, error) {
	return kafkago.Message{}, errors.New("not implemented")
}
func (noopReader) CommitMessages(context.Context, ...kafkago.Message) error { return nil }
func (noopReader) Close() error                                             { return nil }

func TestRegisterJob(t *testing.T) {
	t.Parallel()

	redisOpt := asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}
	rootCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc := &Service{
		logger:    logging.NopLogger{},
		scheduler: asynq.NewScheduler(redisOpt, nil),
		jobs:      make(map[string]jobconfig.JobConfig),
		registry:  transport.NewConsumerRegistry(logging.NopLogger{}),
		rootCtx:   rootCtx,
	}

	cfg := jobconfig.JobConfig{
		Name:              "test_job",
		JobExecutor:       &testutil.MockJobExecutor{},
		TaskExecutor:      &testutil.MockTaskExecutor{},
		Schedule:          "0 * * * *",
		JobCompletionCron: "*/5 * * * *",
		Kafka: jobconfig.KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			TaskTopic:     "test-topic",
			ConsumerGroup: "test-group",
		},
		Publisher:  &testutil.MockPublisher{},
		TaskReader: &noopReader{},
	}

	assert.NoError(t, svc.Register(cfg))
	assert.Contains(t, svc.jobs, "test_job")
}

func TestRegisterJobWithCompletionCron(t *testing.T) {
	t.Parallel()

	redisOpt := asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}
	rootCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc := &Service{
		logger:    logging.NopLogger{},
		scheduler: asynq.NewScheduler(redisOpt, nil),
		jobs:      make(map[string]jobconfig.JobConfig),
		registry:  transport.NewConsumerRegistry(logging.NopLogger{}),
		rootCtx:   rootCtx,
	}

	cfg := jobconfig.JobConfig{
		Name:              "cron_checked_job",
		JobExecutor:       &testutil.MockJobExecutor{},
		TaskExecutor:      &testutil.MockTaskExecutor{},
		Schedule:          "0 * * * *",
		JobCompletionCron: "*/5 * * * *",
		Kafka: jobconfig.KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			TaskTopic:     "test-topic",
			ConsumerGroup: "test-group",
		},
		Publisher:  &testutil.MockPublisher{},
		TaskReader: &noopReader{},
	}

	assert.NoError(t, svc.Register(cfg))
	assert.Contains(t, svc.jobs, "cron_checked_job")
	assert.Equal(t, "*/5 * * * *", svc.jobs["cron_checked_job"].JobCompletionCron)
}

func TestRegisterManualJobWithCompletionCron(t *testing.T) {
	t.Parallel()

	redisOpt := asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}
	rootCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc := &Service{
		logger:    logging.NopLogger{},
		scheduler: asynq.NewScheduler(redisOpt, nil),
		jobs:      make(map[string]jobconfig.JobConfig),
		registry:  transport.NewConsumerRegistry(logging.NopLogger{}),
		rootCtx:   rootCtx,
	}

	cfg := jobconfig.JobConfig{
		Name:              "manual_cron_checked_job",
		JobExecutor:       &testutil.MockJobExecutor{},
		TaskExecutor:      &testutil.MockTaskExecutor{},
		JobCompletionCron: "*/5 * * * *",
		Kafka: jobconfig.KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			TaskTopic:     "manual-topic",
			ConsumerGroup: "manual-group",
		},
		Publisher:  &testutil.MockPublisher{},
		TaskReader: &noopReader{},
	}

	// A manual job (no Schedule) can still register a JobCompletionCron,
	// since the two are independent triggers.
	assert.NoError(t, svc.RegisterManual(cfg))
	assert.Contains(t, svc.jobs, "manual_cron_checked_job")
}

func TestRegisterJobInvalidCompletionCron(t *testing.T) {
	t.Parallel()

	redisOpt := asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}
	rootCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc := &Service{
		logger:    logging.NopLogger{},
		scheduler: asynq.NewScheduler(redisOpt, nil),
		jobs:      make(map[string]jobconfig.JobConfig),
		registry:  transport.NewConsumerRegistry(logging.NopLogger{}),
		rootCtx:   rootCtx,
	}

	cfg := jobconfig.JobConfig{
		Name:              "bad_cron_job",
		JobExecutor:       &testutil.MockJobExecutor{},
		TaskExecutor:      &testutil.MockTaskExecutor{},
		JobCompletionCron: "not a cron expression",
		Kafka: jobconfig.KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			TaskTopic:     "bad-topic",
			ConsumerGroup: "bad-group",
		},
		Publisher:  &testutil.MockPublisher{},
		TaskReader: &noopReader{},
	}

	err := svc.RegisterManual(cfg)
	assert.ErrorContains(t, err, "register completion cron")
	assert.NotContains(t, svc.jobs, "bad_cron_job")
}

func TestRegisterJobValidation(t *testing.T) {
	t.Parallel()
	svc := &Service{logger: logging.NopLogger{}, jobs: make(map[string]jobconfig.JobConfig), registry: transport.NewConsumerRegistry(logging.NopLogger{})}

	err := svc.Register(jobconfig.JobConfig{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "job name cannot be empty")
}

func TestRegisterManualJob(t *testing.T) {
	t.Parallel()

	redisOpt := asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}
	rootCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc := &Service{
		logger:    logging.NopLogger{},
		scheduler: asynq.NewScheduler(redisOpt, nil),
		jobs:      make(map[string]jobconfig.JobConfig),
		registry:  transport.NewConsumerRegistry(logging.NopLogger{}),
		rootCtx:   rootCtx,
	}

	cfg := jobconfig.JobConfig{
		Name:              "manual_job",
		JobExecutor:       &testutil.MockJobExecutor{},
		TaskExecutor:      &testutil.MockTaskExecutor{},
		JobCompletionCron: "*/5 * * * *",
		Kafka: jobconfig.KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			TaskTopic:     "manual-topic",
			ConsumerGroup: "manual-group",
		},
		Publisher:  &testutil.MockPublisher{},
		TaskReader: &noopReader{},
	}

	err := svc.RegisterManual(cfg)
	assert.NoError(t, err)
	assert.Contains(t, svc.jobs, "manual_job")
	assert.Equal(t, "", svc.jobs["manual_job"].Schedule)
}

func TestRegisterManualJobValidation(t *testing.T) {
	t.Parallel()
	svc := &Service{logger: logging.NopLogger{}, jobs: make(map[string]jobconfig.JobConfig), registry: transport.NewConsumerRegistry(logging.NopLogger{})}

	err := svc.RegisterManual(jobconfig.JobConfig{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "job name cannot be empty")
}

func TestRegisterManualJobDuplicate(t *testing.T) {
	t.Parallel()

	redisOpt := asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}
	rootCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc := &Service{
		logger:    logging.NopLogger{},
		scheduler: asynq.NewScheduler(redisOpt, nil),
		jobs:      make(map[string]jobconfig.JobConfig),
		registry:  transport.NewConsumerRegistry(logging.NopLogger{}),
		rootCtx:   rootCtx,
	}

	cfg := jobconfig.JobConfig{
		Name:              "dup_job",
		JobExecutor:       &testutil.MockJobExecutor{},
		TaskExecutor:      &testutil.MockTaskExecutor{},
		JobCompletionCron: "*/5 * * * *",
		Kafka: jobconfig.KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			TaskTopic:     "dup-topic",
			ConsumerGroup: "dup-group",
		},
		Publisher:  &testutil.MockPublisher{},
		TaskReader: &noopReader{},
	}

	err := svc.RegisterManual(cfg)
	assert.NoError(t, err)

	err = svc.RegisterManual(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")
}

func TestTrigger(t *testing.T) {
	t.Parallel()

	repo := &testutil.MockRepository{}
	jobExec := &testutil.MockJobExecutor{}
	pub := &testutil.MockPublisher{}

	redisOpt := asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}
	rootCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	svc := &Service{
		logger:     logging.NopLogger{},
		repository: repo,
		scheduler:  asynq.NewScheduler(redisOpt, nil),
		jobs:       make(map[string]jobconfig.JobConfig),
		registry:   transport.NewConsumerRegistry(logging.NopLogger{}),
		rootCtx:    rootCtx,
	}

	cfg := jobconfig.JobConfig{
		Name:              "triggered_job",
		JobExecutor:       jobExec,
		TaskExecutor:      &testutil.MockTaskExecutor{},
		JobCompletionCron: "*/5 * * * *",
		Kafka:             jobconfig.KafkaConfig{Brokers: []string{"localhost:9092"}, TaskTopic: "t-topic", ConsumerGroup: "t-group"},
		Publisher:         pub,
		TaskReader:        &noopReader{},
		Enabled:           func() bool { return true },
	}

	assert.NoError(t, svc.RegisterManual(cfg))

	taskData := []jobconfig.TaskData{{Name: "task1", Params: map[string]interface{}{"k": "v"}}}
	job := &domain.Job{ID: uuid.New(), Name: "triggered_job", Status: domain.JobStatusPending}
	tasks := []domain.Task{{ID: uuid.New(), Name: "task1"}}

	jobExec.On("Execute", mock.Anything, mock.Anything).Return(taskData, nil)
	repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref-1").Return((*domain.Job)(nil), []domain.Task{}, nil)
	repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, mock.Anything).Return(job, tasks, nil)
	repo.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	pub.On("PublishTasks", mock.Anything, tasks, "t-topic").Return(nil)

	err := svc.Trigger(context.Background(), "triggered_job", "ref-1", nil)
	assert.NoError(t, err)
}

func TestServiceTrigger(t *testing.T) {
	t.Parallel()

	repo := &testutil.MockRepository{}
	jobExec := &testutil.MockJobExecutor{}
	pub := &testutil.MockPublisher{}
	svc := testService(repo, map[string]jobconfig.JobConfig{
		"manual-job": {
			Name: "manual-job", JobExecutor: jobExec, Publisher: pub,
			Kafka:   jobconfig.KafkaConfig{TaskTopic: "m-topic"},
			Enabled: func() bool { return true },
		},
	})

	taskData := []jobconfig.TaskData{{Name: "t1", Params: map[string]interface{}{}}}
	job := &domain.Job{ID: uuid.New(), Name: "manual-job", Status: domain.JobStatusPending}
	tasks := []domain.Task{{ID: uuid.New(), Name: "t1"}}

	jobExec.On("Execute", mock.Anything, mock.Anything).Return(taskData, nil)
	repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref-2").Return((*domain.Job)(nil), []domain.Task{}, nil)
	repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, mock.Anything).Return(job, tasks, nil)
	repo.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	pub.On("PublishTasks", mock.Anything, tasks, "m-topic").Return(nil)

	err := svc.Trigger(context.Background(), "manual-job", "ref-2", nil)
	assert.NoError(t, err)
}

func TestExecuteJob(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobExec := &testutil.MockJobExecutor{}
		pub := &testutil.MockPublisher{}
		svc := testService(repo, map[string]jobconfig.JobConfig{
			"test-job": {
				Name: "test-job", JobExecutor: jobExec, Publisher: pub,
				Kafka: jobconfig.KafkaConfig{TaskTopic: "test-topic"}, Enabled: func() bool { return true },
			},
		})

		taskData := []jobconfig.TaskData{{Name: "task1", Params: map[string]interface{}{"k": "v"}}}
		job := &domain.Job{ID: uuid.New(), Name: "test-job", Status: domain.JobStatusPending}
		tasks := []domain.Task{{ID: uuid.New(), Name: "task1"}}

		jobExec.On("Execute", mock.Anything, mock.Anything).Return(taskData, nil)
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref").Return((*domain.Job)(nil), []domain.Task{}, nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, mock.Anything).Return(job, tasks, nil)
		repo.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		pub.On("PublishTasks", mock.Anything, tasks, "test-topic").Return(nil)

		assert.NoError(t, svc.ExecuteJob(context.Background(), "test-job", "ref", nil))
	})

	t.Run("zero tasks completes immediately", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		jobExec := &testutil.MockJobExecutor{}
		svc := testService(repo, map[string]jobconfig.JobConfig{
			"test-job": {
				Name: "test-job", JobExecutor: jobExec, Publisher: &testutil.MockPublisher{},
				Kafka: jobconfig.KafkaConfig{TaskTopic: "test-topic"}, Enabled: func() bool { return true },
			},
		})

		job := &domain.Job{ID: uuid.New(), Name: "test-job", Status: domain.JobStatusPending, UniqueReferenceID: testutil.StringPtr("ref")}
		jobExec.On("Execute", mock.Anything, mock.Anything).Return([]jobconfig.TaskData{}, nil)
		repo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, "ref").Return((*domain.Job)(nil), []domain.Task{}, nil)
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, mock.Anything).Return(job, []domain.Task{}, nil)
		repo.On("FindJobByID", mock.Anything, job.ID).Return(job, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, job.ID).Return(
			repository.TaskStatusSummary{Total: 0, Completed: 0, NonTerminal: 0}, nil)
		repo.On("UpdateJob", mock.Anything, job.ID, mock.Anything).Return(nil)
		repo.On("UpdateJobIfStatus", mock.Anything, job.ID, domain.JobStatusRunning, mock.Anything).Return(true, nil)

		assert.NoError(t, svc.ExecuteJob(context.Background(), "test-job", "ref", nil))
		assert.Equal(t, domain.JobStatusCompleted, job.Status)
	})

	t.Run("missing job config", func(t *testing.T) {
		svc := testService(&testutil.MockRepository{}, map[string]jobconfig.JobConfig{})
		err := svc.ExecuteJob(context.Background(), "missing", "ref", nil)
		assert.Error(t, err)
	})
}

func TestExecuteTask(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
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

		assert.NoError(t, svc.ExecuteTask(context.Background(), payload))
	})

	t.Run("invalid payload", func(t *testing.T) {
		svc := testService(&testutil.MockRepository{}, nil)
		err := svc.ExecuteTask(context.Background(), []byte("{bad"))
		assert.Error(t, err)
	})
}

func TestRetryJob(t *testing.T) {
	t.Run("new clone streams tasks in batches", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		pub := &testutil.MockPublisher{}
		svc := testService(repo, map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", Publisher: pub, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}},
		})

		originalID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "test-job", Status: domain.JobStatusCompleted}
		// The service generates its own clonedJob.ID internally; we use a shared
		// object so startClonedJob sees a mutable PENDING→RUNNING job.
		pendingClone := &domain.Job{Name: "test-job", Status: domain.JobStatusPending}
		srcTask := domain.Task{ID: uuid.New(), JobID: originalID, Status: domain.TaskStatusMaxRetriesExhausted, SequenceNumber: 0}

		// Original job lookup
		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
		repo.On("FindPendingJobByParentID", mock.Anything, originalID).Return((*domain.Job)(nil), nil)
		// Pre-create job with no tasks; Return nil tasks — service uses its own clonedJob ptr
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, []domain.Task(nil)).Return(pendingClone, []domain.Task(nil), nil)
		// First batch: 1 source task (partial-retry, afterSeq=-1)
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, originalID,
			[]string{domain.TaskStatusMaxRetriesExhausted}, -1, repository.TaskInsertBatchSize).Return([]domain.Task{srcTask}, nil)
		// Second batch: empty → loop ends (afterSeq=0 after first batch)
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, originalID,
			[]string{domain.TaskStatusMaxRetriesExhausted}, 0, repository.TaskInsertBatchSize).Return([]domain.Task{}, nil)
		repo.On("CreateTasks", mock.Anything, mock.MatchedBy(func(tasks []domain.Task) bool { return len(tasks) == 1 })).Return(nil)
		pub.On("PublishTasks", mock.Anything, mock.Anything, "topic").Return(nil)
		// startClonedJob — cloned job ID is unknown at test time, so use Anything
		repo.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		result, err := svc.RetryJob(context.Background(), originalID, false, "ref")
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("reuses existing pending clone and re-publishes", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		pub := &testutil.MockPublisher{}
		svc := testService(repo, map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", Publisher: pub, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}},
		})

		originalID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "test-job", Status: domain.JobStatusCompleted}
		clonedID := uuid.New()
		cloned := &domain.Job{ID: clonedID, Name: "test-job", Status: domain.JobStatusPending}
		existingTask := domain.Task{ID: uuid.New(), JobID: clonedID, Status: domain.TaskStatusPending, SequenceNumber: 0}

		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
		repo.On("FindPendingJobByParentID", mock.Anything, originalID).Return(cloned, nil)
		// streamPublishTasks: first page returns 1 task for the existing clone
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, clonedID,
			[]string(nil), -1, repository.TaskInsertBatchSize).Return([]domain.Task{existingTask}, nil)
		// second page: empty → done
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, clonedID,
			[]string(nil), 0, repository.TaskInsertBatchSize).Return([]domain.Task{}, nil)
		pub.On("PublishTasks", mock.Anything, mock.Anything, "topic").Return(nil)
		// startClonedJob uses the known clonedID returned by FindPendingJobByParentID
		repo.On("UpdateJob", mock.Anything, clonedID, mock.Anything).Return(nil)
		repo.On("UpdateJobIfStatus", mock.Anything, clonedID, domain.JobStatusRunning, mock.Anything).Return(true, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, clonedID).Return(
			repository.TaskStatusSummary{Total: 1, NonTerminal: 1}, nil)

		result, err := svc.RetryJob(context.Background(), originalID, false, "ref")
		assert.NoError(t, err)
		assert.Equal(t, clonedID, result.ID)
	})

	t.Run("reuses empty pending clone and completes", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		svc := testService(repo, map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", Publisher: &testutil.MockPublisher{}, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}, Hooks: jobconfig.NopHooks()},
		})

		originalID := uuid.New()
		clonedID := uuid.New()
		original := &domain.Job{ID: originalID, Name: "test-job", Status: domain.JobStatusCompleted}
		cloned := &domain.Job{ID: clonedID, Name: "test-job", Status: domain.JobStatusPending}

		repo.On("FindJobByID", mock.Anything, originalID).Return(original, nil)
		repo.On("FindPendingJobByParentID", mock.Anything, originalID).Return(cloned, nil)
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, clonedID,
			[]string(nil), -1, repository.TaskInsertBatchSize).Return([]domain.Task{}, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, clonedID).Return(repository.TaskStatusSummary{Total: 0}, nil).Twice()
		repo.On("FindJobByID", mock.Anything, clonedID).Return(cloned, nil)
		repo.On("UpdateJob", mock.Anything, clonedID, mock.Anything).Return(nil)
		repo.On("UpdateJobIfStatus", mock.Anything, clonedID, domain.JobStatusRunning, mock.Anything).Return(true, nil)

		result, err := svc.RetryJob(context.Background(), originalID, false, "ref")
		assert.NoError(t, err)
		assert.Equal(t, domain.JobStatusCompleted, result.Status)
	})
}

func TestGetRegisteredJobs(t *testing.T) {
	svc := testService(&testutil.MockRepository{}, map[string]jobconfig.JobConfig{
		"job-a": {Name: "job-a"},
		"job-b": {Name: "job-b"},
	})

	jobs := svc.GetRegisteredJobs()
	assert.Len(t, jobs, 2)
	assert.Contains(t, jobs, "job-a")
	assert.Contains(t, jobs, "job-b")

	// Returned map must be a copy: mutating it must not affect the service.
	delete(jobs, "job-a")
	assert.Contains(t, svc.jobs, "job-a")
}

func TestNewValidatesConfig(t *testing.T) {
	_, err := New(Config{})
	assert.ErrorContains(t, err, "config validation failed")
}

// fakeGormDB builds a *gorm.DB backed by the pgx stdlib driver without ever
// dialing a real Postgres server. sql.Open and gorm.Open with an explicit
// Conn are both lazy — no network I/O happens until a query is executed —
// so this is safe to use in unit tests that only need a non-nil *gorm.DB.
func fakeGormDB(t *testing.T) *gorm.DB {
	t.Helper()
	sqlDB, err := sql.Open("pgx", "postgres://user:pass@127.0.0.1:5432/db?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{
		Logger: gormLogger.Default.LogMode(gormLogger.Silent),
		// gorm.Open pings the connection by default; disable that so this
		// helper works without a real Postgres server listening.
		DisableAutomaticPing: true,
	})
	require.NoError(t, err)
	return db
}

func TestNewBuildsServiceWithProvidedDB(t *testing.T) {
	cfg := Config{
		ServiceName: "svc",
		Redis:       RedisConfig{Addr: "127.0.0.1:6379"},
		Logger:      logging.NopLogger{},
		DB:          fakeGormDB(t),
	}

	svc, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.NotNil(t, svc.DB())
	assert.Empty(t, svc.GetRegisteredJobs())
}

func TestServiceStartAndStop(t *testing.T) {
	redisOpt := asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}
	rootCtx, cancel := context.WithCancel(context.Background())
	svc := &Service{
		cfg:       Config{ServiceName: "svc"},
		logger:    logging.NopLogger{},
		scheduler: asynq.NewScheduler(redisOpt, nil),
		jobs:      make(map[string]jobconfig.JobConfig),
		registry:  transport.NewConsumerRegistry(logging.NopLogger{}),
		rootCtx:   rootCtx,
		cancel:    cancel,
	}

	scheduledCfg := jobconfig.JobConfig{
		Name: "scheduled-job", JobExecutor: &testutil.MockJobExecutor{}, TaskExecutor: &testutil.MockTaskExecutor{},
		Schedule:          "0 * * * *",
		JobCompletionCron: "*/5 * * * *",
		Kafka:             jobconfig.KafkaConfig{Brokers: []string{"localhost:9092"}, TaskTopic: "topic", ConsumerGroup: "group"},
		Publisher:         &testutil.MockPublisher{}, TaskReader: &noopReader{},
	}
	require.NoError(t, svc.Register(scheduledCfg))

	manualCfg := jobconfig.JobConfig{
		Name: "manual-job", JobExecutor: &testutil.MockJobExecutor{}, TaskExecutor: &testutil.MockTaskExecutor{},
		JobCompletionCron: "*/5 * * * *",
		Kafka:             jobconfig.KafkaConfig{Brokers: []string{"localhost:9092"}, TaskTopic: "topic2", ConsumerGroup: "group2"},
		Publisher:         &testutil.MockPublisher{}, TaskReader: &noopReader{},
	}
	require.NoError(t, svc.RegisterManual(manualCfg))

	mux := asynq.NewServeMux()
	assert.NoError(t, svc.Start(context.Background(), mux))

	// Stop must be safe to call and should not panic even without a live
	// Redis connection (the scheduler/registry only dial lazily).
	assert.NotPanics(t, svc.Stop)
}

func TestStartConsoleRequiresDB(t *testing.T) {
	svc := testService(&testutil.MockRepository{}, nil)
	srv, err := svc.StartConsole(":0")
	assert.Nil(t, srv)
	assert.ErrorContains(t, err, "console requires a database connection")
}

func TestServiceDB(t *testing.T) {
	t.Run("returns nil for a mock repository", func(t *testing.T) {
		svc := testService(&testutil.MockRepository{}, nil)
		assert.Nil(t, svc.DB())
	})

	t.Run("unwraps the concrete repository's db field", func(t *testing.T) {
		svc := &Service{repository: repository.NewRepository(nil)}
		// The underlying db is nil because this test never connects to a
		// real database; DB() should just forward it unchanged.
		assert.Nil(t, svc.DB())
	})
}

func TestStartConsoleRequiresDBEvenWithConcreteRepository(t *testing.T) {
	svc := &Service{logger: logging.NopLogger{}, repository: repository.NewRepository(nil), jobs: map[string]jobconfig.JobConfig{}}
	srv, err := svc.StartConsole(":0")
	assert.Nil(t, srv)
	assert.ErrorContains(t, err, "console requires a database connection")
}

func TestStartConsoleStartsServer(t *testing.T) {
	db := fakeGormDB(t)
	svc := &Service{
		logger:     logging.NopLogger{},
		repository: repository.NewRepository(db),
		jobs:       map[string]jobconfig.JobConfig{},
	}
	srv, err := svc.StartConsole(":0")
	require.NoError(t, err)
	require.NotNil(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
}
