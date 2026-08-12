package scheduler

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"gorm.io/gorm"

	"github.com/zepto-labs/scherry/internal/console"
	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/executor"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
	"github.com/zepto-labs/scherry/internal/transport"
)

type Service struct {
	cfg        Config
	logger     logging.Logger
	repository repository.Repository
	scheduler  *asynq.Scheduler
	jobs       map[string]jobconfig.JobConfig
	registry   *transport.ConsumerRegistry
	rootCtx    context.Context
	cancel     context.CancelFunc

	// sharedConsumers records the (consumer group, task topic) pairs owned by a
	// RegisterShared call, so that a later plain Register/RegisterManual cannot
	// silently join a shared consumer by reusing the same group and topic.
	sharedConsumers map[consumerRef]struct{}
}

// consumerRef identifies a Kafka consumer by its group and topic.
type consumerRef struct {
	group string
	topic string
}

func New(cfg Config) (*Service, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	logger := cfg.Logger

	db := cfg.DB
	if db == nil {
		opened, err := openDatabase(cfg.Database)
		if err != nil {
			return nil, err
		}
		db = opened
	}

	redisOpt := cfg.Redis.AsynqClientOpt()

	rootCtx, cancel := context.WithCancel(context.Background())
	return &Service{
		cfg:        cfg,
		logger:     logger,
		repository: repository.NewRepository(db),
		scheduler:  asynq.NewScheduler(redisOpt, nil),
		jobs:       make(map[string]jobconfig.JobConfig),
		registry:   transport.NewConsumerRegistry(logger),
		rootCtx:    rootCtx,
		cancel:     cancel,
	}, nil
}

// Register registers a scheduled job with a cron expression. The job is
// triggered automatically by the Asynq scheduler according to its Schedule.
func (s *Service) Register(jobCfg jobconfig.JobConfig) error {
	if err := jobconfig.Validate(jobCfg); err != nil {
		return err
	}
	if _, exists := s.jobs[jobCfg.Name]; exists {
		return fmt.Errorf("job %s already registered", jobCfg.Name)
	}

	if err := s.setupJobInfra(&jobCfg); err != nil {
		return err
	}

	s.jobs[jobCfg.Name] = jobCfg

	task := asynq.NewTask(jobCfg.Name, nil, jobCfg.AsynqOptions...)
	entryID, err := s.scheduler.Register(jobCfg.Schedule, task)
	if err != nil {
		return fmt.Errorf("register asynq schedule for %s: %w", jobCfg.Name, err)
	}

	s.logger.Info("registered job", "name", jobCfg.Name, "asynq_entry_id", entryID)
	return nil
}

// RegisterManual registers a job without a cron schedule. The job is never
// triggered automatically; use Service.Trigger or Service.ExecuteJob to run it
// on-demand.
func (s *Service) RegisterManual(jobCfg jobconfig.JobConfig) error {
	if err := jobconfig.ValidateManual(jobCfg); err != nil {
		return err
	}
	if _, exists := s.jobs[jobCfg.Name]; exists {
		return fmt.Errorf("job %s already registered", jobCfg.Name)
	}

	if err := s.setupJobInfra(&jobCfg); err != nil {
		return err
	}

	s.jobs[jobCfg.Name] = jobCfg

	s.logger.Info("registered manual job", "name", jobCfg.Name)
	return nil
}

// RegisterShared registers two or more jobs that explicitly share one Kafka
// consumer, one Kafka producer, and one TaskExecutor. It is the only way to
// couple jobs onto shared consumer resources; passing the same ConsumerGroup
// and TaskTopic to separate Register/RegisterManual calls does not couple them
// and is rejected.
//
// Each job in cfg.Jobs keeps its own triggering/splitting side (Name,
// JobExecutor, Schedule, Retry, JobCompletionCron, hooks, ...). Jobs with a
// Schedule are wired to the Asynq cron; jobs without one are manual and run via
// Trigger. Every task any of them produces flows through the single shared
// consumer and is handled by cfg.TaskExecutor.
func (s *Service) RegisterShared(cfg jobconfig.SharedKafkaConfig) error {
	if err := jobconfig.ValidateShared(cfg); err != nil {
		return err
	}

	for i := range cfg.Jobs {
		if _, exists := s.jobs[cfg.Jobs[i].Name]; exists {
			return fmt.Errorf("job %s already registered", cfg.Jobs[i].Name)
		}
	}
	if owner, ok := s.consumerOwner(cfg.Kafka.ConsumerGroup, cfg.Kafka.TaskTopic); ok {
		return fmt.Errorf("consumer group %q with topic %q already in use by %s", cfg.Kafka.ConsumerGroup, cfg.Kafka.TaskTopic, owner)
	}

	publisher := cfg.Publisher
	if publisher == nil {
		publisher = transport.NewTaskPublisher(transport.NewKafkaWriter(cfg.Kafka), s.logger)
	}
	taskReader := cfg.TaskReader
	if taskReader == nil {
		taskReader = transport.NewKafkaReader(cfg.Kafka, cfg.Kafka.ConsumerGroup, cfg.Kafka.TaskTopic)
	}
	retryGroup := cfg.Kafka.ConsumerGroup + "-retry"
	retryReader := cfg.RetryReader
	if retryReader == nil && cfg.Kafka.TaskRetryTopic != "" {
		retryReader = transport.NewKafkaReader(cfg.Kafka, retryGroup, cfg.Kafka.TaskRetryTopic)
	}

	// Finalise every job's config and register its cron/schedule before
	// committing anything to s.jobs, so a mid-loop failure leaves no job
	// half-registered.
	prepared := make([]jobconfig.JobConfig, 0, len(cfg.Jobs))
	for i := range cfg.Jobs {
		jobCfg := cfg.Jobs[i]
		jobCfg.Kafka = cfg.Kafka
		jobCfg.TaskExecutor = cfg.TaskExecutor
		jobCfg.Publisher = publisher
		jobCfg.Hooks = jobconfig.MergeHooks(jobCfg.Hooks)
		if jobCfg.Retry.MaxRetries == 0 {
			jobCfg.Retry.MaxRetries = 3
		}

		task := asynq.NewTask(executor.CompletionCheckTaskType(jobCfg.Name), nil)
		entryID, err := s.scheduler.Register(jobCfg.JobCompletionCron, task)
		if err != nil {
			return fmt.Errorf("register completion cron for %s: %w", jobCfg.Name, err)
		}
		s.logger.Info("registered job completion cron", "name", jobCfg.Name, "cron", jobCfg.JobCompletionCron, "asynq_entry_id", entryID)

		if jobCfg.Schedule != "" {
			schedTask := asynq.NewTask(jobCfg.Name, nil, jobCfg.AsynqOptions...)
			schedEntryID, err := s.scheduler.Register(jobCfg.Schedule, schedTask)
			if err != nil {
				return fmt.Errorf("register asynq schedule for %s: %w", jobCfg.Name, err)
			}
			s.logger.Info("registered job", "name", jobCfg.Name, "asynq_entry_id", schedEntryID)
		}
		prepared = append(prepared, jobCfg)
	}

	if err := s.registry.Start(s.rootCtx, taskReader, s.taskRunner(), false, cfg.Kafka.ConsumerGroup, cfg.Kafka.TaskTopic); err != nil {
		return fmt.Errorf("start shared task consumer: %w", err)
	}
	if retryReader != nil {
		if err := s.registry.Start(s.rootCtx, retryReader, s.taskRunner(), true, retryGroup, cfg.Kafka.TaskRetryTopic); err != nil {
			return fmt.Errorf("start shared retry consumer: %w", err)
		}
	}

	for _, jobCfg := range prepared {
		s.jobs[jobCfg.Name] = jobCfg
	}
	s.reserveSharedConsumer(cfg.Kafka.ConsumerGroup, cfg.Kafka.TaskTopic)

	s.logger.Info("registered shared consumer",
		"consumer_group", cfg.Kafka.ConsumerGroup,
		"task_topic", cfg.Kafka.TaskTopic,
		"jobs", len(prepared),
	)
	return nil
}

// isSharedConsumer reports whether the (group, topic) pair is owned by a
// RegisterShared call.
func (s *Service) isSharedConsumer(group, topic string) bool {
	_, ok := s.sharedConsumers[consumerRef{group: group, topic: topic}]
	return ok
}

func (s *Service) reserveSharedConsumer(group, topic string) {
	if s.sharedConsumers == nil {
		s.sharedConsumers = make(map[consumerRef]struct{})
	}
	s.sharedConsumers[consumerRef{group: group, topic: topic}] = struct{}{}
}

// consumerOwner returns a human-readable description of what already uses the
// given (group, topic) pair, if anything: an existing job or a shared consumer.
func (s *Service) consumerOwner(group, topic string) (string, bool) {
	if s.isSharedConsumer(group, topic) {
		return "a shared consumer", true
	}
	for name, jc := range s.jobs {
		if jc.Kafka.ConsumerGroup == group && jc.Kafka.TaskTopic == topic {
			return fmt.Sprintf("job %q", name), true
		}
	}
	return "", false
}

// Trigger executes a registered job by name. It can be used with both
// scheduled and manual jobs, and is the programmatic equivalent of a cron
// fire for manual jobs registered via RegisterManual.
func (s *Service) Trigger(ctx context.Context, jobName, refID string, metadata map[string]interface{}) error {
	return s.ExecuteJob(ctx, jobName, refID, metadata)
}

// setupJobInfra wires default Kafka publisher, task reader, and retry reader
// when they are not provided in the config, starts the Kafka consumers, and
// normalises the job's Hooks so every hook field is non-nil.
func (s *Service) setupJobInfra(jobCfg *jobconfig.JobConfig) error {
	if s.isSharedConsumer(jobCfg.Kafka.ConsumerGroup, jobCfg.Kafka.TaskTopic) {
		return fmt.Errorf("consumer group %q with topic %q is managed by a shared consumer; register this job together with the others via RegisterShared", jobCfg.Kafka.ConsumerGroup, jobCfg.Kafka.TaskTopic)
	}
	jobCfg.Hooks = jobconfig.MergeHooks(jobCfg.Hooks)
	if jobCfg.Publisher == nil {
		jobCfg.Publisher = transport.NewTaskPublisher(transport.NewKafkaWriter(jobCfg.Kafka), s.logger)
	}
	if jobCfg.TaskReader == nil {
		jobCfg.TaskReader = transport.NewKafkaReader(jobCfg.Kafka, jobCfg.Kafka.ConsumerGroup, jobCfg.Kafka.TaskTopic)
	}

	retryGroup := jobCfg.Kafka.ConsumerGroup + "-retry"

	if jobCfg.RetryReader == nil && jobCfg.Kafka.TaskRetryTopic != "" {
		jobCfg.RetryReader = transport.NewKafkaReader(jobCfg.Kafka, retryGroup, jobCfg.Kafka.TaskRetryTopic)
	}
	if jobCfg.Retry.MaxRetries == 0 {
		jobCfg.Retry.MaxRetries = 3
	}

	if err := s.registry.Start(s.rootCtx, jobCfg.TaskReader, s.taskRunner(), false, jobCfg.Kafka.ConsumerGroup, jobCfg.Kafka.TaskTopic); err != nil {
		return fmt.Errorf("start task consumer: %w", err)
	}
	if jobCfg.RetryReader != nil {
		if err := s.registry.Start(s.rootCtx, jobCfg.RetryReader, s.taskRunner(), true, retryGroup, jobCfg.Kafka.TaskRetryTopic); err != nil {
			return fmt.Errorf("start retry consumer: %w", err)
		}
	}

	if jobCfg.JobCompletionCron != "" {
		task := asynq.NewTask(executor.CompletionCheckTaskType(jobCfg.Name), nil)
		entryID, err := s.scheduler.Register(jobCfg.JobCompletionCron, task)
		if err != nil {
			return fmt.Errorf("register completion cron for %s: %w", jobCfg.Name, err)
		}
		s.logger.Info("registered job completion cron", "name", jobCfg.Name, "cron", jobCfg.JobCompletionCron, "asynq_entry_id", entryID)
	}
	return nil
}

// taskRunner adapts Service to transport.TaskRunner so the Kafka consumers
// can execute tasks without transport importing scheduler.
type taskRunner struct{ svc *Service }

func (r taskRunner) ExecuteTask(ctx context.Context, message []byte) error {
	return r.svc.ExecuteTask(ctx, message)
}

func (s *Service) taskRunner() transport.TaskRunner { return taskRunner{svc: s} }

func (s *Service) Start(ctx context.Context, mux *asynq.ServeMux) error {
	for jobName := range s.jobs {
		jobCfg := s.jobs[jobName]
		if jobCfg.Schedule != "" {
			name := jobName
			mux.HandleFunc(name, func(c context.Context, task *asynq.Task) error {
				taskID, _ := asynq.GetTaskID(c)
				retryCount, _ := asynq.GetRetryCount(c)
				maxRetry, _ := asynq.GetMaxRetry(c)
				metadata := map[string]interface{}{
					"asynq_task_id":     taskID,
					"asynq_retry_count": retryCount,
					"asynq_max_retry":   maxRetry,
				}
				if err := s.ExecuteJob(c, task.Type(), taskID, metadata); err != nil {
					s.logger.Error("scheduled job failed", "job", task.Type(), "error", err)
					return err
				}
				return nil
			})
			s.logger.Info("registered asynq handler", "job", name)
		}

		if jobCfg.JobCompletionCron != "" {
			name := jobName
			mux.HandleFunc(executor.CompletionCheckTaskType(name), func(c context.Context, task *asynq.Task) error {
				if err := s.CheckJobCompletions(c, name); err != nil {
					s.logger.Error("job completion check failed", "job", name, "error", err)
					return err
				}
				return nil
			})
			s.logger.Info("registered job completion check handler", "job", name)
		}
	}

	if err := s.scheduler.Start(); err != nil {
		return fmt.Errorf("start asynq scheduler: %w", err)
	}
	s.logger.Info("scheduler started", "service", s.cfg.ServiceName)
	return nil
}

func (s *Service) Stop() {
	s.logger.Info("stopping scheduler", "service", s.cfg.ServiceName)
	s.cancel()
	s.scheduler.Shutdown()
	s.registry.StopAll()
}

func (s *Service) GetRegisteredJobs() map[string]jobconfig.JobConfig {
	out := make(map[string]jobconfig.JobConfig, len(s.jobs))
	for name, cfg := range s.jobs {
		out[name] = cfg
	}
	return out
}

func (s *Service) DB() *gorm.DB {
	if s.repository == nil {
		return nil
	}
	return s.repository.DB()
}

// TerminateJob manually cancels a non-terminal job identified by jobID, marking
// the job and all of its currently-pending tasks as CANCELLED. Tasks that are
// already in a terminal state (COMPLETED, FAILED, MAX_RETRIES_EXHAUSTED,
// REJECTED, CANCELLED) are left unchanged. Running tasks that are consumed from
// Kafka after termination will see the parent job as cancelled and exit without
// executing (consistent with the CancelPrevious behaviour).
//
// Returns repository.ErrJobAlreadyTerminal when the job is already in a terminal state.
func (s *Service) TerminateJob(ctx context.Context, jobID uuid.UUID) error {
	s.logger.Info("terminating job", "job_id", jobID)
	if err := s.repository.TerminateJob(ctx, jobID); err != nil {
		return fmt.Errorf("terminate job %s: %w", jobID, err)
	}
	s.logger.Info("job terminated", "job_id", jobID)
	return nil
}

// GetJobTasks returns a paginated slice of tasks for the given job, ordered by
// sequence_number ASC to match the order in which the JobExecutor originally
// generated them. The filter's JobID field is required; Limit, Offset, and
// Status are optional (Limit defaults to 50, max 200).
//
// This method requires the Service to be backed by a database connection.
func (s *Service) GetJobTasks(ctx context.Context, filter console.TaskFilter) (*console.TaskPage, error) {
	db := s.DB()
	if db == nil {
		return nil, errors.New("GetJobTasks requires a database connection")
	}
	return console.ListTasksForJob(ctx, db, filter)
}

// ConsoleHandler returns an http.Handler serving the job-history console: an
// embedded single-page UI at "/" plus a small JSON API under "/api". Mount it
// wherever you like, or use StartConsole to run it on its own port.
//
// The console has no built-in authentication — serve it on localhost or behind
// your own auth/network controls.
func (s *Service) ConsoleHandler() http.Handler {
	return console.NewHandler(s)
}

// StartConsole builds the console handler and serves it on addr (e.g. ":3002")
// in a background goroutine, returning the server so the caller can Shutdown it.
func (s *Service) StartConsole(addr string) (*http.Server, error) {
	if s.DB() == nil {
		return nil, errors.New("console requires a database connection")
	}
	srv := &http.Server{Addr: addr, Handler: s.ConsoleHandler()}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("console server stopped", "error", err)
		}
	}()
	s.logger.Info("history console listening", "addr", addr)
	return srv, nil
}

// ExecuteJob runs jobName's JobExecutor and publishes the resulting tasks.
// Delegates to internal/executor; see executor.ExecuteJob for the full flow.
func (s *Service) ExecuteJob(ctx context.Context, jobName, refID string, metadata map[string]interface{}) error {
	return executor.ExecuteJob(ctx, s.repository, s.logger, s.jobs, jobName, refID, metadata)
}

// ExecuteTask runs a single task message end-to-end.
// Delegates to internal/executor; see executor.ExecuteTask for the full flow.
func (s *Service) ExecuteTask(ctx context.Context, message []byte) error {
	return executor.ExecuteTask(ctx, s.repository, s.logger, s.jobs, message)
}

// RetryJob re-publishes the tasks of a terminal job as a new cloned job.
// Delegates to internal/executor; see executor.RetryJob for the full flow.
func (s *Service) RetryJob(ctx context.Context, jobID uuid.UUID, fullRetry bool, refID string) (*domain.Job, error) {
	return executor.RetryJob(ctx, s.repository, s.logger, s.jobs, jobID, fullRetry, refID)
}

// RetryTriggerJob re-runs a trigger-failure job that never created tasks.
func (s *Service) RetryTriggerJob(ctx context.Context, jobID uuid.UUID, refID string) (*domain.Job, error) {
	return executor.RetryTriggerJob(ctx, s.repository, s.logger, s.jobs, jobID, refID)
}

// AsynqRedisOpt returns the Redis connection options used by Asynq.
func (s *Service) AsynqRedisOpt() asynq.RedisClientOpt {
	return s.cfg.Redis.AsynqClientOpt()
}

// RetryAsynqTask re-enqueues an archived or retrying Asynq trigger task.
func (s *Service) RetryAsynqTask(ctx context.Context, queue, taskID string) error {
	if queue == "" {
		queue = "default"
	}
	inspector := asynq.NewInspector(s.cfg.Redis.AsynqClientOpt())
	defer inspector.Close()
	if err := inspector.RunTask(queue, taskID); err != nil {
		return fmt.Errorf("retry asynq task %s: %w", taskID, err)
	}
	s.logger.Info("retried asynq trigger", "task_id", taskID, "queue", queue)
	return nil
}

// FindJobByID loads a single job row by id.
func (s *Service) FindJobByID(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	return s.repository.FindJobByID(ctx, jobID)
}

// RegisterRetryHandler registers an Asynq handler for retryJobType that
// invokes RetryJob using the Asynq task ID as the deduplication ref.
func (s *Service) RegisterRetryHandler(mux *asynq.ServeMux, retryJobType string) {
	executor.RegisterRetryHandler(mux, retryJobType, s.repository, s.logger, s.jobs)
}

// CheckJobCompletions finalizes jobName's RUNNING instances whose tasks have
// all reached a terminal state (or which have no tasks at all). It is the
// handler behind JobConfig.JobCompletionCron, and can also be called
// directly (e.g. from your own scheduler or an admin action) whenever you
// want a check to run outside the configured cron.
// Delegates to internal/executor; see executor.CheckJobCompletions.
func (s *Service) CheckJobCompletions(ctx context.Context, jobName string) error {
	return executor.CheckJobCompletions(ctx, s.repository, s.logger, s.jobs, jobName)
}
