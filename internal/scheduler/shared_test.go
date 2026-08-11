package scheduler

import (
	"context"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/testutil"
	"github.com/zepto-labs/scherry/internal/transport"
)

func newSharedTestService(t *testing.T) *Service {
	t.Helper()
	redisOpt := asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}
	rootCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Service{
		logger:    logging.NopLogger{},
		scheduler: asynq.NewScheduler(redisOpt, nil),
		jobs:      make(map[string]jobconfig.JobConfig),
		registry:  transport.NewConsumerRegistry(logging.NopLogger{}),
		rootCtx:   rootCtx,
	}
}

func sharedRegistration() jobconfig.SharedKafkaConfig {
	return jobconfig.SharedKafkaConfig{
		Kafka: jobconfig.KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			TaskTopic:     "shared-topic",
			ConsumerGroup: "shared-group",
		},
		TaskExecutor: &testutil.MockTaskExecutor{},
		Jobs: []jobconfig.JobConfig{
			{Name: "shared-a", JobExecutor: &testutil.MockJobExecutor{}, Schedule: "0 * * * *", JobCompletionCron: "*/5 * * * *"},
			{Name: "shared-b", JobExecutor: &testutil.MockJobExecutor{}, JobCompletionCron: "*/5 * * * *"},
		},
		Publisher:  &testutil.MockPublisher{},
		TaskReader: &noopReader{},
	}
}

func TestRegisterShared(t *testing.T) {
	svc := newSharedTestService(t)

	require.NoError(t, svc.RegisterShared(sharedRegistration()))

	require.Contains(t, svc.jobs, "shared-a")
	require.Contains(t, svc.jobs, "shared-b")

	a := svc.jobs["shared-a"]
	b := svc.jobs["shared-b"]

	// Both jobs inherit the common Kafka config and the shared TaskExecutor.
	assert.Equal(t, "shared-group", a.Kafka.ConsumerGroup)
	assert.Equal(t, "shared-topic", a.Kafka.TaskTopic)
	assert.Equal(t, "shared-group", b.Kafka.ConsumerGroup)
	assert.NotNil(t, a.TaskExecutor)
	assert.NotNil(t, b.TaskExecutor)
	assert.NotNil(t, a.Publisher)
	assert.NotNil(t, b.Publisher)

	// A single shared consumer is started for both jobs (no retry topic set).
	assert.Equal(t, 1, svc.registry.Len())
}

func TestRegisterSharedStartsRetryConsumer(t *testing.T) {
	svc := newSharedTestService(t)
	cfg := sharedRegistration()
	cfg.Kafka.TaskRetryTopic = "shared-retry"
	cfg.RetryReader = &noopReader{}

	require.NoError(t, svc.RegisterShared(cfg))

	// One main consumer + one retry consumer, shared by both jobs.
	assert.Equal(t, 2, svc.registry.Len())
}

func TestRegisterSharedValidationError(t *testing.T) {
	svc := newSharedTestService(t)
	cfg := sharedRegistration()
	cfg.Jobs = cfg.Jobs[:1]

	err := svc.RegisterShared(cfg)
	assert.ErrorContains(t, err, "at least two jobs")
	assert.Empty(t, svc.jobs)
}

func TestRegisterSharedRejectsDuplicateJobName(t *testing.T) {
	svc := newSharedTestService(t)
	svc.jobs["shared-a"] = jobconfig.JobConfig{Name: "shared-a"}

	err := svc.RegisterShared(sharedRegistration())
	assert.ErrorContains(t, err, "already registered")
}

func TestRegisterSharedThenPlainRegisterRejected(t *testing.T) {
	svc := newSharedTestService(t)
	require.NoError(t, svc.RegisterShared(sharedRegistration()))

	plain := jobconfig.JobConfig{
		Name:              "intruder",
		JobExecutor:       &testutil.MockJobExecutor{},
		TaskExecutor:      &testutil.MockTaskExecutor{},
		JobCompletionCron: "*/5 * * * *",
		Kafka: jobconfig.KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			TaskTopic:     "shared-topic",
			ConsumerGroup: "shared-group",
		},
		Publisher:  &testutil.MockPublisher{},
		TaskReader: &noopReader{},
	}

	err := svc.RegisterManual(plain)
	assert.ErrorContains(t, err, "managed by a shared consumer")
	assert.NotContains(t, svc.jobs, "intruder")
}

func TestPlainRegisterThenRegisterSharedRejected(t *testing.T) {
	svc := newSharedTestService(t)
	plain := jobconfig.JobConfig{
		Name:              "existing",
		JobExecutor:       &testutil.MockJobExecutor{},
		TaskExecutor:      &testutil.MockTaskExecutor{},
		JobCompletionCron: "*/5 * * * *",
		Kafka: jobconfig.KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			TaskTopic:     "shared-topic",
			ConsumerGroup: "shared-group",
		},
		Publisher:  &testutil.MockPublisher{},
		TaskReader: &noopReader{},
	}
	require.NoError(t, svc.RegisterManual(plain))

	err := svc.RegisterShared(sharedRegistration())
	assert.ErrorContains(t, err, "already in use")
}
