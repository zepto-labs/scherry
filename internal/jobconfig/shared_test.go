package jobconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func validSharedConfig() SharedKafkaConfig {
	return SharedKafkaConfig{
		Kafka:        KafkaConfig{Brokers: []string{"b:9092"}, TaskTopic: "t", ConsumerGroup: "g"},
		TaskExecutor: stubTaskExecutor{},
		Jobs: []JobConfig{
			{Name: "job-a", JobExecutor: stubJobExecutor{}, JobCompletionCron: "* * * * *", Schedule: "* * * * *"},
			{Name: "job-b", JobExecutor: stubJobExecutor{}, JobCompletionCron: "* * * * *"},
		},
	}
}

func TestValidateShared(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		assert.NoError(t, ValidateShared(validSharedConfig()))
	})

	t.Run("no brokers", func(t *testing.T) {
		cfg := validSharedConfig()
		cfg.Kafka.Brokers = nil
		assert.ErrorContains(t, ValidateShared(cfg), "kafka brokers are required")
	})

	t.Run("no task topic", func(t *testing.T) {
		cfg := validSharedConfig()
		cfg.Kafka.TaskTopic = " "
		assert.ErrorContains(t, ValidateShared(cfg), "kafka task topic is required")
	})

	t.Run("no consumer group", func(t *testing.T) {
		cfg := validSharedConfig()
		cfg.Kafka.ConsumerGroup = ""
		assert.ErrorContains(t, ValidateShared(cfg), "kafka consumer group is required")
	})

	t.Run("nil shared task executor", func(t *testing.T) {
		cfg := validSharedConfig()
		cfg.TaskExecutor = nil
		assert.ErrorContains(t, ValidateShared(cfg), "shared task executor cannot be nil")
	})

	t.Run("fewer than two jobs", func(t *testing.T) {
		cfg := validSharedConfig()
		cfg.Jobs = cfg.Jobs[:1]
		assert.ErrorContains(t, ValidateShared(cfg), "at least two jobs")
	})

	t.Run("empty job name", func(t *testing.T) {
		cfg := validSharedConfig()
		cfg.Jobs[1].Name = "  "
		assert.ErrorContains(t, ValidateShared(cfg), "job name cannot be empty")
	})

	t.Run("nil job executor", func(t *testing.T) {
		cfg := validSharedConfig()
		cfg.Jobs[0].JobExecutor = nil
		assert.ErrorContains(t, ValidateShared(cfg), "job executor cannot be nil")
	})

	t.Run("missing completion cron", func(t *testing.T) {
		cfg := validSharedConfig()
		cfg.Jobs[0].JobCompletionCron = ""
		assert.ErrorContains(t, ValidateShared(cfg), "job completion cron cannot be empty")
	})

	t.Run("per-job task executor rejected", func(t *testing.T) {
		cfg := validSharedConfig()
		cfg.Jobs[0].TaskExecutor = stubTaskExecutor{}
		assert.ErrorContains(t, ValidateShared(cfg), "task executor must be set on the shared config")
	})

	t.Run("per-job kafka rejected", func(t *testing.T) {
		cfg := validSharedConfig()
		cfg.Jobs[0].Kafka = KafkaConfig{TaskTopic: "other"}
		assert.ErrorContains(t, ValidateShared(cfg), "kafka config must be set on the shared config")
	})

	t.Run("duplicate job names", func(t *testing.T) {
		cfg := validSharedConfig()
		cfg.Jobs[1].Name = "job-a"
		assert.ErrorContains(t, ValidateShared(cfg), "duplicate job name")
	})
}
