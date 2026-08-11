package jobconfig

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// stubJobExecutor and stubTaskExecutor are trivial, always-nil-returning
// implementations used only to satisfy the "is set" checks in Validate /
// ValidateManual; they deliberately avoid depending on internal/testutil to
// sidestep the jobconfig -> testutil -> jobconfig test import cycle.
type stubJobExecutor struct{}

func (stubJobExecutor) Execute(context.Context, map[string]interface{}) ([]TaskData, error) {
	return nil, nil
}

type stubTaskExecutor struct{}

func (stubTaskExecutor) Execute(context.Context, map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
}

func TestJobConfigEnabled(t *testing.T) {
	t.Run("defaults to true when Enabled is nil", func(t *testing.T) {
		jc := JobConfig{}
		assert.True(t, IsEnabled(jc))
	})

	t.Run("delegates to Enabled func", func(t *testing.T) {
		jc := JobConfig{Enabled: func() bool { return false }}
		assert.False(t, IsEnabled(jc))

		jc.Enabled = func() bool { return true }
		assert.True(t, IsEnabled(jc))
	})
}

func TestValidateManualJobConfig(t *testing.T) {
	valid := JobConfig{
		Name: "job", JobExecutor: stubJobExecutor{}, TaskExecutor: stubTaskExecutor{},
		Kafka:             KafkaConfig{Brokers: []string{"b:9092"}, TaskTopic: "t", ConsumerGroup: "g"},
		JobCompletionCron: "* * * * *",
	}

	t.Run("valid", func(t *testing.T) {
		assert.NoError(t, ValidateManual(valid))
	})

	t.Run("empty name", func(t *testing.T) {
		cfg := valid
		cfg.Name = ""
		assert.ErrorContains(t, ValidateManual(cfg), "job name cannot be empty")
	})

	t.Run("nil job executor", func(t *testing.T) {
		cfg := valid
		cfg.JobExecutor = nil
		assert.ErrorContains(t, ValidateManual(cfg), "job executor cannot be nil")
	})

	t.Run("nil task executor", func(t *testing.T) {
		cfg := valid
		cfg.TaskExecutor = nil
		assert.ErrorContains(t, ValidateManual(cfg), "task executor cannot be nil")
	})

	t.Run("no brokers", func(t *testing.T) {
		cfg := valid
		cfg.Kafka.Brokers = nil
		assert.ErrorContains(t, ValidateManual(cfg), "kafka brokers are required")
	})

	t.Run("no task topic", func(t *testing.T) {
		cfg := valid
		cfg.Kafka.TaskTopic = ""
		assert.ErrorContains(t, ValidateManual(cfg), "kafka task topic is required")
	})

	t.Run("no consumer group", func(t *testing.T) {
		cfg := valid
		cfg.Kafka.ConsumerGroup = ""
		assert.ErrorContains(t, ValidateManual(cfg), "kafka consumer group is required")
	})

	t.Run("no job completion cron", func(t *testing.T) {
		cfg := valid
		cfg.JobCompletionCron = "  "
		assert.ErrorContains(t, ValidateManual(cfg), "job completion cron cannot be empty")
	})
}

func TestValidateJobConfig(t *testing.T) {
	valid := JobConfig{
		Name: "job", JobExecutor: stubJobExecutor{}, TaskExecutor: stubTaskExecutor{},
		Kafka:             KafkaConfig{Brokers: []string{"b:9092"}, TaskTopic: "t", ConsumerGroup: "g"},
		Schedule:          "* * * * *",
		JobCompletionCron: "* * * * *",
	}

	t.Run("valid", func(t *testing.T) {
		assert.NoError(t, Validate(valid))
	})

	t.Run("propagates manual validation errors", func(t *testing.T) {
		cfg := valid
		cfg.Name = ""
		assert.ErrorContains(t, Validate(cfg), "job name cannot be empty")
	})

	t.Run("empty schedule", func(t *testing.T) {
		cfg := valid
		cfg.Schedule = "  "
		assert.ErrorContains(t, Validate(cfg), "schedule cannot be empty")
	})
}

func TestKafkaConfigKeepsKafkaGoTypesUsable(t *testing.T) {
	// Sanity check that the pass-through fields keep their intended zero
	// values when unset.
	cfg := KafkaConfig{}
	assert.Nil(t, cfg.WriterConfig)
	assert.True(t, strings.TrimSpace(cfg.TaskTopic) == "")
}
