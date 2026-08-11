package jobconfig

import (
	"errors"
	"strings"
)

// IsEnabled reports whether a job should run: true when Enabled is unset,
// otherwise the result of calling it. It is a free function (rather than a
// method) because JobConfig already has an exported Enabled field with that
// name, and Go does not allow a field and method to share a name.
func IsEnabled(jc JobConfig) bool {
	if jc.Enabled != nil {
		return jc.Enabled()
	}
	return true
}

// ValidateManual validates a job configuration that has no cron schedule.
func ValidateManual(cfg JobConfig) error {
	if strings.TrimSpace(cfg.Name) == "" {
		return errors.New("job name cannot be empty")
	}
	if cfg.JobExecutor == nil {
		return errors.New("job executor cannot be nil")
	}
	if cfg.TaskExecutor == nil {
		return errors.New("task executor cannot be nil")
	}
	if len(cfg.Kafka.Brokers) == 0 {
		return errors.New("kafka brokers are required")
	}
	if strings.TrimSpace(cfg.Kafka.TaskTopic) == "" {
		return errors.New("kafka task topic is required")
	}
	if strings.TrimSpace(cfg.Kafka.ConsumerGroup) == "" {
		return errors.New("kafka consumer group is required")
	}
	if strings.TrimSpace(cfg.JobCompletionCron) == "" {
		return errors.New("job completion cron cannot be empty")
	}
	return nil
}

// Validate validates a scheduled job configuration.
func Validate(cfg JobConfig) error {
	if err := ValidateManual(cfg); err != nil {
		return err
	}
	if strings.TrimSpace(cfg.Schedule) == "" {
		return errors.New("schedule cannot be empty")
	}
	return nil
}
