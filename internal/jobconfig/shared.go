package jobconfig

import (
	"errors"
	"fmt"
	"strings"
)

// SharedKafkaConfig couples several jobs onto one shared Kafka client — a
// single consumer (reader) and a single producer (writer) — plus one shared
// TaskExecutor that processes every message the consumer reads. Registering
// jobs through Service.RegisterShared is the only way to make jobs share these
// resources; reusing the same ConsumerGroup/topic on separate Register calls
// does not couple them.
//
// The coupled jobs stay independent on the triggering/splitting side: each has
// its own Name, JobExecutor, Schedule (or none, for manual), retry policy,
// completion cron, and lifecycle/history. They differ only in how and when
// they produce tasks — every task, regardless of which job produced it, is
// consumed by the single shared consumer and handled by the single shared
// TaskExecutor.
type SharedKafkaConfig struct {
	// Kafka is the single, common Kafka configuration for every coupled job:
	// brokers, task/retry/DLQ topics, consumer group, and consumer/writer
	// tuning. One reader and one writer are built from it and shared by all
	// jobs in Jobs.
	Kafka KafkaConfig

	// TaskExecutor is the common task processor invoked for every message the
	// shared consumer reads, regardless of which coupled job produced the task.
	TaskExecutor TaskExecutor

	// Jobs are the coupled jobs. Each carries only its triggering/splitting
	// side (Name, JobExecutor, Schedule, AsynqOptions, Retry, JobCompletionCron,
	// CancelPrevious, Enabled, Hooks). Their Kafka and TaskExecutor fields, and
	// any Publisher/TaskReader/RetryReader, must be left zero — the shared
	// config provides them.
	Jobs []JobConfig

	// Publisher, TaskReader and RetryReader are optional injection points for
	// the shared producer and consumers (primarily for testing). When nil they
	// are built from Kafka.
	Publisher   Publisher
	TaskReader  MessageReader
	RetryReader MessageReader
}

// ValidateShared validates a shared-consumer registration.
func ValidateShared(cfg SharedKafkaConfig) error {
	if len(cfg.Kafka.Brokers) == 0 {
		return errors.New("kafka brokers are required")
	}
	if strings.TrimSpace(cfg.Kafka.TaskTopic) == "" {
		return errors.New("kafka task topic is required")
	}
	if strings.TrimSpace(cfg.Kafka.ConsumerGroup) == "" {
		return errors.New("kafka consumer group is required")
	}
	if cfg.TaskExecutor == nil {
		return errors.New("shared task executor cannot be nil")
	}
	if len(cfg.Jobs) < 2 {
		return errors.New("shared consumer requires at least two jobs")
	}

	seen := make(map[string]struct{}, len(cfg.Jobs))
	for i := range cfg.Jobs {
		job := cfg.Jobs[i]
		if err := validateSharedJob(job); err != nil {
			return fmt.Errorf("shared job %q: %w", strings.TrimSpace(job.Name), err)
		}
		if _, dup := seen[job.Name]; dup {
			return fmt.Errorf("duplicate job name %q in shared consumer", job.Name)
		}
		seen[job.Name] = struct{}{}
	}
	return nil
}

func validateSharedJob(job JobConfig) error {
	if strings.TrimSpace(job.Name) == "" {
		return errors.New("job name cannot be empty")
	}
	if job.JobExecutor == nil {
		return errors.New("job executor cannot be nil")
	}
	if strings.TrimSpace(job.JobCompletionCron) == "" {
		return errors.New("job completion cron cannot be empty")
	}
	if job.TaskExecutor != nil {
		return errors.New("task executor must be set on the shared config, not on the job")
	}
	if kafkaConfigSet(job.Kafka) {
		return errors.New("kafka config must be set on the shared config, not on the job")
	}
	if job.Publisher != nil || job.TaskReader != nil || job.RetryReader != nil {
		return errors.New("publisher/reader must be set on the shared config, not on the job")
	}
	return nil
}

// kafkaConfigSet reports whether any caller-facing field of a KafkaConfig has
// been populated. It intentionally ignores the ConsumerConfig template, which
// has no meaning without the routing fields above it.
func kafkaConfigSet(k KafkaConfig) bool {
	return len(k.Brokers) > 0 ||
		strings.TrimSpace(k.TaskTopic) != "" ||
		strings.TrimSpace(k.TaskRetryTopic) != "" ||
		strings.TrimSpace(k.TaskDLQTopic) != "" ||
		strings.TrimSpace(k.ConsumerGroup) != "" ||
		k.WriterConfig != nil
}
