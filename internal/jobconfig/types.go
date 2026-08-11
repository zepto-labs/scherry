// Package jobconfig holds the runtime configuration types a caller supplies
// when registering a job (JobConfig, KafkaConfig, TaskData) plus the small
// interfaces a job implements or receives (JobExecutor, TaskExecutor,
// Publisher). It is split out from the orchestration core so that
// internal/executor, internal/console, and internal/transport can all depend
// on these shapes without depending on internal/scheduler's *Service.
package jobconfig

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/zepto-labs/scherry/internal/domain"
)

// KafkaConfig holds the settings for a registered job's Kafka integration.
//
// The first five fields are library-managed routing concerns and are required
// (or conditionally required). The remaining two fields are pass-through
// kafka-go configuration that give the caller full control over every
// producer and consumer knob without the library re-inventing them:
//
//   - ConsumerConfig  — a kafkago.ReaderConfig value; Brokers, GroupID, and
//     Topic are always overridden by the library. Everything else (TLS via
//     Dialer, SASL, MinBytes, MaxWait, SessionTimeout, StartOffset, …) is
//     passed straight to kafka-go, which applies its own defaults for any
//     zero-value field.
//
//   - WriterConfig    — an optional *kafkago.Writer; if non-nil it is used as
//     the producer as-is (the library only sets Addr from Brokers when Addr
//     is nil). If nil, a writer is created from Brokers with a Hash balancer
//     so that DistributionKey-based partition routing works out of the box.
//
// TLS and SASL are configured through the native kafka-go fields:
//
//	ConsumerConfig.Dialer = &kafkago.Dialer{TLS: tlsCfg, SASLMechanism: saslMech}
//	WriterConfig = &kafkago.Writer{Transport: &kafkago.Transport{TLS: tlsCfg, SASL: saslMech}}

type KafkaConfig struct {
	// Brokers is the list of Kafka broker addresses ("host:port").
	Brokers []string
	// TaskTopic is the Kafka topic tasks are published to and consumed from.
	TaskTopic string
	// TaskRetryTopic is the topic for retryable task failures.
	// Leave empty to disable the retry consumer.
	TaskRetryTopic string
	// TaskDLQTopic is the dead-letter topic for tasks that have exhausted all
	// retries. Leave empty to discard failed tasks silently.
	TaskDLQTopic string
	// ConsumerGroup is the Kafka consumer group ID for the main task consumer.
	// The retry consumer uses "<ConsumerGroup>-retry" automatically.
	ConsumerGroup string

	// ConsumerConfig is the kafka-go ReaderConfig template used for all
	// consumers. Brokers, GroupID, and Topic are always set by the library
	// and cannot be overridden here. All other fields are forwarded verbatim
	// to kafka-go; unset fields receive kafka-go's own defaults.
	ConsumerConfig kafkago.ReaderConfig

	// WriterConfig is an optional pre-configured *kafkago.Writer used as the
	// producer. When non-nil it is used directly; the library only sets Addr
	// from Brokers if Addr is nil. When nil, a writer is created with a Hash
	// balancer (to honour DistributionKey) and kafka-go defaults for
	// everything else.
	WriterConfig *kafkago.Writer
}

// RetryConfig groups the two knobs that together define a job's retry
// policy, so they are configured in one place instead of living at
// different levels of JobConfig.
type RetryConfig struct {
	// MaxRetries is the maximum number of attempts allowed for each task
	// belonging to this job before it is sent to the DLQ topic. Defaults to
	// 3 when left at zero.
	MaxRetries int
	// RetryDelay is the minimum wait before a failed task is re-attempted.
	RetryDelay time.Duration
}

type JobConfig struct {
	Name           string
	JobExecutor    JobExecutor
	TaskExecutor   TaskExecutor
	Kafka          KafkaConfig
	Schedule       string
	AsynqOptions   []asynq.Option
	Retry          RetryConfig
	CancelPrevious bool
	Enabled        func() bool
	Hooks          Hooks
	Publisher      Publisher
	TaskReader     MessageReader
	RetryReader    MessageReader

	// JobCompletionCron is a required cron expression (same format as
	// Schedule) that periodically checks this job's RUNNING instances for
	// completion instead of re-checking after every single task finishes.
	// On each trigger, every RUNNING job of this name whose tasks have all
	// reached a terminal state (or that has no tasks at all) is transitioned
	// to COMPLETED/FAILED/PARTIAL_FAILED and its OnJobFinished hook fires —
	// only the trigger changes, not the outcome. Register/RegisterManual
	// reject a config with this left empty.
	JobCompletionCron string
}

type TaskData struct {
	Name   string
	Params map[string]interface{}
	// DistributionKey is an optional caller-supplied key used as the Kafka
	// message key for this task. When set, all tasks sharing the same key
	// are routed to the same partition, which is useful for ordering or
	// affinity requirements (e.g. grouping by customer ID or account ID).
	// When empty, no message key is set and the producer distributes tasks
	// round-robin across partitions.
	DistributionKey string
}

type JobExecutor interface {
	Execute(ctx context.Context, metadata map[string]interface{}) ([]TaskData, error)
}

type TaskExecutor interface {
	Execute(ctx context.Context, params map[string]interface{}) (map[string]interface{}, error)
}

type Publisher interface {
	PublishTasks(ctx context.Context, tasks []domain.Task, topic string) error
	PublishRetryTasks(ctx context.Context, tasks []domain.Task, topic string, retryDelay time.Duration) error
}

// MessageReader and MessageWriter mirror internal/transport's reader/writer
// interfaces. They are redeclared here (rather than imported) so that
// jobconfig does not depend on transport: JobConfig.TaskReader/RetryReader
// need the shape, not the implementation, and transport's concrete readers
// already satisfy this structurally.
type MessageReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}
