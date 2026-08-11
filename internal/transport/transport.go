// Package transport contains the Kafka-facing plumbing: MessageReader and
// MessageWriter wrappers around kafka-go, the task publisher, and the
// consumer goroutines that feed messages into task execution.
//
// It depends on internal/jobconfig (for KafkaConfig) and internal/domain
// (for Task), but never on internal/scheduler's *Service: the consumer takes
// a narrow TaskRunner interface instead, so callers (internal/scheduler) can
// pass any type with an ExecuteTask method without transport importing them.
package transport

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/zepto-labs/scherry/internal/jobconfig"
)

type MessageReader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

type kafkaReader struct {
	reader *kafkago.Reader
}

// NewKafkaReader constructs a MessageReader from cfg.ConsumerConfig, setting
// Brokers, GroupID, and Topic from the managed fields. All other ReaderConfig
// fields are forwarded to kafka-go as-is; unset fields receive kafka-go's own
// defaults. Use this to inject a custom reader into JobConfig.TaskReader or
// JobConfig.RetryReader.
func NewKafkaReader(cfg jobconfig.KafkaConfig, groupID, topic string) MessageReader {
	rc := cfg.ConsumerConfig // copy the template; kafka-go applies its own defaults
	rc.Brokers = cfg.Brokers
	rc.GroupID = groupID
	rc.Topic = topic
	return &kafkaReader{reader: kafkago.NewReader(rc)}
}

func (r *kafkaReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	return r.reader.FetchMessage(ctx)
}

func (r *kafkaReader) CommitMessages(ctx context.Context, msgs ...kafkago.Message) error {
	return r.reader.CommitMessages(ctx, msgs...)
}

func (r *kafkaReader) Close() error {
	return r.reader.Close()
}

type kafkaWriter struct {
	writer *kafkago.Writer
}

// NewKafkaWriter constructs a MessageWriter from cfg.
//
//   - If cfg.WriterConfig is non-nil it is used directly; Addr is set from
//     cfg.Brokers when it is nil.
//   - Otherwise a new writer is created with a Hash balancer (so that
//     DistributionKey-based partition routing works) and kafka-go defaults
//     for every other field.
//
// Use this to inject a custom writer into a Publisher via NewTaskPublisher.
func NewKafkaWriter(cfg jobconfig.KafkaConfig) MessageWriter {
	if cfg.WriterConfig != nil {
		if cfg.WriterConfig.Addr == nil {
			cfg.WriterConfig.Addr = kafkago.TCP(cfg.Brokers...)
		}
		return &kafkaWriter{writer: cfg.WriterConfig}
	}
	return &kafkaWriter{
		writer: &kafkago.Writer{
			Addr:     kafkago.TCP(cfg.Brokers...),
			Balancer: &kafkago.Hash{},
		},
	}
}

func (w *kafkaWriter) WriteMessages(ctx context.Context, msgs ...kafkago.Message) error {
	return w.writer.WriteMessages(ctx, msgs...)
}

func (w *kafkaWriter) Close() error {
	return w.writer.Close()
}
