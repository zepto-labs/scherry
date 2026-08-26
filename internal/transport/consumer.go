package transport

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/zepto-labs/scherry/internal/logging"
)

// TaskRunner is the single capability the consumer needs from the
// orchestration core: run one task message to completion. Depending on this
// narrow interface (instead of internal/scheduler's *Service) means
// transport never imports scheduler, so scheduler is free to import
// transport to start consumers without an import cycle.
type TaskRunner interface {
	ExecuteTask(ctx context.Context, message []byte) error
}

type taskConsumer struct {
	reader  MessageReader
	runner  TaskRunner
	isRetry bool
	logger  logging.Logger
}

func newTaskConsumer(reader MessageReader, runner TaskRunner, logger logging.Logger, isRetry bool) *taskConsumer {
	return &taskConsumer{reader: reader, runner: runner, isRetry: isRetry, logger: logger}
}

func (c *taskConsumer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Error("kafka fetch failed", "error", err, "retry", c.isRetry)
			continue
		}

		if err := c.processMessage(ctx, msg); err != nil {
			c.logger.Error("task message processing failed", "error", err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("kafka commit failed", "error", err)
		}
	}
}

func (c *taskConsumer) processMessage(ctx context.Context, msg kafkago.Message) error {
	if c.isRetry {
		return c.processRetryMessage(ctx, msg.Value)
	}
	return c.runner.ExecuteTask(ctx, msg.Value)
}

func (c *taskConsumer) processRetryMessage(ctx context.Context, value []byte) error {
	var retryMsg RetryMessage
	if err := json.Unmarshal(value, &retryMsg); err != nil {
		return err
	}

	wait := time.Until(retryMsg.ProcessAfter)
	if wait > 0 {
		c.logger.Info("waiting before retry", "task_id", retryMsg.Task.ID, "wait", wait)
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			// Shutdown was requested while waiting for the retry delay to
			// elapse. Return without committing so the message is redelivered
			// and retried by another consumer, instead of blocking the
			// shutdown for the full (possibly large) retry delay.
			return ctx.Err()
		case <-timer.C:
		}
	}

	payload, err := json.Marshal(retryMsg.Task)
	if err != nil {
		return err
	}
	return c.runner.ExecuteTask(ctx, payload)
}

type consumerKey struct {
	Group string
	Topic string
}

// ConsumerRegistry starts and tracks the Kafka consumer goroutines for every
// (group, topic) pair a caller registers, and stops them all on Stop.
type ConsumerRegistry struct {
	consumers map[consumerKey]*taskConsumer
	cancel    map[consumerKey]context.CancelFunc
	logger    logging.Logger
}

func NewConsumerRegistry(logger logging.Logger) *ConsumerRegistry {
	return &ConsumerRegistry{
		consumers: make(map[consumerKey]*taskConsumer),
		cancel:    make(map[consumerKey]context.CancelFunc),
		logger:    logger,
	}
}

func (r *ConsumerRegistry) Start(ctx context.Context, reader MessageReader, runner TaskRunner, isRetry bool, group, topic string) error {
	if reader == nil {
		return errors.New("message reader cannot be nil")
	}

	key := consumerKey{Group: group, Topic: topic}
	if _, exists := r.consumers[key]; exists {
		r.logger.Info("reusing existing consumer", "group", group, "topic", topic)
		return nil
	}

	consumerCtx, cancel := context.WithCancel(ctx)
	consumer := newTaskConsumer(reader, runner, r.logger, isRetry)
	r.consumers[key] = consumer
	r.cancel[key] = cancel
	go consumer.run(consumerCtx)
	r.logger.Info("started kafka consumer", "group", group, "topic", topic, "retry", isRetry)
	return nil
}

func (r *ConsumerRegistry) StopAll() {
	for key, cancel := range r.cancel {
		r.logger.Info("stopping consumer", "group", key.Group, "topic", key.Topic)
		cancel()
	}
	r.consumers = make(map[consumerKey]*taskConsumer)
	r.cancel = make(map[consumerKey]context.CancelFunc)
}

// Len returns the number of currently-tracked consumers. Exported for tests.
func (r *ConsumerRegistry) Len() int {
	return len(r.consumers)
}
