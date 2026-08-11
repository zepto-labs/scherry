package transport

import (
	"context"
	"encoding/json"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
)

type RetryMessage struct {
	Task         domain.Task `json:"task"`
	ProcessAfter time.Time   `json:"process_after"`
	OriginalKey  string      `json:"original_key"`
}

// partitionKey returns the Kafka message key for a task.
// It uses DistributionKey when set; otherwise it returns nil so the
// producer's Hash balancer falls back to round-robin partition selection.
func partitionKey(task domain.Task) []byte {
	if task.DistributionKey != nil && *task.DistributionKey != "" {
		return []byte(*task.DistributionKey)
	}
	return nil
}

type taskPublisher struct {
	writer MessageWriter
	logger logging.Logger
}

func NewTaskPublisher(writer MessageWriter, logger logging.Logger) jobconfig.Publisher {
	return &taskPublisher{writer: writer, logger: logger}
}

func (p *taskPublisher) PublishTasks(ctx context.Context, tasks []domain.Task, topic string) error {
	messages := make([]kafkago.Message, 0, len(tasks))
	for _, task := range tasks {
		payload, err := json.Marshal(&task)
		if err != nil {
			p.logger.Error("failed to marshal task", "task_id", task.ID, "error", err)
			continue
		}
		messages = append(messages, kafkago.Message{
			Topic: topic,
			Key:   partitionKey(task),
			Value: payload,
		})
	}
	if len(messages) == 0 {
		return nil
	}
	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		p.logger.Error("failed to publish tasks", "error", err)
		return err
	}
	p.logger.Info("published tasks", "count", len(messages), "topic", topic)
	return nil
}

func (p *taskPublisher) PublishRetryTasks(ctx context.Context, tasks []domain.Task, topic string, retryDelay time.Duration) error {
	messages := make([]kafkago.Message, 0, len(tasks))
	for _, task := range tasks {
		key := partitionKey(task)
		retryMsg := RetryMessage{
			Task:         task,
			ProcessAfter: time.Now().Add(retryDelay),
			OriginalKey:  string(key),
		}
		payload, err := json.Marshal(&retryMsg)
		if err != nil {
			p.logger.Error("failed to marshal retry task", "task_id", task.ID, "error", err)
			continue
		}
		messages = append(messages, kafkago.Message{
			Topic: topic,
			Key:   key,
			Value: payload,
		})
	}
	if len(messages) == 0 {
		return nil
	}
	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		p.logger.Error("failed to publish retry tasks", "error", err)
		return err
	}
	p.logger.Info("published retry tasks", "count", len(messages), "topic", topic)
	return nil
}
