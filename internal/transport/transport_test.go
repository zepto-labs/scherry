package transport

import (
	"context"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"

	"github.com/zepto-labs/scherry/internal/jobconfig"
)

func TestNewKafkaReader(t *testing.T) {
	cfg := jobconfig.KafkaConfig{Brokers: []string{"localhost:9092"}}
	reader := NewKafkaReader(cfg, "group-1", "topic-1")

	assert.NotNil(t, reader)
	// kafka-go's Reader connects lazily; closing an unused reader must not
	// error or block.
	assert.NoError(t, reader.Close())
}

func TestNewKafkaWriterDefaultsToHashBalancer(t *testing.T) {
	cfg := jobconfig.KafkaConfig{Brokers: []string{"localhost:9092"}}
	writer := NewKafkaWriter(cfg)

	assert.NotNil(t, writer)
	assert.NoError(t, writer.Close())
}

func TestNewKafkaWriterUsesProvidedWriterConfig(t *testing.T) {
	t.Run("sets Addr from brokers when unset", func(t *testing.T) {
		cfg := jobconfig.KafkaConfig{
			Brokers:      []string{"localhost:9092"},
			WriterConfig: &kafkago.Writer{},
		}
		writer := NewKafkaWriter(cfg)
		assert.NotNil(t, writer)
		assert.NoError(t, writer.Close())
	})

	t.Run("preserves an already-set Addr", func(t *testing.T) {
		explicit := kafkago.TCP("custom:9092")
		cfg := jobconfig.KafkaConfig{
			Brokers:      []string{"localhost:9092"},
			WriterConfig: &kafkago.Writer{Addr: explicit},
		}
		writer := NewKafkaWriter(cfg)
		assert.NotNil(t, writer)
		assert.NoError(t, writer.Close())
	})
}

func TestKafkaReaderMethods(t *testing.T) {
	cfg := jobconfig.KafkaConfig{Brokers: []string{"localhost:9092"}}
	reader := NewKafkaReader(cfg, "group", "topic")
	t.Cleanup(func() { _ = reader.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := reader.FetchMessage(ctx)
	assert.Error(t, err)

	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	assert.Error(t, reader.CommitMessages(ctx2))
}

func TestKafkaWriterWriteMessages(t *testing.T) {
	cfg := jobconfig.KafkaConfig{Brokers: []string{"localhost:9092"}}
	writer := NewKafkaWriter(cfg)
	t.Cleanup(func() { _ = writer.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := writer.WriteMessages(ctx, kafkago.Message{Topic: "topic", Value: []byte("x")})
	assert.Error(t, err)
}
