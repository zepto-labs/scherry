package transport

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/testutil"
)

type mockWriter struct {
	mock.Mock
}

func (m *mockWriter) WriteMessages(ctx context.Context, msgs ...kafkago.Message) error {
	args := m.Called(ctx, msgs)
	return args.Error(0)
}

func (m *mockWriter) Close() error { return nil }

func strPtr(s string) *string { return &s }

func TestPartitionKey(t *testing.T) {
	t.Run("returns nil when DistributionKey is nil", func(t *testing.T) {
		task := domain.Task{ID: uuid.MustParse("11111111-1111-1111-1111-111111111111")}
		assert.Nil(t, partitionKey(task))
	})

	t.Run("returns nil when DistributionKey is empty string", func(t *testing.T) {
		task := domain.Task{ID: uuid.MustParse("22222222-2222-2222-2222-222222222222"), DistributionKey: strPtr("")}
		assert.Nil(t, partitionKey(task))
	})

	t.Run("uses DistributionKey when set", func(t *testing.T) {
		task := domain.Task{ID: uuid.New(), DistributionKey: strPtr("customer-42")}
		assert.Equal(t, []byte("customer-42"), partitionKey(task))
	})
}

func newPublisherTask(t *testing.T, distKey *string) domain.Task {
	t.Helper()
	task, _ := testutil.NewTestTaskPair()
	task.DistributionKey = distKey
	return *task
}

func TestPublishTasks_PartitionKey(t *testing.T) {
	t.Run("publishes with nil key when no DistributionKey", func(t *testing.T) {
		w := &mockWriter{}
		pub := NewTaskPublisher(w, logging.NopLogger{})
		task := newPublisherTask(t, nil)

		w.On("WriteMessages", mock.Anything, mock.MatchedBy(func(msgs []kafkago.Message) bool {
			return len(msgs) == 1 && msgs[0].Key == nil
		})).Return(nil)

		require.NoError(t, pub.PublishTasks(context.Background(), []domain.Task{task}, "topic"))
		w.AssertExpectations(t)
	})

	t.Run("publishes with DistributionKey when set", func(t *testing.T) {
		w := &mockWriter{}
		pub := NewTaskPublisher(w, logging.NopLogger{})
		task := newPublisherTask(t, strPtr("account-99"))

		w.On("WriteMessages", mock.Anything, mock.MatchedBy(func(msgs []kafkago.Message) bool {
			return len(msgs) == 1 && string(msgs[0].Key) == "account-99"
		})).Return(nil)

		require.NoError(t, pub.PublishTasks(context.Background(), []domain.Task{task}, "topic"))
		w.AssertExpectations(t)
	})
}

func TestPublishRetryTasks_PartitionKey(t *testing.T) {
	t.Run("retry uses DistributionKey in both message key and OriginalKey field", func(t *testing.T) {
		w := &mockWriter{}
		pub := NewTaskPublisher(w, logging.NopLogger{})
		task := newPublisherTask(t, strPtr("order-7"))

		w.On("WriteMessages", mock.Anything, mock.MatchedBy(func(msgs []kafkago.Message) bool {
			if len(msgs) != 1 {
				return false
			}
			if string(msgs[0].Key) != "order-7" {
				return false
			}
			var rm RetryMessage
			if err := json.Unmarshal(msgs[0].Value, &rm); err != nil {
				return false
			}
			return rm.OriginalKey == "order-7"
		})).Return(nil)

		require.NoError(t, pub.PublishRetryTasks(context.Background(), []domain.Task{task}, "retry", time.Minute))
		w.AssertExpectations(t)
	})
}

func TestPublishTasksErrors(t *testing.T) {
	t.Run("write failure propagates", func(t *testing.T) {
		w := &mockWriter{}
		pub := NewTaskPublisher(w, logging.NopLogger{})
		task := newPublisherTask(t, nil)

		w.On("WriteMessages", mock.Anything, mock.Anything).Return(errors.New("kafka down"))
		err := pub.PublishTasks(context.Background(), []domain.Task{task}, "topic")
		assert.Error(t, err)
	})

	t.Run("empty task list is a no-op", func(t *testing.T) {
		pub := NewTaskPublisher(&mockWriter{}, logging.NopLogger{})
		assert.NoError(t, pub.PublishTasks(context.Background(), nil, "topic"))
	})
}

func TestPublishRetryTasksErrors(t *testing.T) {
	w := &mockWriter{}
	pub := NewTaskPublisher(w, logging.NopLogger{})
	task := newPublisherTask(t, nil)

	w.On("WriteMessages", mock.Anything, mock.Anything).Return(errors.New("kafka down"))
	err := pub.PublishRetryTasks(context.Background(), []domain.Task{task}, "retry", time.Minute)
	assert.Error(t, err)
}
