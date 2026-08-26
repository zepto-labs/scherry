package transport

import (
	"context"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zepto-labs/scherry/internal/executor"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
	"github.com/zepto-labs/scherry/internal/testutil"
)

// execRunner adapts internal/executor's free functions to the TaskRunner
// interface, so these tests exercise the same execution path production code
// does instead of a bare stub.
type execRunner struct {
	repo repository.Repository
	jobs map[string]jobconfig.JobConfig
}

func (r execRunner) ExecuteTask(ctx context.Context, message []byte) error {
	return executor.ExecuteTask(ctx, r.repo, logging.NopLogger{}, r.jobs, message)
}

type mockMessageReader struct {
	mock.Mock
}

func (m *mockMessageReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	args := m.Called(ctx)
	return args.Get(0).(kafkago.Message), args.Error(1)
}

func (m *mockMessageReader) CommitMessages(ctx context.Context, msgs ...kafkago.Message) error {
	args := m.Called(ctx, msgs)
	return args.Error(0)
}

func (m *mockMessageReader) Close() error { return nil }

func TestTaskConsumerProcessMessage(t *testing.T) {
	t.Run("main consumer executes the task directly", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		taskExec := &testutil.MockTaskExecutor{}
		runner := execRunner{repo: repo, jobs: map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", TaskExecutor: taskExec, Hooks: jobconfig.NopHooks()},
		}}
		consumer := newTaskConsumer(&mockMessageReader{}, runner, logging.NopLogger{}, false)

		task, job := testutil.NewTestTaskPair()
		payload := testutil.MustMarshal(task)

		repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
		repo.On("UpdateTask", mock.Anything, task.ID, mock.Anything).Return(nil)
		taskExec.On("Execute", mock.Anything, mock.Anything).Return(nil, nil)

		err := consumer.processMessage(context.Background(), kafkago.Message{Value: payload})
		assert.NoError(t, err)
	})

	t.Run("retry consumer delegates to processRetryMessage", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		taskExec := &testutil.MockTaskExecutor{}
		runner := execRunner{repo: repo, jobs: map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", TaskExecutor: taskExec, Hooks: jobconfig.NopHooks()},
		}}
		consumer := newTaskConsumer(&mockMessageReader{}, runner, logging.NopLogger{}, true)

		task, job := testutil.NewTestTaskPair()
		retryMsg := RetryMessage{Task: *task, ProcessAfter: time.Now().Add(-time.Minute)}
		payload := testutil.MustMarshal(retryMsg)

		repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
		repo.On("UpdateTask", mock.Anything, task.ID, mock.Anything).Return(nil)
		taskExec.On("Execute", mock.Anything, mock.Anything).Return(nil, nil)

		err := consumer.processMessage(context.Background(), kafkago.Message{Value: payload})
		assert.NoError(t, err)
	})
}

func TestTaskConsumerProcessRetryMessage(t *testing.T) {
	t.Run("invalid json returns error", func(t *testing.T) {
		runner := execRunner{repo: &testutil.MockRepository{}}
		consumer := newTaskConsumer(&mockMessageReader{}, runner, logging.NopLogger{}, true)

		err := consumer.processRetryMessage(context.Background(), []byte("{not-json"))
		assert.Error(t, err)
	})

	t.Run("waits until ProcessAfter before executing", func(t *testing.T) {
		repo := &testutil.MockRepository{}
		taskExec := &testutil.MockTaskExecutor{}
		runner := execRunner{repo: repo, jobs: map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", TaskExecutor: taskExec, Hooks: jobconfig.NopHooks()},
		}}
		consumer := newTaskConsumer(&mockMessageReader{}, runner, logging.NopLogger{}, true)

		task, job := testutil.NewTestTaskPair()
		retryMsg := RetryMessage{Task: *task, ProcessAfter: time.Now().Add(20 * time.Millisecond)}
		payload := testutil.MustMarshal(retryMsg)

		repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
		repo.On("UpdateTask", mock.Anything, task.ID, mock.Anything).Return(nil)
		taskExec.On("Execute", mock.Anything, mock.Anything).Return(nil, nil)

		start := time.Now()
		err := consumer.processRetryMessage(context.Background(), payload)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, time.Since(start), 15*time.Millisecond)
	})

	t.Run("returns promptly when the context is cancelled while waiting", func(t *testing.T) {
		// A far-future ProcessAfter means the wait would block for an hour if
		// it ignored cancellation; the test must return well before that.
		taskExec := &testutil.MockTaskExecutor{}
		runner := execRunner{repo: &testutil.MockRepository{}, jobs: map[string]jobconfig.JobConfig{
			"test-job": {Name: "test-job", TaskExecutor: taskExec, Hooks: jobconfig.NopHooks()},
		}}
		consumer := newTaskConsumer(&mockMessageReader{}, runner, logging.NopLogger{}, true)

		task, _ := testutil.NewTestTaskPair()
		retryMsg := RetryMessage{Task: *task, ProcessAfter: time.Now().Add(time.Hour)}
		payload := testutil.MustMarshal(retryMsg)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		done := make(chan error, 1)
		go func() { done <- consumer.processRetryMessage(ctx, payload) }()

		select {
		case err := <-done:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("processRetryMessage did not return after context cancellation")
		}
		// The task must not have been executed after the wait was cancelled.
		taskExec.AssertNotCalled(t, "Execute", mock.Anything, mock.Anything)
	})
}

func TestConsumerRegistryStartAndStopAll(t *testing.T) {
	registry := NewConsumerRegistry(logging.NopLogger{})
	runner := execRunner{repo: &testutil.MockRepository{}}

	t.Run("rejects nil reader", func(t *testing.T) {
		err := registry.Start(context.Background(), nil, runner, false, "group", "topic")
		assert.ErrorContains(t, err, "message reader cannot be nil")
	})

	t.Run("starts and reuses an existing consumer for the same key", func(t *testing.T) {
		reader := &mockMessageReader{}
		reader.On("FetchMessage", mock.Anything).Return(kafkago.Message{}, context.Canceled).Maybe()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		assert.NoError(t, registry.Start(ctx, reader, runner, false, "group", "topic"))
		assert.Equal(t, 1, registry.Len())

		// Same group/topic key should be a no-op reuse, not a second consumer.
		assert.NoError(t, registry.Start(ctx, reader, runner, false, "group", "topic"))
		assert.Equal(t, 1, registry.Len())
	})

	t.Run("stopAll cancels every consumer and clears the registry", func(t *testing.T) {
		registry.StopAll()
		assert.Equal(t, 0, registry.Len())
	})
}

func TestTaskConsumerRunStopsOnContextCancel(t *testing.T) {
	reader := &mockMessageReader{}
	reader.On("FetchMessage", mock.Anything).Return(kafkago.Message{}, context.Canceled).Maybe()

	runner := execRunner{repo: &testutil.MockRepository{}}
	consumer := newTaskConsumer(reader, runner, logging.NopLogger{}, false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		consumer.run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("taskConsumer.run did not return after context cancellation")
	}
}

func TestTaskConsumerRunProcessesMessagesAndCommits(t *testing.T) {
	repo := &testutil.MockRepository{}
	taskExec := &testutil.MockTaskExecutor{}
	runner := execRunner{repo: repo, jobs: map[string]jobconfig.JobConfig{
		"test-job": {Name: "test-job", TaskExecutor: taskExec, Hooks: jobconfig.NopHooks()},
	}}

	task, job := testutil.NewTestTaskPair()
	payload := testutil.MustMarshal(task)
	msg := kafkago.Message{Value: payload}

	repo.On("FindTaskWithJob", mock.Anything, task.ID).Return(task, job, nil)
	repo.On("UpdateTask", mock.Anything, task.ID, mock.Anything).Return(nil)
	taskExec.On("Execute", mock.Anything, mock.Anything).Return(nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	reader := &mockMessageReader{}
	reader.On("FetchMessage", mock.Anything).Return(msg, nil).Once()
	reader.On("CommitMessages", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		cancel() // stop the loop after the first successful commit
	}).Return(nil).Once()
	reader.On("FetchMessage", mock.Anything).Return(kafkago.Message{}, context.Canceled).Maybe()

	consumer := newTaskConsumer(reader, runner, logging.NopLogger{}, false)

	done := make(chan struct{})
	go func() {
		consumer.run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("taskConsumer.run did not return after processing the message")
	}
	reader.AssertExpectations(t)
}
