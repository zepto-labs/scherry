package domain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTaskLifecycle(t *testing.T) {
	t.Run("start from pending", func(t *testing.T) {
		task := &Task{Status: TaskStatusPending}
		assert.NoError(t, task.Start(context.Background()))
		assert.Equal(t, TaskStatusRunning, task.Status)
	})

	t.Run("complete from running", func(t *testing.T) {
		task := &Task{Status: TaskStatusRunning}
		assert.NoError(t, task.Complete(context.Background()))
		assert.Equal(t, TaskStatusCompleted, task.Status)
	})

	t.Run("retry from failed", func(t *testing.T) {
		task := &Task{Status: TaskStatusFailed}
		assert.NoError(t, task.Retry(context.Background()))
		assert.Equal(t, TaskStatusPending, task.Status)
	})

	t.Run("exhaust retries from failed", func(t *testing.T) {
		task := &Task{Status: TaskStatusFailed}
		assert.NoError(t, task.ExhaustRetries(context.Background()))
		assert.Equal(t, TaskStatusMaxRetriesExhausted, task.Status)
	})

	t.Run("invalid transition is rejected", func(t *testing.T) {
		task := &Task{Status: TaskStatusCompleted}
		assert.Error(t, task.Retry(context.Background()))
		assert.Equal(t, TaskStatusCompleted, task.Status)
	})
}

func TestTaskUpdateStatus(t *testing.T) {
	task := &Task{Status: TaskStatusPending}
	task.UpdateStatus(TaskStatusRunning)
	assert.Equal(t, TaskStatusRunning, task.Status)
	assert.False(t, task.UpdatedAt.IsZero())
}
