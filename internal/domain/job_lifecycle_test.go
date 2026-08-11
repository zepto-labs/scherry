package domain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJobLifecycle(t *testing.T) {
	t.Run("start from pending", func(t *testing.T) {
		j := &Job{Status: JobStatusPending}
		assert.NoError(t, j.Start(context.Background()))
		assert.Equal(t, JobStatusRunning, j.Status)
	})

	t.Run("complete from running", func(t *testing.T) {
		j := &Job{Status: JobStatusRunning}
		assert.NoError(t, j.Complete(context.Background()))
		assert.Equal(t, JobStatusCompleted, j.Status)
	})

	t.Run("fail from running", func(t *testing.T) {
		j := &Job{Status: JobStatusRunning}
		assert.NoError(t, j.Fail(context.Background()))
		assert.Equal(t, JobStatusFailed, j.Status)
	})

	t.Run("partial fail from running", func(t *testing.T) {
		j := &Job{Status: JobStatusRunning}
		assert.NoError(t, j.PartialFail(context.Background()))
		assert.Equal(t, JobStatusPartialFailed, j.Status)
	})

	t.Run("fail from pending", func(t *testing.T) {
		j := &Job{Status: JobStatusPending}
		assert.NoError(t, j.Fail(context.Background()))
		assert.Equal(t, JobStatusFailed, j.Status)
	})
}

func TestJobStatusPredicates(t *testing.T) {
	t.Run("IsExecutionPending", func(t *testing.T) {
		assert.True(t, (&Job{Status: JobStatusPending}).IsExecutionPending())
		assert.True(t, (&Job{Status: JobStatusRunning}).IsExecutionPending())
		assert.False(t, (&Job{Status: JobStatusCompleted}).IsExecutionPending())
	})

	t.Run("IsPending and IsRunning", func(t *testing.T) {
		assert.True(t, (&Job{Status: JobStatusPending}).IsPending())
		assert.True(t, (&Job{Status: JobStatusRunning}).IsRunning())
		assert.False(t, (&Job{Status: JobStatusCompleted}).IsPending())
		assert.False(t, (&Job{Status: JobStatusPending}).IsRunning())
	})
}
