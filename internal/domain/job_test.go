package domain

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJobCancel(t *testing.T) {
	t.Run("from pending", func(t *testing.T) {
		j := &Job{Status: JobStatusPending}
		assert.NoError(t, j.Cancel(context.Background()))
		assert.Equal(t, JobStatusCancelled, j.Status)
		assert.True(t, j.IsCancelled())
	})

	t.Run("from running", func(t *testing.T) {
		j := &Job{Status: JobStatusRunning}
		assert.NoError(t, j.Cancel(context.Background()))
		assert.Equal(t, JobStatusCancelled, j.Status)
	})

	t.Run("from terminal state is rejected", func(t *testing.T) {
		j := &Job{Status: JobStatusCompleted}
		assert.Error(t, j.Cancel(context.Background()))
		assert.Equal(t, JobStatusCompleted, j.Status)
	})
}

func TestJobIsRejected(t *testing.T) {
	assert.True(t, (&Job{Status: JobStatusRejected}).IsRejected())
	assert.False(t, (&Job{Status: JobStatusPending}).IsRejected())
}

func TestJobIsCancelled(t *testing.T) {
	assert.True(t, (&Job{Status: JobStatusCancelled}).IsCancelled())
	assert.False(t, (&Job{Status: JobStatusRunning}).IsCancelled())
}

func TestJobTableName(t *testing.T) {
	assert.Equal(t, "scherry_jobs", (&Job{}).TableName())
}

func TestTaskTableName(t *testing.T) {
	assert.Equal(t, "scherry_tasks", (&Task{}).TableName())
}

func TestTaskGetParamsSetParamsSetResult(t *testing.T) {
	task := &Task{}

	t.Run("GetParams on unset params does not error", func(t *testing.T) {
		params, err := task.GetParams()
		assert.NoError(t, err)
		assert.Empty(t, params)
	})

	t.Run("SetParams then GetParams round-trips", func(t *testing.T) {
		assert.NoError(t, task.SetParams(map[string]interface{}{"a": float64(1)}))
		params, err := task.GetParams()
		assert.NoError(t, err)
		assert.Equal(t, map[string]interface{}{"a": float64(1)}, params)
	})

	t.Run("SetResult", func(t *testing.T) {
		assert.NoError(t, task.SetResult(map[string]interface{}{"ok": true}))
	})
}

func TestJobSetMetadata(t *testing.T) {
	j := &Job{}
	assert.NoError(t, j.SetMetadata(map[string]interface{}{"key": "value"}))

	meta, err := j.MetadataMap()
	assert.NoError(t, err)
	assert.Equal(t, "value", meta["key"])
}
