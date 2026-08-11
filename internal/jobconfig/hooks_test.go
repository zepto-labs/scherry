package jobconfig

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNopHooksDoNotPanic(t *testing.T) {
	h := NopHooks()
	h.OnTasksPublished(context.Background(), "job", 1)
	h.OnTaskStarted(context.Background(), "job", "task", 1)
	h.OnTaskFinished(context.Background(), "job", "task", "status", time.Second)
	h.OnJobFinished(context.Background(), "job", "ref", "status", time.Second)
}

func TestMergeHooks(t *testing.T) {
	t.Run("all nil falls back to nop hooks", func(t *testing.T) {
		merged := MergeHooks(Hooks{})
		assert.NotNil(t, merged.OnTasksPublished)
		assert.NotNil(t, merged.OnTaskStarted)
		assert.NotNil(t, merged.OnTaskFinished)
		assert.NotNil(t, merged.OnJobFinished)
		// Should not panic when invoked.
		merged.OnJobFinished(context.Background(), "job", "ref", "status", 0)
	})

	t.Run("only overridden hooks are replaced", func(t *testing.T) {
		var called bool
		merged := MergeHooks(Hooks{
			OnTaskStarted: func(context.Context, string, string, int) { called = true },
		})

		merged.OnTaskStarted(context.Background(), "job", "task", 1)
		assert.True(t, called)

		// The other three hooks should still be safe no-ops.
		merged.OnTasksPublished(context.Background(), "job", 1)
		merged.OnTaskFinished(context.Background(), "job", "task", "status", 0)
		merged.OnJobFinished(context.Background(), "job", "ref", "status", 0)
	})

	t.Run("all hooks overridden", func(t *testing.T) {
		calls := map[string]bool{}
		merged := MergeHooks(Hooks{
			OnTasksPublished: func(context.Context, string, int) { calls["published"] = true },
			OnTaskStarted:    func(context.Context, string, string, int) { calls["started"] = true },
			OnTaskFinished:   func(context.Context, string, string, string, time.Duration) { calls["finished"] = true },
			OnJobFinished:    func(context.Context, string, string, string, time.Duration) { calls["job_finished"] = true },
		})

		merged.OnTasksPublished(context.Background(), "job", 1)
		merged.OnTaskStarted(context.Background(), "job", "task", 1)
		merged.OnTaskFinished(context.Background(), "job", "task", "status", 0)
		merged.OnJobFinished(context.Background(), "job", "ref", "status", 0)

		assert.True(t, calls["published"])
		assert.True(t, calls["started"])
		assert.True(t, calls["finished"])
		assert.True(t, calls["job_finished"])
	})
}
