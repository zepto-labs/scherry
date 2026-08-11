package jobconfig

import (
	"context"
	"time"
)

type Hooks struct {
	OnTasksPublished func(ctx context.Context, jobName string, count int)
	OnTaskStarted    func(ctx context.Context, jobName, taskID string, attempt int)
	OnTaskFinished   func(ctx context.Context, jobName, taskID, status string, duration time.Duration)
	OnJobFinished    func(ctx context.Context, jobName, refID, status string, duration time.Duration)
}

func NopHooks() Hooks {
	return Hooks{
		OnTasksPublished: func(context.Context, string, int) {},
		OnTaskStarted:    func(context.Context, string, string, int) {},
		OnTaskFinished:   func(context.Context, string, string, string, time.Duration) {},
		OnJobFinished:    func(context.Context, string, string, string, time.Duration) {},
	}
}

// MergeHooks fills in any nil field of h with a no-op, so callers can always
// invoke every hook without a nil check.
func MergeHooks(h Hooks) Hooks {
	nop := NopHooks()
	if h.OnTasksPublished != nil {
		nop.OnTasksPublished = h.OnTasksPublished
	}
	if h.OnTaskStarted != nil {
		nop.OnTaskStarted = h.OnTaskStarted
	}
	if h.OnTaskFinished != nil {
		nop.OnTaskFinished = h.OnTaskFinished
	}
	if h.OnJobFinished != nil {
		nop.OnJobFinished = h.OnJobFinished
	}
	return nop
}
