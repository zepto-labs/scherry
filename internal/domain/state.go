package domain

import (
	"context"

	"github.com/looplab/fsm"
)

var (
	jobTransitions = fsm.Events{
		{Name: JobActionStart, Src: []string{JobStatusPending}, Dst: JobStatusRunning},
		{Name: JobActionReject, Src: []string{JobStatusPending}, Dst: JobStatusRejected},
		{Name: JobActionCancel, Src: []string{JobStatusPending}, Dst: JobStatusCancelled},
		{Name: JobActionFail, Src: []string{JobStatusPending}, Dst: JobStatusFailed},
		{Name: JobActionComplete, Src: []string{JobStatusRunning}, Dst: JobStatusCompleted},
		{Name: JobActionPartialFail, Src: []string{JobStatusRunning}, Dst: JobStatusPartialFailed},
		{Name: JobActionFail, Src: []string{JobStatusRunning}, Dst: JobStatusFailed},
		{Name: JobActionCancel, Src: []string{JobStatusRunning}, Dst: JobStatusCancelled},
	}

	taskTransitions = fsm.Events{
		{Name: TaskActionStart, Src: []string{TaskStatusPending, TaskStatusRunning}, Dst: TaskStatusRunning},
		{Name: TaskActionReject, Src: []string{TaskStatusPending}, Dst: TaskStatusRejected},
		{Name: TaskActionCancel, Src: []string{TaskStatusPending}, Dst: TaskStatusCancelled},
		{Name: TaskActionComplete, Src: []string{TaskStatusRunning}, Dst: TaskStatusCompleted},
		{Name: TaskActionFail, Src: []string{TaskStatusRunning}, Dst: TaskStatusFailed},
		{Name: TaskActionRetry, Src: []string{TaskStatusFailed}, Dst: TaskStatusPending},
		{Name: TaskActionExhaustRetries, Src: []string{TaskStatusFailed}, Dst: TaskStatusMaxRetriesExhausted},
		{Name: TaskActionCancel, Src: []string{TaskStatusPending, TaskStatusRunning}, Dst: TaskStatusCancelled},
	}
)

func newJobStateMachine(job *Job) *fsm.FSM {
	return fsm.NewFSM(job.Status, jobTransitions, fsm.Callbacks{
		"enter_state": func(_ context.Context, e *fsm.Event) {
			job.UpdateStatus(e.Dst)
		},
	})
}

func newTaskStateMachine(task *Task) *fsm.FSM {
	return fsm.NewFSM(task.Status, taskTransitions, fsm.Callbacks{
		"enter_state": func(_ context.Context, e *fsm.Event) {
			task.UpdateStatus(e.Dst)
		},
	})
}
