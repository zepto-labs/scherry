package scherry

// This file, and the other alias_*.go files in this package, re-export types,
// constants, and constructors that live in internal/ packages so every
// caller — inside this module and outside it — can keep referring to them by
// their original, unqualified names (Job, Task, Service, Config, ...). The
// implementations were grouped into internal/ packages by concern
// (domain model, persistence, logging, job config, Kafka transport, the
// HTTP console, and orchestration); these alias files are the seam that
// keeps that split invisible to callers.
//
// alias_domain.go covers internal/domain: the persisted Job/Task types, their
// status/action constants, and the terminal-state helper slices.

import (
	"github.com/zepto-labs/scherry/internal/domain"
)

type (
	Job  = domain.Job
	Task = domain.Task
)

const (
	JobStatusPending       = domain.JobStatusPending
	JobStatusRunning       = domain.JobStatusRunning
	JobStatusCompleted     = domain.JobStatusCompleted
	JobStatusFailed        = domain.JobStatusFailed
	JobStatusPartialFailed = domain.JobStatusPartialFailed
	JobStatusRejected      = domain.JobStatusRejected
	JobStatusCancelled     = domain.JobStatusCancelled

	TaskStatusPending             = domain.TaskStatusPending
	TaskStatusRunning             = domain.TaskStatusRunning
	TaskStatusCompleted           = domain.TaskStatusCompleted
	TaskStatusFailed              = domain.TaskStatusFailed
	TaskStatusRejected            = domain.TaskStatusRejected
	TaskStatusCancelled           = domain.TaskStatusCancelled
	TaskStatusMaxRetriesExhausted = domain.TaskStatusMaxRetriesExhausted

	JobActionStart       = domain.JobActionStart
	JobActionComplete    = domain.JobActionComplete
	JobActionFail        = domain.JobActionFail
	JobActionPartialFail = domain.JobActionPartialFail
	JobActionReject      = domain.JobActionReject
	JobActionCancel      = domain.JobActionCancel

	TaskActionStart          = domain.TaskActionStart
	TaskActionComplete       = domain.TaskActionComplete
	TaskActionFail           = domain.TaskActionFail
	TaskActionReject         = domain.TaskActionReject
	TaskActionCancel         = domain.TaskActionCancel
	TaskActionRetry          = domain.TaskActionRetry
	TaskActionExhaustRetries = domain.TaskActionExhaustRetries
)

var (
	NonTerminalJobStates = domain.NonTerminalJobStates
	TerminalJobStates    = domain.TerminalJobStates
)
