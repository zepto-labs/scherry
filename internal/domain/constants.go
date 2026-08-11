package domain

const (
	JobStatusPending       = "PENDING"
	JobStatusRunning       = "RUNNING"
	JobStatusCompleted     = "COMPLETED"
	JobStatusFailed        = "FAILED"
	JobStatusPartialFailed = "PARTIAL_FAILED"
	JobStatusRejected      = "REJECTED"
	JobStatusCancelled     = "CANCELLED"
)

const (
	TaskStatusPending             = "PENDING"
	TaskStatusRunning             = "RUNNING"
	TaskStatusCompleted           = "COMPLETED"
	TaskStatusFailed              = "FAILED"
	TaskStatusRejected            = "REJECTED"
	TaskStatusCancelled           = "CANCELLED"
	TaskStatusMaxRetriesExhausted = "MAX_RETRIES_EXHAUSTED"
)

const (
	JobActionStart       = "start"
	JobActionComplete    = "complete"
	JobActionFail        = "fail"
	JobActionPartialFail = "partial_fail"
	JobActionReject      = "reject"
	JobActionCancel      = "cancel"
)

const (
	TaskActionStart          = "start"
	TaskActionComplete       = "complete"
	TaskActionFail           = "fail"
	TaskActionReject         = "reject"
	TaskActionCancel         = "cancel"
	TaskActionRetry          = "retry"
	TaskActionExhaustRetries = "exhaust_retries"
)

var NonTerminalJobStates = []string{JobStatusRunning, JobStatusPending}

var TerminalJobStates = []string{
	JobStatusCompleted,
	JobStatusFailed,
	JobStatusPartialFailed,
	JobStatusRejected,
	JobStatusCancelled,
}
