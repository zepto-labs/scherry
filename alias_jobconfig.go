package scherry

// alias_jobconfig.go covers internal/jobconfig: the runtime configuration a
// caller supplies when registering a job (JobConfig, KafkaConfig, TaskData,
// Hooks) plus the small interfaces a job implements or receives (JobExecutor,
// TaskExecutor, Publisher).

import (
	"github.com/zepto-labs/scherry/internal/jobconfig"
)

type (
	JobConfig         = jobconfig.JobConfig
	KafkaConfig       = jobconfig.KafkaConfig
	SharedKafkaConfig = jobconfig.SharedKafkaConfig
	RetryConfig       = jobconfig.RetryConfig
	TaskData          = jobconfig.TaskData
	JobExecutor       = jobconfig.JobExecutor
	TaskExecutor      = jobconfig.TaskExecutor
	Publisher         = jobconfig.Publisher
	Hooks             = jobconfig.Hooks
)

var NopHooks = jobconfig.NopHooks
