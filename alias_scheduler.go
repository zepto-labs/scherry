package scherry

// alias_scheduler.go covers internal/scheduler: Service (the library's main
// entry point) and the bootstrap Config types used to construct one.

import (
	"github.com/zepto-labs/scherry/internal/executor"
	"github.com/zepto-labs/scherry/internal/scheduler"
)

type (
	Service        = scheduler.Service
	Config         = scheduler.Config
	RedisConfig    = scheduler.RedisConfig
	DatabaseConfig = scheduler.DatabaseConfig
)

var New = scheduler.New

// RetryJobPayload is the Asynq task payload used by RegisterRetryHandler.
type RetryJobPayload = executor.RetryJobPayload
