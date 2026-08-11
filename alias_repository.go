package scherry

// alias_repository.go covers internal/repository: the Repository interface,
// its GORM-backed implementation, and the errors/aggregate types it returns.

import (
	"github.com/zepto-labs/scherry/internal/repository"
)

type (
	Repository        = repository.Repository
	TaskStatusSummary = repository.TaskStatusSummary
)

var (
	NewRepository         = repository.NewRepository
	ErrJobAlreadyTerminal = repository.ErrJobAlreadyTerminal
)
