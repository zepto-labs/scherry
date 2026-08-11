// Package executor implements job and task execution, job finalization, and
// job retry — the core orchestration logic that used to live directly on
// *Service. It depends on internal/domain, internal/repository,
// internal/jobconfig, and internal/logging, but not on internal/scheduler:
// every entry point takes the Repository, Logger, and registered-jobs map it
// needs as explicit parameters (bundled internally into *deps) instead of a
// *Service, so internal/scheduler can import executor without a cycle.
package executor

import (
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
)

// deps bundles the collaborators every execution helper needs. It exists so
// the helpers below can stay methods (mirroring their pre-split form as
// *Service methods) instead of every function threading repo/logger/jobs
// through its signature individually.
type deps struct {
	repo   repository.Repository
	logger logging.Logger
	jobs   map[string]jobconfig.JobConfig
}
