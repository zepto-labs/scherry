package executor

import (
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
)

// newDeps builds a *deps for tests, mirroring the pre-split testService
// helper: every job's Hooks are merged with the no-op defaults (as
// Service.setupJobInfra would do at registration time) so tests that don't
// set every hook field don't panic on a nil func, and a nop logger is used
// plus whatever repo/jobs the test supplies (jobs may be nil; reads from a
// nil map are safe zero-value lookups in Go).
func newDeps(repo repository.Repository, jobs map[string]jobconfig.JobConfig) *deps {
	for name, cfg := range jobs {
		cfg.Hooks = jobconfig.MergeHooks(cfg.Hooks)
		jobs[name] = cfg
	}
	return &deps{repo: repo, logger: logging.NopLogger{}, jobs: jobs}
}
