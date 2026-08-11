package scherry

// alias_console.go covers internal/console: the view/filter/pagination types
// used by Service.GetJobTasks and (internally) the HTTP console. The
// HTTP handler itself is exposed via Service.ConsoleHandler/StartConsole
// (see alias_scheduler.go), not re-declared here.

import (
	"github.com/zepto-labs/scherry/internal/console"
)

type (
	JobFilter  = console.JobFilter
	JobSummary = console.JobSummary
	JobDetail  = console.JobDetail
	TaskFilter = console.TaskFilter
	TaskPage   = console.TaskPage
)
