package console

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/zepto-labs/scherry/internal/jobconfig"
)

const (
	defaultUpcomingCount = 5
	maxUpcomingCount     = 20
)

// jobConfigView is the scheduler-agnostic configuration of a registered job,
// derived from jobconfig.JobConfig (interface fields such as executors are omitted).
type jobConfigView struct {
	Name           string   `json:"name"`
	Schedule       string   `json:"schedule"`
	Manual         bool     `json:"manual"`
	Enabled        bool     `json:"enabled"`
	MaxRetries     int      `json:"max_retries"`
	CancelPrevious bool     `json:"cancel_previous"`
	Brokers        []string `json:"brokers"`
	TaskTopic      string   `json:"task_topic"`
	TaskRetryTopic string   `json:"task_retry_topic"`
	TaskDLQTopic   string   `json:"task_dlq_topic"`
	ConsumerGroup  string   `json:"consumer_group"`
	RetryDelayMs   int64    `json:"retry_delay_ms"`
	ParseError     string   `json:"parse_error,omitempty"`
}

// upcomingRunView is a single future fire time for a job.
type upcomingRunView struct {
	JobName  string    `json:"job_name"`
	Schedule string    `json:"schedule"`
	RunAt    time.Time `json:"run_at"`
}

// handleConfig lists every registered job with its configuration.
func (api *consoleAPI) handleConfig(w http.ResponseWriter, r *http.Request) {
	jobs := api.svc.GetRegisteredJobs()

	views := make([]jobConfigView, 0, len(jobs))
	for _, jc := range jobs {
		isManual := strings.TrimSpace(jc.Schedule) == ""
		k := jc.Kafka
		v := jobConfigView{
			Name:           jc.Name,
			Schedule:       jc.Schedule,
			Manual:         isManual,
			Enabled:        jobconfig.IsEnabled(jc),
			MaxRetries:     jc.Retry.MaxRetries,
			CancelPrevious: jc.CancelPrevious,
			Brokers:        k.Brokers,
			TaskTopic:      k.TaskTopic,
			TaskRetryTopic: k.TaskRetryTopic,
			TaskDLQTopic:   k.TaskDLQTopic,
			ConsumerGroup:  k.ConsumerGroup,
			RetryDelayMs:   jc.Retry.RetryDelay.Milliseconds(),
		}
		if !isManual {
			if _, err := cron.ParseStandard(jc.Schedule); err != nil {
				v.ParseError = err.Error()
			}
		}
		views = append(views, v)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })

	writeJSON(w, http.StatusOK, map[string]interface{}{"jobs": views})
}

// handleUpcoming computes the next `count` fire times per job from its cron
// schedule, merged across jobs and sorted soonest-first. It reads no scheduler
// state, so it is unaffected by which scheduler library drives execution.
func (api *consoleAPI) handleUpcoming(w http.ResponseWriter, r *http.Request) {
	count := atoiOr(r.URL.Query().Get("count"), defaultUpcomingCount)
	if count <= 0 {
		count = defaultUpcomingCount
	}
	if count > maxUpcomingCount {
		count = maxUpcomingCount
	}

	now := time.Now()
	runs := make([]upcomingRunView, 0)
	for _, jc := range api.svc.GetRegisteredJobs() {
		sched, err := cron.ParseStandard(jc.Schedule)
		if err != nil {
			continue
		}
		next := now
		for i := 0; i < count; i++ {
			next = sched.Next(next)
			if next.IsZero() {
				break
			}
			runs = append(runs, upcomingRunView{JobName: jc.Name, Schedule: jc.Schedule, RunAt: next})
		}
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].RunAt.Before(runs[j].RunAt) })

	writeJSON(w, http.StatusOK, map[string]interface{}{"runs": runs})
}
