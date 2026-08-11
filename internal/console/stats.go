package console

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/zepto-labs/scherry/internal/domain"
)

// jobStatusCount is one (job name, job status) tally for the executions bar
// chart. Counts are job runs (scherry_jobs rows), not tasks.
type jobStatusCount struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Count  int    `json:"count"`
}

// timeseriesBucket is one time bucket of task counts split into outcome groups.
type timeseriesBucket struct {
	T         time.Time `json:"t"`
	Succeeded int       `json:"succeeded"`
	Failed    int       `json:"failed"`
	Other     int       `json:"other"`
}

// jobMetricRow is the DB-side per-job execution metric (before next-run is
// merged in from the registered schedule).
type jobMetricRow struct {
	Name    string  `json:"name"`
	Success int     `json:"success"`
	Error   int     `json:"error"`
	Total   int     `json:"total"`
	AvgMs   float64 `json:"avg_ms"`
}

// jobMetricView is a full row for the job metrics table.
type jobMetricView struct {
	Name     string     `json:"name"`
	NextRun  *time.Time `json:"next_run"`
	AvgMs    float64    `json:"avg_ms"`
	Success  int        `json:"success"`
	Error    int        `json:"error"`
	ErrorPct float64    `json:"error_pct"`
}

// resolveBucket whitelists the date_trunc unit; it is never taken from raw input
// verbatim into SQL. Empty picks a unit from the range width.
func resolveBucket(bucket string, from, to *time.Time) string {
	switch bucket {
	case "minute", "hour", "day":
		return bucket
	}
	if from != nil {
		end := time.Now()
		if to != nil {
			end = *to
		}
		width := end.Sub(*from)
		switch {
		case width <= 2*time.Hour:
			return "minute"
		case width <= 48*time.Hour:
			return "hour"
		}
	}
	return "day"
}

// createdAtRange appends optional created_at bounds to a condition/arg pair.
func createdAtRange(conds []string, args []interface{}, from, to *time.Time) ([]string, []interface{}) {
	if from != nil {
		conds = append(conds, fmt.Sprintf("created_at >= $%d", len(args)+1))
		args = append(args, *from)
	}
	if to != nil {
		conds = append(conds, fmt.Sprintf("created_at <= $%d", len(args)+1))
		args = append(args, *to)
	}
	return conds, args
}

// JobExecutionCounts returns per-(job name, job status) run counts, optionally
// bounded by job created_at.
func (c *consoleStore) JobExecutionCounts(ctx context.Context, from, to *time.Time) ([]jobStatusCount, error) {
	conds := []string{"1 = 1"}
	args := []interface{}{}
	conds, args = createdAtRange(conds, args, from, to)

	query := fmt.Sprintf(`
		SELECT name, status, count(*) AS count
		FROM scherry_jobs
		WHERE %s
		GROUP BY name, status
		ORDER BY name ASC`, strings.Join(conds, " AND "))

	var out []jobStatusCount
	if err := c.db.WithContext(ctx).Raw(query, args...).Scan(&out).Error; err != nil {
		return nil, err
	}
	return out, nil
}

// SQL fragments for the three time-series outcome buckets, built from the status
// constants so they track any status renames.
func outcomeFilters() (succeeded, failed, other string) {
	failedIn := fmt.Sprintf("'%s','%s'", domain.TaskStatusFailed, domain.TaskStatusMaxRetriesExhausted)
	succeeded = fmt.Sprintf("count(*) FILTER (WHERE t.status = '%s') AS succeeded", domain.TaskStatusCompleted)
	failed = fmt.Sprintf("count(*) FILTER (WHERE t.status IN (%s)) AS failed", failedIn)
	other = fmt.Sprintf("count(*) FILTER (WHERE t.status NOT IN ('%s',%s)) AS other", domain.TaskStatusCompleted, failedIn)
	return
}

// TaskTimeseries returns task counts bucketed by created_at into outcome groups,
// optionally filtered to a single job name.
func (c *consoleStore) TaskTimeseries(ctx context.Context, job string, from, to *time.Time, unit string) ([]timeseriesBucket, error) {
	conds := []string{"1 = 1"}
	args := []interface{}{}
	next := func() string { return fmt.Sprintf("$%d", len(args)+1) }

	joinJobs := ""
	if job != "" {
		joinJobs = "JOIN scherry_jobs j ON j.id = t.job_id"
		conds = append(conds, "j.name = "+next())
		args = append(args, job)
	}
	if from != nil {
		conds = append(conds, "t.created_at >= "+next())
		args = append(args, *from)
	}
	if to != nil {
		conds = append(conds, "t.created_at <= "+next())
		args = append(args, *to)
	}

	succeeded, failed, other := outcomeFilters()
	query := fmt.Sprintf(`
		SELECT date_trunc('%s', t.created_at) AS t, %s, %s, %s
		FROM scherry_tasks t %s
		WHERE %s
		GROUP BY 1
		ORDER BY 1 ASC`, unit, succeeded, failed, other, joinJobs, strings.Join(conds, " AND "))

	var out []timeseriesBucket
	if err := c.db.WithContext(ctx).Raw(query, args...).Scan(&out).Error; err != nil {
		return nil, err
	}
	return out, nil
}

// JobMetrics returns per-job-name execution metrics: success/error counts and
// average run duration. Success = COMPLETED; Error = FAILED + PARTIAL_FAILED.
func (c *consoleStore) JobMetrics(ctx context.Context, from, to *time.Time) (map[string]jobMetricRow, error) {
	conds := []string{"1 = 1"}
	args := []interface{}{}
	conds, args = createdAtRange(conds, args, from, to)

	query := fmt.Sprintf(`
		SELECT name,
			count(*) FILTER (WHERE status = '%s') AS success,
			count(*) FILTER (WHERE status IN ('%s','%s')) AS error,
			count(*) AS total,
			coalesce(avg(duration_ms) FILTER (WHERE duration_ms IS NOT NULL), 0) AS avg_ms
		FROM scherry_jobs
		WHERE %s
		GROUP BY name`,
		domain.JobStatusCompleted, domain.JobStatusFailed, domain.JobStatusPartialFailed, strings.Join(conds, " AND "))

	var rows []jobMetricRow
	if err := c.db.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	byName := make(map[string]jobMetricRow, len(rows))
	for _, r := range rows {
		byName[r.Name] = r
	}
	return byName, nil
}

func (api *consoleAPI) handleStatsJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	rows, err := api.store.JobExecutionCounts(r.Context(), parseTimeParam(q.Get("from")), parseTimeParam(q.Get("to")))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"counts": rows})
}

func (api *consoleAPI) handleStatsTimeseries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from := parseTimeParam(q.Get("from"))
	to := parseTimeParam(q.Get("to"))
	unit := resolveBucket(q.Get("bucket"), from, to)

	buckets, err := api.store.TaskTimeseries(r.Context(), q.Get("job"), from, to, unit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"buckets": buckets,
		"bucket":  unit,
		"job":     q.Get("job"),
	})
}

// handleStatsJobMetrics merges DB execution metrics with each registered job's
// next scheduled run. Rows are the registered jobs (which carry the schedule).
func (api *consoleAPI) handleStatsJobMetrics(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	metrics, err := api.store.JobMetrics(r.Context(), parseTimeParam(q.Get("from")), parseTimeParam(q.Get("to")))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	now := time.Now()
	views := make([]jobMetricView, 0, len(metrics))
	for _, jc := range api.svc.GetRegisteredJobs() {
		m := metrics[jc.Name]
		var nextRun *time.Time
		if sched, perr := cron.ParseStandard(jc.Schedule); perr == nil {
			n := sched.Next(now)
			nextRun = &n
		}
		var pct float64
		if finished := m.Success + m.Error; finished > 0 {
			pct = float64(m.Error) / float64(finished) * 100
		}
		views = append(views, jobMetricView{
			Name:     jc.Name,
			NextRun:  nextRun,
			AvgMs:    m.AvgMs,
			Success:  m.Success,
			Error:    m.Error,
			ErrorPct: pct,
		})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })

	writeJSON(w, http.StatusOK, map[string]interface{}{"jobs": views})
}
