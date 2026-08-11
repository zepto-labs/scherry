package console

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hibiken/asynq"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/executor"
	"github.com/zepto-labs/scherry/internal/jobconfig"
)

const (
	defaultAsynqQueue = "default"
	asynqJobIDPrefix  = "asynq:"
	maxAsynqOrphans   = 500
)

// triggerType maps an Asynq task type string to the display job name.
type triggerType struct {
	displayName string
	triggerKind string // "schedule" | "completion_check"
}

func registeredTriggerTypes(jobs map[string]jobconfig.JobConfig) map[string]triggerType {
	types := make(map[string]triggerType, len(jobs)*2)
	for name := range jobs {
		types[name] = triggerType{displayName: name, triggerKind: "schedule"}
		types[executor.CompletionCheckTaskType(name)] = triggerType{displayName: name, triggerKind: "completion_check"}
	}
	return types
}

// listAsynqTriggers returns failed/retrying Asynq trigger tasks for registered jobs.
func listAsynqTriggers(ctx context.Context, redisOpt asynq.RedisClientOpt, jobs map[string]jobconfig.JobConfig, filter JobFilter) ([]JobSummary, error) {
	types := registeredTriggerTypes(jobs)
	if len(types) == 0 {
		return nil, nil
	}

	inspector := asynq.NewInspector(redisOpt)
	defer inspector.Close()

	var tasks []*asynq.TaskInfo
	for _, listFn := range []func(string, ...asynq.ListOption) ([]*asynq.TaskInfo, error){
		inspector.ListArchivedTasks,
		inspector.ListRetryTasks,
	} {
		batch, err := listFn(defaultAsynqQueue, asynq.PageSize(maxAsynqOrphans))
		if err != nil {
			return nil, fmt.Errorf("list asynq triggers: %w", err)
		}
		tasks = append(tasks, batch...)
	}

	summaries := make([]JobSummary, 0)
	for _, info := range tasks {
		tt, ok := types[info.Type]
		if !ok {
			continue
		}
		if filter.RefID != "" && filter.RefID != info.ID {
			continue
		}
		if filter.Name != "" && filter.Name != tt.displayName {
			continue
		}
		summary := taskInfoToSummary(info, tt)
		if filter.Status != "" && summary.Status != filter.Status {
			continue
		}
		if filter.From != nil && summary.CreatedAt.Before(*filter.From) {
			continue
		}
		if filter.To != nil && summary.CreatedAt.After(*filter.To) {
			continue
		}
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

func taskInfoToSummary(info *asynq.TaskInfo, tt triggerType) JobSummary {
	status := domain.JobStatusFailed
	if info.State == asynq.TaskStateRetry {
		status = domain.JobStatusPending
	}

	createdAt := info.LastFailedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	startedAt := createdAt
	finishedAt := createdAt
	var durationMs int64

	refID := info.ID
	meta := map[string]interface{}{
		"asynq_task_id":     info.ID,
		"asynq_state":       info.State.String(),
		"asynq_queue":       info.Queue,
		"trigger_kind":      tt.triggerKind,
		"asynq_retry_count": info.Retried,
		"asynq_max_retry":   info.MaxRetry,
		"error":             info.LastErr,
		"source":            "asynq",
	}
	if !info.NextProcessAt.IsZero() {
		meta["next_process_at"] = info.NextProcessAt.Format(time.RFC3339)
	}
	if info.IsOrphaned {
		meta["is_orphaned"] = true
	}

	return JobSummary{
		Name:              tt.displayName,
		Status:            status,
		UniqueReferenceID: &refID,
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
		StartedAt:         &startedAt,
		FinishedAt:        &finishedAt,
		DurationMs:        &durationMs,
		Source:            "asynq",
		AsynqQueue:        info.Queue,
		AsynqTaskID:       info.ID,
		Metadata:          meta,
	}
}

func asynqTaskDetail(info *asynq.TaskInfo, tt triggerType) JobSummary {
	return taskInfoToSummary(info, tt)
}

func getAsynqTrigger(ctx context.Context, redisOpt asynq.RedisClientOpt, jobs map[string]jobconfig.JobConfig, queue, taskID string) (*JobSummary, error) {
	if queue == "" {
		queue = defaultAsynqQueue
	}
	types := registeredTriggerTypes(jobs)
	inspector := asynq.NewInspector(redisOpt)
	defer inspector.Close()

	info, err := inspector.GetTaskInfo(queue, taskID)
	if err != nil {
		return nil, err
	}
	tt, ok := types[info.Type]
	if !ok {
		return nil, fmt.Errorf("task %s is not a registered trigger", taskID)
	}
	summary := asynqTaskDetail(info, tt)
	return &summary, nil
}

func mergeJobSummaries(pg []JobSummary, asynq []JobSummary, offset, limit int) []JobSummary {
	merged := make([]JobSummary, 0, len(pg)+len(asynq))
	merged = append(merged, pg...)
	merged = append(merged, asynq...)

	sort.Slice(merged, func(i, j int) bool {
		return summarySortTime(merged[i]).After(summarySortTime(merged[j]))
	})

	if offset >= len(merged) {
		return nil
	}
	end := offset + limit
	if end > len(merged) {
		end = len(merged)
	}
	return merged[offset:end]
}

func summarySortTime(s JobSummary) time.Time {
	if s.FinishedAt != nil {
		return *s.FinishedAt
	}
	if s.StartedAt != nil {
		return *s.StartedAt
	}
	return s.CreatedAt
}

func parseAsynqJobID(raw string) (queue, taskID string, ok bool) {
	if !strings.HasPrefix(raw, asynqJobIDPrefix) {
		return "", "", false
	}
	return defaultAsynqQueue, strings.TrimPrefix(raw, asynqJobIDPrefix), true
}

func asynqJobID(taskID string) string {
	return asynqJobIDPrefix + taskID
}
