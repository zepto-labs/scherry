package console

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/executor"
	"github.com/zepto-labs/scherry/internal/jobconfig"
)

func setupMiniRedis(t *testing.T) asynq.RedisClientOpt {
	t.Helper()
	mr := miniredis.RunT(t)
	return asynq.RedisClientOpt{Addr: mr.Addr()}
}

// archiveAsynqTask enqueues a task of the given type, lets it fail once, and
// returns its ID after it reaches the archived state.
//
// The worker is stopped before returning: leaving it running would let it
// compete for tasks with any server started by a later call on the same Redis,
// and a server without a handler for the other task type fails the task before
// that call's handler ever sees it.
func archiveAsynqTask(t *testing.T, opt asynq.RedisClientOpt, taskType string) string {
	t.Helper()
	processed := make(chan struct{}, 1)
	srv := asynq.NewServer(opt, asynq.Config{Concurrency: 1, LogLevel: asynq.FatalLevel})
	mux := asynq.NewServeMux()
	mux.HandleFunc(taskType, func(context.Context, *asynq.Task) error {
		processed <- struct{}{}
		return errors.New("trigger failed")
	})
	go func() { _ = srv.Run(mux) }()
	defer srv.Shutdown()

	client := asynq.NewClient(opt)
	defer func() { _ = client.Close() }()

	info, err := client.Enqueue(asynq.NewTask(taskType, nil), asynq.MaxRetry(0))
	require.NoError(t, err)

	select {
	case <-processed:
	case <-time.After(3 * time.Second):
		t.Fatal("task was not processed")
	}

	inspector := asynq.NewInspector(opt)
	defer func() { _ = inspector.Close() }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ti, err := inspector.GetTaskInfo(defaultAsynqQueue, info.ID)
		if err == nil && ti.State == asynq.TaskStateArchived {
			return info.ID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("task never archived")
	return ""
}

func TestMergeJobSummaries(t *testing.T) {
	now := time.Now()
	older := now.Add(-time.Hour)
	pg := []JobSummary{{
		ID: uuid.New(), Name: "a", Status: domain.JobStatusFailed,
		CreatedAt: older, Source: "postgres",
	}}
	asynq := []JobSummary{{
		Name: "b", Status: domain.JobStatusFailed,
		CreatedAt: now, Source: "asynq", AsynqTaskID: "task-1",
	}}

	merged := mergeJobSummaries(pg, asynq, 0, 10)
	assert.Len(t, merged, 2)
	assert.Equal(t, "b", merged[0].Name)
	assert.Equal(t, "a", merged[1].Name)

	page := mergeJobSummaries(pg, asynq, 1, 10)
	assert.Len(t, page, 1)
	assert.Equal(t, "a", page[0].Name)
}

func TestTaskInfoToSummary(t *testing.T) {
	failedAt := time.Now().Add(-time.Minute)
	ref := "task-abc"
	info := &asynq.TaskInfo{
		ID: "task-abc", Queue: "default", Type: "payroll",
		State: asynq.TaskStateArchived, MaxRetry: 3, Retried: 3,
		LastErr: "boom", LastFailedAt: failedAt,
	}
	summary := taskInfoToSummary(info, triggerType{displayName: "payroll", triggerKind: "schedule"})
	assert.Equal(t, "payroll", summary.Name)
	assert.Equal(t, domain.JobStatusFailed, summary.Status)
	assert.Equal(t, "asynq", summary.Source)
	assert.Equal(t, &ref, summary.UniqueReferenceID)
	assert.Equal(t, "boom", summary.Metadata["error"])
}

func TestParseAsynqJobID(t *testing.T) {
	queue, taskID, ok := parseAsynqJobID("asynq:abc-123")
	assert.True(t, ok)
	assert.Equal(t, "default", queue)
	assert.Equal(t, "abc-123", taskID)

	_, _, ok = parseAsynqJobID(uuid.New().String())
	assert.False(t, ok)
}

func TestSummaryToViewAsynq(t *testing.T) {
	summary := JobSummary{
		Name: "payroll", Status: domain.JobStatusFailed,
		Source: "asynq", AsynqTaskID: "task-xyz",
		CreatedAt: time.Now(),
		Metadata:  map[string]interface{}{"error": "failed"},
	}
	view := summaryToView(summary)
	assert.Equal(t, "asynq:task-xyz", view.ID)
	assert.Equal(t, "asynq", view.Source)
	assert.Equal(t, "failed", view.Metadata.(map[string]interface{})["error"])
}

func TestRegisteredTriggerTypes(t *testing.T) {
	types := registeredTriggerTypes(map[string]jobconfig.JobConfig{
		"payroll": {Name: "payroll"},
	})
	assert.Equal(t, "payroll", types["payroll"].displayName)
	assert.Equal(t, "schedule", types["payroll"].triggerKind)
	assert.Equal(t, "completion_check", types[executor.CompletionCheckTaskType("payroll")].triggerKind)
	assert.Empty(t, registeredTriggerTypes(nil))
}

func TestTaskInfoToSummaryVariants(t *testing.T) {
	t.Run("retry state and metadata fields", func(t *testing.T) {
		next := time.Now().Add(time.Hour)
		info := &asynq.TaskInfo{
			ID: "task-1", Queue: "default", Type: "payroll",
			State: asynq.TaskStateRetry, MaxRetry: 5, Retried: 2,
			LastErr: "retrying", IsOrphaned: true, NextProcessAt: next,
		}
		summary := taskInfoToSummary(info, triggerType{displayName: "payroll", triggerKind: "schedule"})
		assert.Equal(t, domain.JobStatusPending, summary.Status)
		assert.Equal(t, "retrying", summary.Metadata["error"])
		assert.Equal(t, true, summary.Metadata["is_orphaned"])
		assert.NotEmpty(t, summary.Metadata["next_process_at"])
	})

	t.Run("uses current time when last failed is zero", func(t *testing.T) {
		info := &asynq.TaskInfo{ID: "task-2", Queue: "default", Type: "payroll", State: asynq.TaskStateArchived}
		summary := taskInfoToSummary(info, triggerType{displayName: "payroll", triggerKind: "schedule"})
		assert.False(t, summary.CreatedAt.IsZero())
	})
}

func TestSummarySortTime(t *testing.T) {
	created := time.Now().Add(-2 * time.Hour)
	started := time.Now().Add(-time.Hour)
	finished := time.Now()
	assert.Equal(t, finished, summarySortTime(JobSummary{FinishedAt: &finished, StartedAt: &started, CreatedAt: created}))
	assert.Equal(t, started, summarySortTime(JobSummary{StartedAt: &started, CreatedAt: created}))
	assert.Equal(t, created, summarySortTime(JobSummary{CreatedAt: created}))
}

func TestMergeJobSummariesPagination(t *testing.T) {
	now := time.Now()
	items := []JobSummary{{Name: "a", CreatedAt: now}, {Name: "b", CreatedAt: now.Add(-time.Hour)}}
	assert.Nil(t, mergeJobSummaries(items, nil, 5, 10))
	assert.Len(t, mergeJobSummaries(items, nil, 1, 10), 1)
}

func TestAsynqTaskDetail(t *testing.T) {
	info := &asynq.TaskInfo{ID: "task-1", Queue: "default", Type: "payroll", State: asynq.TaskStateArchived}
	tt := triggerType{displayName: "payroll", triggerKind: "schedule"}
	summary := asynqTaskDetail(info, tt)
	assert.Equal(t, "payroll", summary.Name)
	assert.Equal(t, "asynq:task-1", asynqJobID(info.ID))
}

func TestListAsynqTriggers(t *testing.T) {
	t.Run("empty when no registered jobs", func(t *testing.T) {
		opt := setupMiniRedis(t)
		summaries, err := listAsynqTriggers(context.Background(), opt, nil, JobFilter{})
		assert.NoError(t, err)
		assert.Nil(t, summaries)
	})

	t.Run("returns archived trigger tasks with filters", func(t *testing.T) {
		opt := setupMiniRedis(t)
		taskID := archiveAsynqTask(t, opt, "payroll")
		// Unregistered task type should be ignored.
		archiveAsynqTask(t, opt, "unknown-type")

		jobs := map[string]jobconfig.JobConfig{"payroll": {Name: "payroll"}}
		all, err := listAsynqTriggers(context.Background(), opt, jobs, JobFilter{})
		require.NoError(t, err)
		require.Len(t, all, 1)
		assert.Equal(t, "payroll", all[0].Name)
		assert.Equal(t, taskID, all[0].AsynqTaskID)

		byName, err := listAsynqTriggers(context.Background(), opt, jobs, JobFilter{Name: "payroll"})
		require.NoError(t, err)
		assert.Len(t, byName, 1)

		otherName, err := listAsynqTriggers(context.Background(), opt, jobs, JobFilter{Name: "other"})
		require.NoError(t, err)
		assert.Empty(t, otherName)

		byRef, err := listAsynqTriggers(context.Background(), opt, jobs, JobFilter{RefID: taskID})
		require.NoError(t, err)
		assert.Len(t, byRef, 1)

		byStatus, err := listAsynqTriggers(context.Background(), opt, jobs, JobFilter{Status: domain.JobStatusFailed})
		require.NoError(t, err)
		assert.Len(t, byStatus, 1)

		future := time.Now().Add(time.Hour)
		past, err := listAsynqTriggers(context.Background(), opt, jobs, JobFilter{From: &future})
		require.NoError(t, err)
		assert.Empty(t, past)
	})
}

func TestGetAsynqTrigger(t *testing.T) {
	opt := setupMiniRedis(t)
	taskID := archiveAsynqTask(t, opt, "payroll")
	jobs := map[string]jobconfig.JobConfig{"payroll": {Name: "payroll"}}

	summary, err := getAsynqTrigger(context.Background(), opt, jobs, "", taskID)
	require.NoError(t, err)
	require.NotNil(t, summary)
	assert.Equal(t, "payroll", summary.Name)
	assert.Equal(t, taskID, summary.AsynqTaskID)

	_, err = getAsynqTrigger(context.Background(), opt, jobs, "", "missing-task")
	assert.Error(t, err)

	client := asynq.NewClient(opt)
	t.Cleanup(func() { _ = client.Close() })
	other, err := client.Enqueue(asynq.NewTask("not-registered", nil))
	require.NoError(t, err)
	_, err = getAsynqTrigger(context.Background(), opt, jobs, "", other.ID)
	assert.ErrorContains(t, err, "not a registered trigger")
}
