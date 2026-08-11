package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
)

// newMockConsoleStore builds a *consoleStore backed by go-sqlmock so its raw
// SQL queries can be asserted without touching a live Postgres server.
func newMockConsoleStore(t *testing.T) (*consoleStore, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	require.NoError(t, err)

	return &consoleStore{db: db}, mock
}

func TestConsoleStoreListJobsRefIDFilter(t *testing.T) {
	t.Run("filters by ref id using an exact match", func(t *testing.T) {
		store, mock := newMockConsoleStore(t)
		jobID := uuid.New()

		mock.ExpectQuery(`SELECT(.|\n)*FROM scherry_jobs j(.|\n)*WHERE 1 = 1 AND j\.unique_reference_id = \$1(.|\n)*`).
			WithArgs("abc-123", defaultConsolePageSize, 0).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "parent_job_id", "name", "status", "unique_reference_id",
				"created_at", "updated_at", "started_at", "finished_at", "duration_ms",
				"total", "completed", "failed", "running", "pending",
			}).AddRow(jobID, nil, "job-a", domain.JobStatusCompleted, "abc-123",
				time.Now(), time.Now(), nil, nil, nil,
				1, 1, 0, 0, 0))

		out, err := store.ListJobs(context.Background(), JobFilter{RefID: "abc-123"})
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, jobID, out[0].ID)
		assert.Equal(t, "abc-123", *out[0].UniqueReferenceID)
	})

	t.Run("empty ref id does not add a filter clause", func(t *testing.T) {
		store, mock := newMockConsoleStore(t)

		mock.ExpectQuery(`SELECT(.|\n)*FROM scherry_jobs j(.|\n)*WHERE 1 = 1(.|\n)*`).
			WithArgs(defaultConsolePageSize, 0).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "parent_job_id", "name", "status", "unique_reference_id",
				"created_at", "updated_at", "started_at", "finished_at", "duration_ms",
				"total", "completed", "failed", "running", "pending",
			}))

		out, err := store.ListJobs(context.Background(), JobFilter{})
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("combines ref id with name and status filters", func(t *testing.T) {
		store, mock := newMockConsoleStore(t)

		mock.ExpectQuery(`SELECT(.|\n)*FROM scherry_jobs j(.|\n)*WHERE 1 = 1 AND j\.name = \$1 AND j\.status = \$2 AND j\.unique_reference_id = \$3(.|\n)*`).
			WithArgs("job-a", domain.JobStatusCompleted, "ref", defaultConsolePageSize, 0).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "parent_job_id", "name", "status", "unique_reference_id",
				"created_at", "updated_at", "started_at", "finished_at", "duration_ms",
				"total", "completed", "failed", "running", "pending",
			}))

		_, err := store.ListJobs(context.Background(), JobFilter{Name: "job-a", Status: domain.JobStatusCompleted, RefID: "ref"})
		require.NoError(t, err)
	})
}

func TestConsoleHandlerListJobsRefIDQueryParam(t *testing.T) {
	store, mock := newMockConsoleStore(t)
	svc := &mockScheduler{db: store.db, jobs: map[string]jobconfig.JobConfig{}}
	handler := NewHandler(svc)

	jobID := uuid.New()
	mock.ExpectQuery(`SELECT(.|\n)*FROM scherry_jobs j(.|\n)*WHERE 1 = 1 AND j\.unique_reference_id = \$1(.|\n)*`).
		WithArgs("abc-123", maxConsolePageSize, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "parent_job_id", "name", "status", "unique_reference_id",
			"created_at", "updated_at", "started_at", "finished_at", "duration_ms",
			"total", "completed", "failed", "running", "pending",
		}).AddRow(jobID, nil, "job-a", domain.JobStatusCompleted, "abc-123",
			time.Now(), time.Now(), nil, nil, nil,
			1, 1, 0, 0, 0))

	// The leading/trailing whitespace in the raw query value verifies the
	// handler trims ref_id before it reaches the store.
	req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	req.URL.RawQuery = "ref_id=" + "%20%20abc-123%20%20"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), jobID.String())
}

func TestConsoleStoreGetJobDetail(t *testing.T) {
	t.Run("found with lineage", func(t *testing.T) {
		store, mock := newMockConsoleStore(t)
		jobID := uuid.New()
		parentID := uuid.New()
		childID := uuid.New()
		now := time.Now()

		mock.ExpectQuery(`SELECT \* FROM "scherry_jobs" WHERE id = \$1`).
			WithArgs(jobID, 1).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "parent_job_id", "name", "status", "created_at", "updated_at",
			}).AddRow(jobID, parentID, "job-a", domain.JobStatusCompleted, now, now))

		mock.ExpectQuery(`SELECT id, parent_job_id, name, status`).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "parent_job_id", "name", "status", "unique_reference_id",
				"created_at", "updated_at", "started_at", "finished_at", "duration_ms",
			}).AddRow(parentID, nil, "parent", domain.JobStatusCompleted, nil, now, now, nil, nil, nil).
				AddRow(childID, jobID, "child", domain.JobStatusPending, nil, now, now, nil, nil, nil))

		detail, err := store.GetJobDetail(context.Background(), jobID)
		require.NoError(t, err)
		require.NotNil(t, detail.Job)
		assert.Equal(t, jobID, detail.Job.ID)
		assert.Len(t, detail.Lineage, 2)
	})

	t.Run("not found", func(t *testing.T) {
		store, mock := newMockConsoleStore(t)
		jobID := uuid.New()

		mock.ExpectQuery(`SELECT \* FROM "scherry_jobs" WHERE id = \$1`).
			WithArgs(jobID, 1).
			WillReturnRows(sqlmock.NewRows([]string{"id"}))

		_, err := store.GetJobDetail(context.Background(), jobID)
		assert.Error(t, err)
	})
}

func TestConsoleStoreListTasksForJob(t *testing.T) {
	store, mock := newMockConsoleStore(t)
	jobID := uuid.New()
	taskID := uuid.New()

	mock.ExpectQuery(`SELECT count\(\*\) FROM scherry_tasks WHERE job_id = \$1`).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT (.|\n)*FROM scherry_tasks`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "job_id", "name", "status", "attempt", "max_retries", "distribution_key",
			"params", "result", "created_at", "updated_at", "started_at", "finished_at", "duration_ms", "sequence_number",
		}).AddRow(taskID, jobID, "t1", domain.TaskStatusCompleted, 1, 3, nil, nil, nil, time.Now(), time.Now(), nil, nil, nil, 0))

	page, err := store.ListTasksForJob(context.Background(), TaskFilter{JobID: jobID, Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 1, page.Total)
	assert.Len(t, page.Tasks, 1)
}

func TestConsoleStoreCountTasksForJob(t *testing.T) {
	store, mock := newMockConsoleStore(t)
	jobID := uuid.New()

	mock.ExpectQuery(`SELECT count\(\*\) FROM "scherry_tasks" WHERE job_id = \$1`).
		WithArgs(jobID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	count, err := store.CountTasksForJob(context.Background(), jobID)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestConsoleStoreListExistingRefIDs(t *testing.T) {
	store, mock := newMockConsoleStore(t)
	mock.ExpectQuery(`SELECT DISTINCT unique_reference_id FROM "scherry_jobs"`).
		WillReturnRows(sqlmock.NewRows([]string{"unique_reference_id"}).AddRow("ref-a").AddRow("ref-b"))

	found, err := store.ListExistingRefIDs(context.Background(), []string{"ref-a", "ref-b", "ref-c"})
	require.NoError(t, err)
	assert.Len(t, found, 2)
	assert.Contains(t, found, "ref-a")
	assert.Contains(t, found, "ref-b")
}

func TestConsoleStoreJobExecutionCounts(t *testing.T) {
	store, mock := newMockConsoleStore(t)
	mock.ExpectQuery(`SELECT name, status, count\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"name", "status", "count"}).
			AddRow("job-a", domain.JobStatusCompleted, 2))

	rows, err := store.JobExecutionCounts(context.Background(), nil, nil)
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, 2, rows[0].Count)
}

func TestConsoleStoreTaskTimeseries(t *testing.T) {
	store, mock := newMockConsoleStore(t)
	mock.ExpectQuery(`SELECT date_trunc`).
		WillReturnRows(sqlmock.NewRows([]string{"t", "succeeded", "failed", "other"}).
			AddRow(time.Now(), 1, 0, 0))

	buckets, err := store.TaskTimeseries(context.Background(), "", nil, nil, "day")
	require.NoError(t, err)
	assert.Len(t, buckets, 1)
}

func TestConsoleStoreJobMetrics(t *testing.T) {
	store, mock := newMockConsoleStore(t)
	mock.ExpectQuery(`SELECT name,`).
		WillReturnRows(sqlmock.NewRows([]string{"name", "success", "error", "total", "avg_ms"}).
			AddRow("job-a", 3, 1, 4, 12.5))

	metrics, err := store.JobMetrics(context.Background(), nil, nil)
	require.NoError(t, err)
	assert.Contains(t, metrics, "job-a")
	assert.Equal(t, 3, metrics["job-a"].Success)
}
