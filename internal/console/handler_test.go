package console

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/executor"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/logging"
	"github.com/zepto-labs/scherry/internal/repository"
	"github.com/zepto-labs/scherry/internal/testutil"
)

// mockScheduler is a minimal console.Scheduler backed by a
// testutil.MockRepository, delegating RetryJob to the real
// internal/executor logic so these tests exercise the same retry path
// production code does instead of a bare stub.
type mockScheduler struct {
	repo repository.Repository
	jobs map[string]jobconfig.JobConfig
	db   *gorm.DB
}

func (m *mockScheduler) DB() *gorm.DB { return m.db }

func (m *mockScheduler) GetRegisteredJobs() map[string]jobconfig.JobConfig {
	out := make(map[string]jobconfig.JobConfig, len(m.jobs))
	for k, v := range m.jobs {
		out[k] = v
	}
	return out
}

func (m *mockScheduler) AsynqRedisOpt() asynq.RedisClientOpt {
	return asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}
}

func (m *mockScheduler) RetryJob(ctx context.Context, jobID uuid.UUID, fullRetry bool, refID string) (*domain.Job, error) {
	return executor.RetryJob(ctx, m.repo, logging.NopLogger{}, m.jobs, jobID, fullRetry, refID)
}

func (m *mockScheduler) RetryTriggerJob(ctx context.Context, jobID uuid.UUID, refID string) (*domain.Job, error) {
	return executor.RetryTriggerJob(ctx, m.repo, logging.NopLogger{}, m.jobs, jobID, refID)
}

func (m *mockScheduler) RetryAsynqTask(ctx context.Context, queue, taskID string) error {
	return errors.New("asynq not available in unit tests")
}

func (m *mockScheduler) FindJobByID(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	return m.repo.FindJobByID(ctx, jobID)
}

func (m *mockScheduler) TerminateJob(ctx context.Context, jobID uuid.UUID) error {
	return m.repo.TerminateJob(ctx, jobID)
}

// redisBackedScheduler uses miniredis for Asynq-backed console endpoints.
type redisBackedScheduler struct {
	mockScheduler
	redisOpt asynq.RedisClientOpt
}

func (r *redisBackedScheduler) AsynqRedisOpt() asynq.RedisClientOpt { return r.redisOpt }

func (r *redisBackedScheduler) RetryAsynqTask(ctx context.Context, queue, taskID string) error {
	if queue == "" {
		queue = "default"
	}
	inspector := asynq.NewInspector(r.redisOpt)
	defer inspector.Close()
	return inspector.RunTask(queue, taskID)
}

func TestDecodeJSONB(t *testing.T) {
	t.Run("not present returns nil", func(t *testing.T) {
		var j pgtype.JSONB
		assert.Nil(t, decodeJSONB(j))
	})

	t.Run("present with valid json", func(t *testing.T) {
		var j pgtype.JSONB
		assert.NoError(t, j.Set(map[string]interface{}{"a": float64(1)}))
		assert.Equal(t, map[string]interface{}{"a": float64(1)}, decodeJSONB(j))
	})

	t.Run("present but invalid json falls back to raw string", func(t *testing.T) {
		j := pgtype.JSONB{Bytes: []byte("not-json"), Status: pgtype.Present}
		assert.Equal(t, "not-json", decodeJSONB(j))
	})

	t.Run("present with empty bytes returns nil", func(t *testing.T) {
		j := pgtype.JSONB{Bytes: []byte{}, Status: pgtype.Present}
		assert.Nil(t, decodeJSONB(j))
	})
}

func TestUUIDPtrToString(t *testing.T) {
	assert.Nil(t, uuidPtrToString(nil))

	id := uuid.New()
	s := uuidPtrToString(&id)
	assert.NotNil(t, s)
	assert.Equal(t, id.String(), *s)
}

func TestParseTimeParam(t *testing.T) {
	assert.Nil(t, parseTimeParam(""))
	assert.Nil(t, parseTimeParam("not-a-time"))

	got := parseTimeParam("2024-01-02T15:04:05Z")
	assert.NotNil(t, got)
	assert.Equal(t, 2024, got.Year())
}

func TestAtoiOr(t *testing.T) {
	assert.Equal(t, 5, atoiOr("", 5))
	assert.Equal(t, 5, atoiOr("nope", 5))
	assert.Equal(t, 42, atoiOr("42", 5))
}

func TestSummaryToView(t *testing.T) {
	id := uuid.New()
	parentID := uuid.New()
	now := time.Now()
	summary := JobSummary{
		ID: id, ParentJobID: &parentID, Name: "n", Status: domain.JobStatusCompleted,
		CreatedAt: now, Total: 3, Completed: 2, Failed: 1, Running: 0, Pending: 0,
	}

	view := summaryToView(summary)
	assert.Equal(t, id.String(), view.ID)
	assert.Equal(t, parentID.String(), *view.ParentJobID)
	assert.Equal(t, "n", view.Name)
	assert.Equal(t, domain.JobStatusCompleted, view.Status)
	assert.Equal(t, progressView{Total: 3, Completed: 2, Failed: 1}, view.Progress)
}

func TestJobToView(t *testing.T) {
	var metadata pgtype.JSONB
	assert.NoError(t, metadata.Set(map[string]interface{}{"k": "v"}))
	job := &domain.Job{ID: uuid.New(), Name: "n", Status: domain.JobStatusRunning, Metadata: metadata}

	view := jobToView(job)
	assert.Equal(t, job.ID.String(), view.ID)
	assert.Equal(t, "n", view.Name)
	assert.Equal(t, map[string]interface{}{"k": "v"}, view.Metadata)
}

func TestTaskToView(t *testing.T) {
	var params, result pgtype.JSONB
	assert.NoError(t, params.Set(map[string]interface{}{"p": float64(1)}))
	assert.NoError(t, result.Set(map[string]interface{}{"r": float64(2)}))
	task := domain.Task{ID: uuid.New(), Name: "t", Status: domain.TaskStatusCompleted, Params: params, Result: result}

	view := taskToView(task)
	assert.Equal(t, task.ID.String(), view.ID)
	assert.Equal(t, map[string]interface{}{"p": float64(1)}, view.Params)
	assert.Equal(t, map[string]interface{}{"r": float64(2)}, view.Result)
}

func TestWriteJSONAndWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusTeapot, map[string]string{"hello": "world"})
	assert.Equal(t, http.StatusTeapot, rec.Code)
	assert.Contains(t, rec.Body.String(), `"hello":"world"`)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	rec2 := httptest.NewRecorder()
	writeError(rec2, http.StatusBadRequest, errors.New("bad input"))
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "bad input")
}

func TestConsoleHandlerIndexAndMeta(t *testing.T) {
	svc := &mockScheduler{repo: &testutil.MockRepository{}, jobs: map[string]jobconfig.JobConfig{
		"job-a": {Name: "job-a"},
		"job-b": {Name: "job-b"},
	}}
	handler := NewHandler(svc)
	assert.NotNil(t, handler)

	t.Run("index page", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "text/html; charset=utf-8", rec.Header().Get("Content-Type"))

		// The page is rendered via html/template, which drops comments but
		// must leave the app shell itself intact.
		body := rec.Body.String()
		assert.Contains(t, body, `<section id="view-overview">`)
		assert.Contains(t, body, `<section id="view-history"`)
		assert.Contains(t, body, `id="drawer"`)
	})

	t.Run("unknown nested path under root pattern 404s", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("meta endpoint reports registered job names", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/jobs/meta", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		body := rec.Body.String()
		assert.Contains(t, body, "job-a")
		assert.Contains(t, body, "job-b")
		assert.Contains(t, body, "job_statuses")
	})
}

func TestConsoleHandlerRetryAndTerminate(t *testing.T) {
	repo := &testutil.MockRepository{}
	pub := &testutil.MockPublisher{}
	svc := &mockScheduler{repo: repo, jobs: map[string]jobconfig.JobConfig{
		"job-a": {Name: "job-a", Publisher: pub, Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}, Hooks: jobconfig.NopHooks()},
	}}
	handler := NewHandler(svc)

	t.Run("retry with invalid job id returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/jobs/not-a-uuid/retry", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("retry with no tasks to retry completes cloned job", func(t *testing.T) {
		jobID := uuid.New()
		job := &domain.Job{ID: jobID, Name: "job-a", Status: domain.JobStatusCompleted}
		repo.On("FindJobByID", mock.Anything, jobID).Return(job, nil).Twice()
		repo.On("FindPendingJobByParentID", mock.Anything, jobID).Return((*domain.Job)(nil), nil).Once()
		repo.On("CreateJobAndTasks", mock.Anything, mock.Anything, []domain.Task(nil)).
			Run(func(args mock.Arguments) {
				clonedJob := args.Get(1).(*domain.Job)
				repo.On("FindJobByID", mock.Anything, clonedJob.ID).Return(clonedJob, nil).Once()
			}).
			Return(nil, nil, nil).Once()
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, jobID,
			[]string{domain.TaskStatusMaxRetriesExhausted}, -1, repository.TaskInsertBatchSize).Return([]domain.Task{}, nil).Once()
		repo.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		repo.On("UpdateJobIfStatus", mock.Anything, mock.Anything, domain.JobStatusRunning, mock.Anything).Return(true, nil)
		repo.On("GetTaskStatusSummary", mock.Anything, mock.Anything).Return(
			repository.TaskStatusSummary{Total: 0}, nil).Once()

		req := httptest.NewRequest(http.MethodPost, "/api/jobs/"+jobID.String()+"/retry", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"retried":true`)
		assert.Contains(t, rec.Body.String(), `"status":"COMPLETED"`)
	})

	t.Run("retry returns retried false when clone already running", func(t *testing.T) {
		jobID := uuid.New()
		clonedID := uuid.New()
		job := &domain.Job{ID: jobID, Name: "job-a", Status: domain.JobStatusCompleted}
		running := &domain.Job{ID: clonedID, Name: "job-a", Status: domain.JobStatusRunning}
		repo.On("FindJobByID", mock.Anything, jobID).Return(job, nil).Twice()
		repo.On("FindPendingJobByParentID", mock.Anything, jobID).Return(running, nil).Once()
		repo.On("FindTasksByJobIDAndStatusesBatch", mock.Anything, clonedID,
			[]string(nil), -1, repository.TaskInsertBatchSize).Return([]domain.Task{}, nil).Once()
		repo.On("GetTaskStatusSummary", mock.Anything, clonedID).Return(
			repository.TaskStatusSummary{Total: 1, NonTerminal: 1}, nil).Once()

		req := httptest.NewRequest(http.MethodPost, "/api/jobs/"+jobID.String()+"/retry", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"retried":false`)
	})

	t.Run("retry trigger failure job with zero tasks", func(t *testing.T) {
		store, sqlMock := newMockConsoleStore(t)
		jobExec := &testutil.MockJobExecutor{}
		triggerRepo := &testutil.MockRepository{}
		triggerSvc := &mockScheduler{
			repo: triggerRepo,
			db:   store.db,
			jobs: map[string]jobconfig.JobConfig{
				"job-a": {
					Name: "job-a", JobExecutor: jobExec, Publisher: pub, Hooks: jobconfig.NopHooks(),
					Kafka: jobconfig.KafkaConfig{TaskTopic: "topic"}, Enabled: func() bool { return true },
				},
			},
		}
		triggerHandler := NewHandler(triggerSvc)

		jobID := uuid.New()
		retriedID := uuid.New()
		job := &domain.Job{ID: jobID, Name: "job-a", Status: domain.JobStatusFailed, UniqueReferenceID: testutil.StringPtr("orig-ref")}
		require.NoError(t, job.SetMetadata(map[string]interface{}{"trigger_failure": true}))
		retried := &domain.Job{ID: retriedID, Name: "job-a", Status: domain.JobStatusPending}
		completed := &domain.Job{ID: retriedID, Name: "job-a", Status: domain.JobStatusCompleted}

		triggerRepo.On("FindJobByID", mock.Anything, jobID).Return(job, nil).Twice()
		sqlMock.ExpectQuery(`SELECT count\(\*\) FROM "scherry_tasks" WHERE job_id = \$1`).
			WithArgs(jobID).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		triggerRepo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, mock.Anything).
			Return((*domain.Job)(nil), []domain.Task{}, nil).Once()
		jobExec.On("Execute", mock.Anything, mock.Anything).Return([]jobconfig.TaskData{}, nil)
		triggerRepo.On("CreateJobAndTasks", mock.Anything, mock.Anything, mock.Anything).Return(retried, []domain.Task{}, nil)
		triggerRepo.On("FindJobByID", mock.Anything, retriedID).Return(retried, nil)
		triggerRepo.On("GetTaskStatusSummary", mock.Anything, retriedID).Return(repository.TaskStatusSummary{Total: 0}, nil)
		triggerRepo.On("UpdateJob", mock.Anything, retriedID, mock.Anything).Return(nil)
		triggerRepo.On("UpdateJobIfStatus", mock.Anything, retriedID, domain.JobStatusRunning, mock.Anything).Return(true, nil)
		triggerRepo.On("FindJobWithTasksByUniqueReferenceID", mock.Anything, mock.Anything).
			Return(completed, []domain.Task{}, nil).Once()

		req := httptest.NewRequest(http.MethodPost, "/api/jobs/"+jobID.String()+"/retry", nil)
		rec := httptest.NewRecorder()
		triggerHandler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"retry_method":"trigger"`)
		assert.Contains(t, rec.Body.String(), `"retried":true`)
	})

	t.Run("retry job not found returns 404", func(t *testing.T) {
		jobID := uuid.New()
		repo.On("FindJobByID", mock.Anything, jobID).Return((*domain.Job)(nil), errors.New("missing")).Once()

		req := httptest.NewRequest(http.MethodPost, "/api/jobs/"+jobID.String()+"/retry", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("retry asynq trigger returns 400 without redis", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/jobs/asynq:task-123/retry", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("job detail for asynq trigger returns 404 without redis", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/jobs/asynq:task-123", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("terminate with invalid job id returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/jobs/not-a-uuid/terminate", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("terminate success", func(t *testing.T) {
		jobID := uuid.New()
		repo.On("TerminateJob", mock.Anything, jobID).Return(nil).Once()

		req := httptest.NewRequest(http.MethodPost, "/api/jobs/"+jobID.String()+"/terminate", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"terminated":true`)
	})

	t.Run("terminate already-terminal job returns 409", func(t *testing.T) {
		jobID := uuid.New()
		repo.On("TerminateJob", mock.Anything, jobID).Return(repository.ErrJobAlreadyTerminal).Once()

		req := httptest.NewRequest(http.MethodPost, "/api/jobs/"+jobID.String()+"/terminate", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusConflict, rec.Code)
	})

	t.Run("terminate unexpected repository error returns 500", func(t *testing.T) {
		jobID := uuid.New()
		repo.On("TerminateJob", mock.Anything, jobID).Return(errors.New("db exploded")).Once()

		req := httptest.NewRequest(http.MethodPost, "/api/jobs/"+jobID.String()+"/terminate", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

func TestConsoleHandlerListJobsWithFilters(t *testing.T) {
	store, sqlMock := newMockConsoleStore(t)
	svc := &mockScheduler{db: store.db, jobs: map[string]jobconfig.JobConfig{}}
	handler := NewHandler(svc)
	jobID := uuid.New()
	now := time.Now()

	sqlMock.ExpectQuery(`SELECT(.|\n)*FROM scherry_jobs j(.|\n)*WHERE 1 = 1 AND j\.name = \$1 AND j\.status = \$2(.|\n)*`).
		WithArgs("job-a", domain.JobStatusCompleted, maxConsolePageSize, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "parent_job_id", "name", "status", "unique_reference_id",
			"created_at", "updated_at", "started_at", "finished_at", "duration_ms",
			"total", "completed", "failed", "running", "pending",
		}).AddRow(jobID, nil, "job-a", domain.JobStatusCompleted, nil, now, now, nil, nil, nil, 0, 0, 0, 0, 0))

	req := httptest.NewRequest(http.MethodGet, "/api/jobs?name=job-a&status=COMPLETED&limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), jobID.String())
}

func TestConsoleHandlerJobDetailAndTasks(t *testing.T) {
	store, mock := newMockConsoleStore(t)
	svc := &mockScheduler{db: store.db, jobs: map[string]jobconfig.JobConfig{}}
	handler := NewHandler(svc)
	jobID := uuid.New()
	now := time.Now()

	t.Run("job detail success", func(t *testing.T) {
		mock.ExpectQuery(`SELECT \* FROM "scherry_jobs" WHERE id = \$1`).
			WithArgs(jobID, 1).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "parent_job_id", "name", "status", "created_at", "updated_at",
			}).AddRow(jobID, nil, "job-a", domain.JobStatusCompleted, now, now))
		mock.ExpectQuery(`SELECT id, parent_job_id, name, status`).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "parent_job_id", "name", "status", "unique_reference_id",
				"created_at", "updated_at", "started_at", "finished_at", "duration_ms",
			}))

		req := httptest.NewRequest(http.MethodGet, "/api/jobs/"+jobID.String(), nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), jobID.String())
	})

	t.Run("job detail invalid id", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/jobs/not-a-uuid", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("list tasks success", func(t *testing.T) {
		taskID := uuid.New()
		mock.ExpectQuery(`SELECT count\(\*\) FROM scherry_tasks WHERE job_id = \$1`).
			WithArgs(jobID).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
		mock.ExpectQuery(`SELECT (.|\n)*FROM scherry_tasks`).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "job_id", "name", "status", "attempt", "max_retries", "distribution_key",
				"params", "result", "created_at", "updated_at", "started_at", "finished_at", "duration_ms", "sequence_number",
			}).AddRow(taskID, jobID, "t1", domain.TaskStatusCompleted, 1, 3, nil, nil, nil, now, now, nil, nil, nil, 0))

		req := httptest.NewRequest(http.MethodGet, "/api/jobs/"+jobID.String()+"/tasks", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), taskID.String())
	})
}

func TestConsoleHandlerStatsEndpoints(t *testing.T) {
	store, mock := newMockConsoleStore(t)
	svc := &mockScheduler{
		db: store.db,
		jobs: map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", Schedule: "0 * * * *"},
		},
	}
	handler := NewHandler(svc)

	t.Run("stats jobs", func(t *testing.T) {
		mock.ExpectQuery(`SELECT name, status, count\(\*\)`).
			WillReturnRows(sqlmock.NewRows([]string{"name", "status", "count"}).
				AddRow("job-a", domain.JobStatusCompleted, 1))

		req := httptest.NewRequest(http.MethodGet, "/api/stats/jobs", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("stats timeseries", func(t *testing.T) {
		mock.ExpectQuery(`SELECT date_trunc`).
			WillReturnRows(sqlmock.NewRows([]string{"t", "succeeded", "failed", "other"}).
				AddRow(time.Now(), 1, 0, 0))

		req := httptest.NewRequest(http.MethodGet, "/api/stats/timeseries?job=job-a", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("stats job metrics", func(t *testing.T) {
		mock.ExpectQuery(`SELECT name,`).
			WillReturnRows(sqlmock.NewRows([]string{"name", "success", "error", "total", "avg_ms"}).
				AddRow("job-a", 2, 1, 3, 10.0))

		req := httptest.NewRequest(http.MethodGet, "/api/stats/job-metrics", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"error_pct"`)
	})
}

func TestConsoleHandlerListJobsError(t *testing.T) {
	store, mock := newMockConsoleStore(t)
	svc := &mockScheduler{db: store.db, jobs: map[string]jobconfig.JobConfig{}}
	handler := NewHandler(svc)

	mock.ExpectQuery(`SELECT(.|\n)*FROM scherry_jobs j`).
		WillReturnError(errors.New("db down"))

	req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestConsoleHandlerAsynqWithRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	opt := asynq.RedisClientOpt{Addr: mr.Addr()}
	taskID := archiveHandlerAsynqTask(t, opt, "payroll")

	store, sqlMock := newMockConsoleStore(t)
	svc := &redisBackedScheduler{
		mockScheduler: mockScheduler{
			repo: &testutil.MockRepository{},
			db:   store.db,
			jobs: map[string]jobconfig.JobConfig{"payroll": {Name: "payroll"}},
		},
		redisOpt: opt,
	}
	handler := NewHandler(svc)

	t.Run("job detail", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/jobs/asynq:"+taskID, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"source":"asynq"`)
	})

	t.Run("list tasks returns empty page", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/jobs/asynq:"+taskID+"/tasks", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"total":0`)
	})

	t.Run("list jobs merges postgres and asynq", func(t *testing.T) {
		sqlMock.ExpectQuery(`SELECT DISTINCT unique_reference_id FROM "scherry_jobs"`).
			WillReturnRows(sqlmock.NewRows([]string{"unique_reference_id"}))
		sqlMock.ExpectQuery(`SELECT(.|\n)*FROM scherry_jobs j`).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "parent_job_id", "name", "status", "unique_reference_id",
				"created_at", "updated_at", "started_at", "finished_at", "duration_ms",
			}))

		req := httptest.NewRequest(http.MethodGet, "/api/jobs?limit=10", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"source":"asynq"`)
	})

	t.Run("retry archived trigger", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/jobs/asynq:"+taskID+"/retry", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), `"retry_method":"asynq"`)
	})
}

func archiveHandlerAsynqTask(t *testing.T, opt asynq.RedisClientOpt, taskType string) string {
	t.Helper()
	processed := make(chan struct{}, 1)
	srv := asynq.NewServer(opt, asynq.Config{Concurrency: 1, LogLevel: asynq.FatalLevel})
	mux := asynq.NewServeMux()
	mux.HandleFunc(taskType, func(context.Context, *asynq.Task) error {
		processed <- struct{}{}
		return errors.New("trigger failed")
	})
	go func() { _ = srv.Run(mux) }()
	t.Cleanup(func() { srv.Shutdown() })

	client := asynq.NewClient(opt)
	t.Cleanup(func() { _ = client.Close() })
	info, err := client.Enqueue(asynq.NewTask(taskType, nil), asynq.MaxRetry(0))
	require.NoError(t, err)

	select {
	case <-processed:
	case <-time.After(3 * time.Second):
		t.Fatal("task was not processed")
	}

	inspector := asynq.NewInspector(opt)
	t.Cleanup(func() { _ = inspector.Close() })
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ti, err := inspector.GetTaskInfo("default", info.ID)
		if err == nil && ti.State == asynq.TaskStateArchived {
			return info.ID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("task never archived")
	return ""
}

func TestConsoleHandlerStatsErrors(t *testing.T) {
	store, mock := newMockConsoleStore(t)
	svc := &mockScheduler{
		db: store.db,
		jobs: map[string]jobconfig.JobConfig{
			"job-a": {Name: "job-a", Schedule: "0 * * * *"},
		},
	}
	handler := NewHandler(svc)

	t.Run("stats jobs db error", func(t *testing.T) {
		mock.ExpectQuery(`SELECT name, status, count\(\*\)`).
			WillReturnError(errors.New("db down"))
		req := httptest.NewRequest(http.MethodGet, "/api/stats/jobs", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})

	t.Run("stats timeseries db error", func(t *testing.T) {
		mock.ExpectQuery(`SELECT date_trunc`).
			WillReturnError(errors.New("db down"))
		req := httptest.NewRequest(http.MethodGet, "/api/stats/timeseries?job=job-a", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})

	t.Run("stats job metrics db error", func(t *testing.T) {
		mock.ExpectQuery(`SELECT name,`).
			WillReturnError(errors.New("db down"))
		req := httptest.NewRequest(http.MethodGet, "/api/stats/job-metrics", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}
