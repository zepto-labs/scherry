package repository

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/zepto-labs/scherry/internal/domain"
)

// sliceAwareValueConverter extends the default driver value conversion with
// support for []uuid.UUID, which CancelPreviousJobs passes directly as a
// query argument for a Postgres `= ANY($n)` clause. A real pgx connection
// knows how to encode this natively; go-sqlmock's plain database/sql driver
// does not, so this test-only converter fills that gap.
type sliceAwareValueConverter struct{}

func (sliceAwareValueConverter) ConvertValue(v interface{}) (driver.Value, error) {
	if ids, ok := v.([]uuid.UUID); ok {
		strs := make([]string, len(ids))
		for i, id := range ids {
			strs[i] = id.String()
		}
		return fmt.Sprintf("{%s}", strings.Join(strs, ",")), nil
	}
	return driver.DefaultParameterConverter.ConvertValue(v)
}

// newMockRepo builds a *repository backed by go-sqlmock so its GORM queries
// can be asserted against a real (mocked) database/sql driver without ever
// touching a live Postgres server.
func newMockRepo(t *testing.T) (*repository, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.ValueConverterOption(sliceAwareValueConverter{}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	require.NoError(t, err)

	return &repository{db: db}, mock
}

func TestNewRepository(t *testing.T) {
	repo := NewRepository(&gorm.DB{})
	assert.NotNil(t, repo)
	_, ok := repo.(*repository)
	assert.True(t, ok)
}

func TestRepositoryFindJobByID(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		jobID := uuid.New()
		now := time.Now()

		rows := sqlmock.NewRows([]string{"id", "name", "status", "created_at"}).
			AddRow(jobID, "job-a", domain.JobStatusRunning, now)
		mock.ExpectQuery(`SELECT \* FROM "scherry_jobs" WHERE id = \$1`).
			WithArgs(jobID, 1).
			WillReturnRows(rows)

		job, err := repo.FindJobByID(context.Background(), jobID)
		require.NoError(t, err)
		assert.Equal(t, jobID, job.ID)
		assert.Equal(t, "job-a", job.Name)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("not found", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		jobID := uuid.New()

		mock.ExpectQuery(`SELECT \* FROM "scherry_jobs" WHERE id = \$1`).
			WithArgs(jobID, 1).
			WillReturnRows(sqlmock.NewRows([]string{"id"}))

		job, err := repo.FindJobByID(context.Background(), jobID)
		assert.Nil(t, job)
		assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
	})
}

func TestRepositoryUpdateJobStatus(t *testing.T) {
	repo, mock := newMockRepo(t)
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE "scherry_jobs" SET`).
		WithArgs(domain.JobStatusCompleted, sqlmock.AnyArg(), jobID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.UpdateJobStatus(context.Background(), jobID, domain.JobStatusCompleted)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRepositoryUpdateJob(t *testing.T) {
	t.Run("empty updates is a no-op", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		err := repo.UpdateJob(context.Background(), uuid.New(), nil)
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("with updates issues an UPDATE", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		jobID := uuid.New()

		mock.ExpectBegin()
		mock.ExpectExec(`UPDATE "scherry_jobs" SET`).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err := repo.UpdateJob(context.Background(), jobID, map[string]interface{}{"status": domain.JobStatusFailed})
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestRepositoryFindCompletableJobIDs(t *testing.T) {
	t.Run("returns matching job ids", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		jobID := uuid.New()

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT j.id
		FROM scherry_jobs j
		WHERE j.name = $1 AND j.status = $2`)).
			WithArgs("test-job", domain.JobStatusRunning,
				domain.TaskStatusCompleted, domain.TaskStatusFailed, domain.TaskStatusMaxRetriesExhausted, domain.TaskStatusRejected, domain.TaskStatusCancelled,
				defaultCompletionCheckBatchSize).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(jobID))

		ids, err := repo.FindCompletableJobIDs(context.Background(), "test-job", 0)
		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{jobID}, ids)
	})

	t.Run("no completable jobs returns empty slice", func(t *testing.T) {
		repo, mock := newMockRepo(t)

		mock.ExpectQuery(regexp.QuoteMeta(`SELECT j.id
		FROM scherry_jobs j
		WHERE j.name = $1 AND j.status = $2`)).
			WillReturnRows(sqlmock.NewRows([]string{"id"}))

		ids, err := repo.FindCompletableJobIDs(context.Background(), "test-job", 50)
		require.NoError(t, err)
		assert.Empty(t, ids)
	})
}

func TestRepositoryUpdateTask(t *testing.T) {
	t.Run("empty updates is a no-op", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		err := repo.UpdateTask(context.Background(), uuid.New(), nil)
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("with updates issues an UPDATE", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		taskID := uuid.New()

		mock.ExpectBegin()
		mock.ExpectExec(`UPDATE "scherry_tasks" SET`).
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		err := repo.UpdateTask(context.Background(), taskID, map[string]interface{}{"status": domain.TaskStatusCompleted})
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestRepositoryUpdateTasksStatus(t *testing.T) {
	repo, mock := newMockRepo(t)
	ids := []uuid.UUID{uuid.New(), uuid.New()}

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE "scherry_tasks" SET`).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	err := repo.UpdateTasksStatus(context.Background(), ids, domain.TaskStatusCancelled)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRepositoryUpdateTasksStatusByJobID(t *testing.T) {
	repo, mock := newMockRepo(t)
	jobID := uuid.New()

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE "scherry_tasks" SET`).
		WillReturnResult(sqlmock.NewResult(0, 5))
	mock.ExpectCommit()

	err := repo.UpdateTasksStatusByJobID(context.Background(), jobID, domain.TaskStatusRejected)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRepositoryFindPendingJobByParentID(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		parentID := uuid.New()
		childID := uuid.New()

		mock.ExpectQuery(`SELECT \* FROM "scherry_jobs" WHERE`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "parent_job_id", "status"}).
				AddRow(childID, parentID, domain.JobStatusPending))

		job, err := repo.FindPendingJobByParentID(context.Background(), parentID)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, childID, job.ID)
	})

	t.Run("not found returns nil, nil", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		parentID := uuid.New()

		mock.ExpectQuery(`SELECT \* FROM "scherry_jobs" WHERE`).
			WillReturnRows(sqlmock.NewRows([]string{"id"}))

		job, err := repo.FindPendingJobByParentID(context.Background(), parentID)
		assert.NoError(t, err)
		assert.Nil(t, job)
	})
}

func TestRepositoryCreateTasksEmptyIsNoOp(t *testing.T) {
	repo, mock := newMockRepo(t)
	err := repo.CreateTasks(context.Background(), nil)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRepositoryFindTasksByJobIDAndStatuses(t *testing.T) {
	repo, mock := newMockRepo(t)
	jobID := uuid.New()

	t.Run("no status filter", func(t *testing.T) {
		mock.ExpectQuery(`SELECT \* FROM "scherry_tasks" WHERE job_id = \$1`).
			WithArgs(jobID).
			WillReturnRows(sqlmock.NewRows([]string{"id", "job_id"}).AddRow(uuid.New(), jobID))

		tasks, err := repo.FindTasksByJobIDAndStatuses(context.Background(), jobID, nil)
		require.NoError(t, err)
		assert.Len(t, tasks, 1)
	})

	t.Run("with status filter", func(t *testing.T) {
		mock.ExpectQuery(`SELECT \* FROM "scherry_tasks" WHERE job_id = \$1 AND status IN`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "job_id"}))

		tasks, err := repo.FindTasksByJobIDAndStatuses(context.Background(), jobID, []string{domain.TaskStatusPending})
		require.NoError(t, err)
		assert.Empty(t, tasks)
	})
}

func TestRepositoryFindTasksByJobIDAndStatusesBatch(t *testing.T) {
	repo, mock := newMockRepo(t)
	jobID := uuid.New()

	mock.ExpectQuery(`SELECT \* FROM "scherry_tasks" WHERE job_id = \$1 AND sequence_number > \$2`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "job_id", "sequence_number"}).
			AddRow(uuid.New(), jobID, 0))

	tasks, err := repo.FindTasksByJobIDAndStatusesBatch(context.Background(), jobID, nil, -1, 100)
	require.NoError(t, err)
	assert.Len(t, tasks, 1)
}

func TestRepositoryFindJobWithTasksByUniqueReferenceID(t *testing.T) {
	t.Run("no matching job", func(t *testing.T) {
		repo, mock := newMockRepo(t)

		mock.ExpectQuery(`SELECT \* FROM "scherry_jobs" WHERE unique_reference_id = \$1`).
			WillReturnRows(sqlmock.NewRows([]string{"id"}))

		job, tasks, err := repo.FindJobWithTasksByUniqueReferenceID(context.Background(), "ref-1")
		assert.NoError(t, err)
		assert.Nil(t, job)
		assert.Nil(t, tasks)
	})

	t.Run("matching job loads its tasks", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		jobID := uuid.New()

		mock.ExpectQuery(`SELECT \* FROM "scherry_jobs" WHERE unique_reference_id = \$1`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "unique_reference_id"}).
				AddRow(jobID, "ref-1"))
		mock.ExpectQuery(`SELECT \* FROM "scherry_tasks" WHERE job_id = \$1`).
			WithArgs(jobID).
			WillReturnRows(sqlmock.NewRows([]string{"id", "job_id"}).AddRow(uuid.New(), jobID))

		job, tasks, err := repo.FindJobWithTasksByUniqueReferenceID(context.Background(), "ref-1")
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, jobID, job.ID)
		assert.Len(t, tasks, 1)
	})
}

func TestRepositoryFindTaskWithJob(t *testing.T) {
	t.Run("query error", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		taskID := uuid.New()

		mock.ExpectQuery(`SELECT \* FROM "scherry_tasks" WHERE id = \$1`).
			WillReturnError(errors.New("boom"))

		task, job, err := repo.FindTaskWithJob(context.Background(), taskID)
		assert.Nil(t, task)
		assert.Nil(t, job)
		assert.Error(t, err)
	})

	t.Run("found with job", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		taskID := uuid.New()
		jobID := uuid.New()
		now := time.Now()

		mock.ExpectQuery(`SELECT \* FROM "scherry_tasks" WHERE id = \$1`).
			WithArgs(taskID, 1).
			WillReturnRows(sqlmock.NewRows([]string{"id", "job_id", "name", "status", "created_at"}).
				AddRow(taskID, jobID, "task-a", domain.TaskStatusPending, now))
		mock.ExpectQuery(`SELECT \* FROM "scherry_jobs" WHERE`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "status", "created_at"}).
				AddRow(jobID, "job-a", domain.JobStatusRunning, now))

		task, job, err := repo.FindTaskWithJob(context.Background(), taskID)
		require.NoError(t, err)
		require.NotNil(t, task)
		require.NotNil(t, job)
		assert.Equal(t, taskID, task.ID)
		assert.Equal(t, jobID, job.ID)
	})

	t.Run("missing associated job", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		taskID := uuid.New()
		jobID := uuid.New()
		now := time.Now()

		mock.ExpectQuery(`SELECT \* FROM "scherry_tasks" WHERE id = \$1`).
			WithArgs(taskID, 1).
			WillReturnRows(sqlmock.NewRows([]string{"id", "job_id", "name", "status", "created_at"}).
				AddRow(taskID, jobID, "task-a", domain.TaskStatusPending, now))
		mock.ExpectQuery(`SELECT \* FROM "scherry_jobs" WHERE`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "status", "created_at"}))

		task, job, err := repo.FindTaskWithJob(context.Background(), taskID)
		assert.Nil(t, task)
		assert.Nil(t, job)
		assert.ErrorContains(t, err, "job not found for task")
	})
}

func TestRepositoryCreateTasks(t *testing.T) {
	repo, mock := newMockRepo(t)
	jobID := uuid.New()
	task := domain.Task{ID: uuid.New(), JobID: jobID, Name: "t1", Status: domain.TaskStatusPending}
	require.NoError(t, task.SetParams(map[string]interface{}{}))
	require.NoError(t, task.SetResult(map[string]interface{}{}))

	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO "scherry_tasks"`).
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(time.Now()))
	mock.ExpectCommit()

	err := repo.CreateTasks(context.Background(), []domain.Task{task})
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRepositoryDB(t *testing.T) {
	db := &gorm.DB{}
	repo := NewRepository(db)
	assert.Equal(t, db, repo.DB())
}

func TestRepositoryTerminateJob(t *testing.T) {
	t.Run("terminates a pending or running job", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		jobID := uuid.New()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(`UPDATE scherry_jobs SET status = $1, updated_at = NOW()`)).
			WithArgs(domain.JobStatusCancelled, jobID, domain.JobStatusPending, domain.JobStatusRunning).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(jobID))
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE scherry_tasks SET status = $1, updated_at = NOW()`)).
			WithArgs(domain.TaskStatusCancelled, jobID, domain.TaskStatusPending).
			WillReturnResult(sqlmock.NewResult(0, 3))
		mock.ExpectCommit()

		err := repo.TerminateJob(context.Background(), jobID)
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("already terminal job returns ErrJobAlreadyTerminal", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		jobID := uuid.New()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(`UPDATE scherry_jobs SET status = $1, updated_at = NOW()`)).
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
		mock.ExpectRollback()

		err := repo.TerminateJob(context.Background(), jobID)
		assert.ErrorIs(t, err, ErrJobAlreadyTerminal)
	})
}

func TestRepositoryCancelPreviousJobs(t *testing.T) {
	t.Run("no other active jobs is a no-op after the first query", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		currentID := uuid.New()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(`UPDATE scherry_jobs SET status = $1, updated_at = NOW()`)).
			WillReturnRows(sqlmock.NewRows([]string{"id"}))
		mock.ExpectCommit()

		err := repo.CancelPreviousJobs(context.Background(), "job-a", currentID)
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("cancels previous jobs and their tasks", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		currentID := uuid.New()
		cancelledID := uuid.New()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta(`UPDATE scherry_jobs SET status = $1, updated_at = NOW()`)).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(cancelledID))
		mock.ExpectExec(regexp.QuoteMeta(`UPDATE scherry_tasks SET status = $1, updated_at = NOW()`)).
			WillReturnResult(sqlmock.NewResult(0, 2))
		mock.ExpectCommit()

		err := repo.CancelPreviousJobs(context.Background(), "job-a", currentID)
		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestRepositoryGetTaskStatusSummary(t *testing.T) {
	repo, mock := newMockRepo(t)
	jobID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT
			COUNT(*) AS total`)).
		WillReturnRows(sqlmock.NewRows([]string{"total", "completed", "non_terminal"}).
			AddRow(5, 3, 2))

	summary, err := repo.GetTaskStatusSummary(context.Background(), jobID)
	require.NoError(t, err)
	assert.Equal(t, TaskStatusSummary{Total: 5, Completed: 3, NonTerminal: 2}, summary)
}

func TestRepositoryCreateJobAndTasks(t *testing.T) {
	t.Run("job only", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		job := &domain.Job{ID: uuid.New(), Name: "job-a", Status: domain.JobStatusPending}
		// Metadata is a pgtype.JSONB; its zero value is "undefined" and fails
		// to encode as a driver value, so real callers always set it first
		// (see prepareJobAndTasks). Mirror that here.
		require.NoError(t, job.SetMetadata(map[string]interface{}{}))

		mock.ExpectBegin()
		mock.ExpectQuery(`INSERT INTO "scherry_jobs"`).
			WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(time.Now()))
		mock.ExpectCommit()

		createdJob, createdTasks, err := repo.CreateJobAndTasks(context.Background(), job, nil)
		require.NoError(t, err)
		assert.Equal(t, job, createdJob)
		assert.Empty(t, createdTasks)
	})

	t.Run("job with tasks", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		job := &domain.Job{ID: uuid.New(), Name: "job-a", Status: domain.JobStatusPending}
		require.NoError(t, job.SetMetadata(map[string]interface{}{}))

		task := domain.Task{ID: uuid.New(), JobID: job.ID, Name: "t1", Status: domain.TaskStatusPending}
		require.NoError(t, task.SetParams(map[string]interface{}{}))
		require.NoError(t, task.SetResult(map[string]interface{}{}))
		tasks := []domain.Task{task}

		mock.ExpectBegin()
		mock.ExpectQuery(`INSERT INTO "scherry_jobs"`).
			WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(time.Now()))
		mock.ExpectQuery(`INSERT INTO "scherry_tasks"`).
			WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(time.Now()))
		mock.ExpectCommit()

		createdJob, createdTasks, err := repo.CreateJobAndTasks(context.Background(), job, tasks)
		require.NoError(t, err)
		assert.Equal(t, job, createdJob)
		assert.Len(t, createdTasks, 1)
	})

	t.Run("rolls back on failure", func(t *testing.T) {
		repo, mock := newMockRepo(t)
		job := &domain.Job{ID: uuid.New(), Name: "job-a", Status: domain.JobStatusPending}
		require.NoError(t, job.SetMetadata(map[string]interface{}{}))

		mock.ExpectBegin()
		mock.ExpectQuery(`INSERT INTO "scherry_jobs"`).WillReturnError(errors.New("insert failed"))
		mock.ExpectRollback()

		_, _, err := repo.CreateJobAndTasks(context.Background(), job, nil)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "create job")
	})
}
