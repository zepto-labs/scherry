// Package testutil holds test doubles shared across internal/executor,
// internal/scheduler, and internal/console's test suites, so each package
// does not need to redeclare the same mocks. It must not be imported by any
// non-test code.
package testutil

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/mock"
	"gorm.io/gorm"

	"github.com/zepto-labs/scherry/internal/domain"
	"github.com/zepto-labs/scherry/internal/jobconfig"
	"github.com/zepto-labs/scherry/internal/repository"
)

// Compile-time assertions that each mock satisfies the interface it doubles.
var (
	_ repository.Repository  = (*MockRepository)(nil)
	_ jobconfig.JobExecutor  = (*MockJobExecutor)(nil)
	_ jobconfig.TaskExecutor = (*MockTaskExecutor)(nil)
	_ jobconfig.Publisher    = (*MockPublisher)(nil)
)

// MockRepository is a testify-mock implementation of repository.Repository.
type MockRepository struct {
	mock.Mock
}

func (m *MockRepository) FindJobByID(ctx context.Context, jobID uuid.UUID) (*domain.Job, error) {
	args := m.Called(ctx, jobID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.Job), args.Error(1)
}

func (m *MockRepository) FindTaskWithJob(ctx context.Context, taskID uuid.UUID) (*domain.Task, *domain.Job, error) {
	args := m.Called(ctx, taskID)
	return args.Get(0).(*domain.Task), args.Get(1).(*domain.Job), args.Error(2)
}

func (m *MockRepository) FindPendingJobByParentID(ctx context.Context, parentJobID uuid.UUID) (*domain.Job, error) {
	args := m.Called(ctx, parentJobID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.Job), args.Error(1)
}

func (m *MockRepository) FindJobWithTasksByUniqueReferenceID(ctx context.Context, uniqueReferenceID string) (*domain.Job, []domain.Task, error) {
	args := m.Called(ctx, uniqueReferenceID)
	var job *domain.Job
	if args.Get(0) != nil {
		job = args.Get(0).(*domain.Job)
	}
	return job, args.Get(1).([]domain.Task), args.Error(2)
}

func (m *MockRepository) FindTasksByJobIDAndStatuses(ctx context.Context, jobID uuid.UUID, statuses []string) ([]domain.Task, error) {
	args := m.Called(ctx, jobID, statuses)
	return args.Get(0).([]domain.Task), args.Error(1)
}

func (m *MockRepository) FindTasksByJobIDAndStatusesBatch(ctx context.Context, jobID uuid.UUID, statuses []string, afterSeq, limit int) ([]domain.Task, error) {
	args := m.Called(ctx, jobID, statuses, afterSeq, limit)
	return args.Get(0).([]domain.Task), args.Error(1)
}

func (m *MockRepository) GetTaskStatusSummary(ctx context.Context, jobID uuid.UUID) (repository.TaskStatusSummary, error) {
	args := m.Called(ctx, jobID)
	return args.Get(0).(repository.TaskStatusSummary), args.Error(1)
}

func (m *MockRepository) CreateTasks(ctx context.Context, tasks []domain.Task) error {
	args := m.Called(ctx, tasks)
	return args.Error(0)
}

func (m *MockRepository) UpdateTasksStatus(ctx context.Context, taskIDs []uuid.UUID, status string) error {
	args := m.Called(ctx, taskIDs, status)
	return args.Error(0)
}

func (m *MockRepository) UpdateTasksStatusByJobID(ctx context.Context, jobID uuid.UUID, newStatus string) error {
	args := m.Called(ctx, jobID, newStatus)
	return args.Error(0)
}

func (m *MockRepository) CancelPreviousJobs(ctx context.Context, jobName string, currentJobID uuid.UUID) error {
	args := m.Called(ctx, jobName, currentJobID)
	return args.Error(0)
}

func (m *MockRepository) TerminateJob(ctx context.Context, jobID uuid.UUID) error {
	args := m.Called(ctx, jobID)
	return args.Error(0)
}

func (m *MockRepository) UpdateJobStatus(ctx context.Context, jobID uuid.UUID, status string) error {
	args := m.Called(ctx, jobID, status)
	return args.Error(0)
}

func (m *MockRepository) UpdateJob(ctx context.Context, jobID uuid.UUID, updates map[string]interface{}) error {
	args := m.Called(ctx, jobID, updates)
	return args.Error(0)
}

func (m *MockRepository) UpdateJobIfStatus(ctx context.Context, jobID uuid.UUID, expectedStatus string, updates map[string]interface{}) (bool, error) {
	args := m.Called(ctx, jobID, expectedStatus, updates)
	return args.Bool(0), args.Error(1)
}

func (m *MockRepository) UpdateTask(ctx context.Context, taskID uuid.UUID, updates map[string]interface{}) error {
	args := m.Called(ctx, taskID, updates)
	return args.Error(0)
}

func (m *MockRepository) FindCompletableJobIDs(ctx context.Context, jobName string, limit int) ([]uuid.UUID, error) {
	args := m.Called(ctx, jobName, limit)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]uuid.UUID), args.Error(1)
}

func (m *MockRepository) CreateJobAndTasks(ctx context.Context, job *domain.Job, tasks []domain.Task) (*domain.Job, []domain.Task, error) {
	args := m.Called(ctx, job, tasks)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}
	return args.Get(0).(*domain.Job), args.Get(1).([]domain.Task), args.Error(2)
}

// DB has no backing connection for the mock; it is not part of any assertion
// so it returns nil directly rather than going through mock.Called().
func (m *MockRepository) DB() *gorm.DB {
	return nil
}

// MockJobExecutor is a testify-mock implementation of jobconfig.JobExecutor.
type MockJobExecutor struct{ mock.Mock }

func (m *MockJobExecutor) Execute(ctx context.Context, metadata map[string]interface{}) ([]jobconfig.TaskData, error) {
	args := m.Called(ctx, metadata)
	return args.Get(0).([]jobconfig.TaskData), args.Error(1)
}

// MockTaskExecutor is a testify-mock implementation of jobconfig.TaskExecutor.
type MockTaskExecutor struct{ mock.Mock }

func (m *MockTaskExecutor) Execute(ctx context.Context, params map[string]interface{}) (map[string]interface{}, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]interface{}), args.Error(1)
}

// MockPublisher is a testify-mock implementation of jobconfig.Publisher.
type MockPublisher struct{ mock.Mock }

func (m *MockPublisher) PublishTasks(ctx context.Context, tasks []domain.Task, topic string) error {
	args := m.Called(ctx, tasks, topic)
	return args.Error(0)
}

func (m *MockPublisher) PublishRetryTasks(ctx context.Context, tasks []domain.Task, topic string, retryDelay time.Duration) error {
	args := m.Called(ctx, tasks, topic, retryDelay)
	return args.Error(0)
}

// StringPtr returns a pointer to v; a small convenience shared by tests that
// need a *string (e.g. Job.UniqueReferenceID).
func StringPtr(v string) *string { return &v }

// NewTestTaskPair builds a PENDING task belonging to a RUNNING job, both with
// valid JSONB fields, for use as a starting fixture in execution/transport
// tests across packages.
func NewTestTaskPair() (*domain.Task, *domain.Job) {
	now := time.Now()
	var params, result pgtype.JSONB
	_ = params.Set(map[string]interface{}{"key": "value"})
	_ = result.Set(map[string]interface{}{})

	jobID := uuid.New()
	task := &domain.Task{
		ID: uuid.New(), JobID: jobID, Name: "task1", Status: domain.TaskStatusPending,
		Attempt: 1, MaxRetries: 3, Params: params, Result: result,
		CreatedAt: now, UpdatedAt: now,
	}
	job := &domain.Job{ID: jobID, Name: "test-job", Status: domain.JobStatusRunning}
	return task, job
}

// MustMarshal JSON-encodes v, panicking on error. For use in tests only.
func MustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
