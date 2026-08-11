package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/looplab/fsm"
)

type Job struct {
	ID                uuid.UUID    `gorm:"primaryKey;column:id;type:uuid"`
	ParentJobID       *uuid.UUID   `gorm:"column:parent_job_id;type:uuid"`
	Name              string       `gorm:"column:name;not null"`
	Status            string       `gorm:"column:status;not null;default:'PENDING'"`
	MaxRetries        int          `gorm:"column:max_retries;default:3"`
	UniqueReferenceID *string      `gorm:"column:unique_reference_id;type:varchar(255)"`
	CreatedAt         time.Time    `gorm:"primaryKey;column:created_at;default:CURRENT_TIMESTAMP"`
	UpdatedAt         time.Time    `gorm:"column:updated_at;default:CURRENT_TIMESTAMP"`
	StartedAt         *time.Time   `gorm:"column:started_at"`
	FinishedAt        *time.Time   `gorm:"column:finished_at"`
	DurationMs        *int64       `gorm:"column:duration_ms"`
	Metadata          pgtype.JSONB `gorm:"column:metadata;type:jsonb"`
}

func (j *Job) TableName() string {
	return "scherry_jobs"
}

func (j *Job) stateMachine() *fsm.FSM {
	return newJobStateMachine(j)
}

func (j *Job) UpdateStatus(status string) {
	j.Status = status
	j.UpdatedAt = time.Now()
}

func (j *Job) Start(ctx context.Context) error {
	return j.stateMachine().Event(ctx, JobActionStart)
}

func (j *Job) Complete(ctx context.Context) error {
	return j.stateMachine().Event(ctx, JobActionComplete)
}

func (j *Job) Fail(ctx context.Context) error {
	return j.stateMachine().Event(ctx, JobActionFail)
}

func (j *Job) PartialFail(ctx context.Context) error {
	return j.stateMachine().Event(ctx, JobActionPartialFail)
}

func (j *Job) Cancel(ctx context.Context) error {
	return j.stateMachine().Event(ctx, JobActionCancel)
}

func (j *Job) IsExecutionPending() bool {
	return slices.Contains(NonTerminalJobStates, j.Status)
}

func (j *Job) IsPending() bool {
	return j.Status == JobStatusPending
}

func (j *Job) IsRunning() bool {
	return j.Status == JobStatusRunning
}

func (j *Job) IsRejected() bool {
	return j.Status == JobStatusRejected
}

func (j *Job) IsCancelled() bool {
	return j.Status == JobStatusCancelled
}

func (j *Job) SetMetadata(metadata map[string]interface{}) error {
	if err := j.Metadata.Set(metadata); err != nil {
		return fmt.Errorf("set job metadata: %w", err)
	}
	return nil
}

func (j *Job) MetadataMap() (map[string]interface{}, error) {
	if j.Metadata.Status != pgtype.Present || len(j.Metadata.Bytes) == 0 {
		return nil, nil
	}
	var meta map[string]interface{}
	if err := json.Unmarshal(j.Metadata.Bytes, &meta); err != nil {
		return nil, fmt.Errorf("decode job metadata: %w", err)
	}
	return meta, nil
}
