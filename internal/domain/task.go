package domain

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/looplab/fsm"
)

type Task struct {
	ID              uuid.UUID    `gorm:"primaryKey;column:id;type:uuid" json:"id"`
	JobID           uuid.UUID    `gorm:"column:job_id;type:uuid;not null" json:"job_id"`
	Name            string       `gorm:"column:name;type:text;not null" json:"name"`
	Status          string       `gorm:"column:status;type:text;not null" json:"status"`
	Attempt         int          `gorm:"column:attempt;default:0" json:"attempt"`
	MaxRetries      int          `gorm:"column:max_retries;default:3" json:"max_retries"`
	// SequenceNumber is the 0-based position of this task in the slice returned
	// by JobExecutor.Execute. It is the authoritative ordering key because all
	// tasks belonging to the same job are inserted in a single batch and
	// therefore share an identical created_at timestamp.
	SequenceNumber  int          `gorm:"column:sequence_number;default:0" json:"sequence_number"`
	DistributionKey *string      `gorm:"column:distribution_key;type:text" json:"distribution_key,omitempty"`
	Params          pgtype.JSONB `gorm:"column:params;type:jsonb" json:"params"`
	Result          pgtype.JSONB `gorm:"column:result;type:jsonb" json:"result"`
	CreatedAt  time.Time    `gorm:"primaryKey;column:created_at;default:now()" json:"created_at"`
	UpdatedAt  time.Time    `gorm:"column:updated_at;default:now()" json:"updated_at"`
	StartedAt  *time.Time   `gorm:"column:started_at" json:"started_at,omitempty"`
	FinishedAt *time.Time   `gorm:"column:finished_at" json:"finished_at,omitempty"`
	DurationMs *int64       `gorm:"column:duration_ms" json:"duration_ms,omitempty"`
	Job        *Job         `gorm:"foreignKey:JobID,CreatedAt;references:ID,CreatedAt" json:"job,omitempty"`
}

func (t *Task) TableName() string {
	return "scherry_tasks"
}

func (t *Task) stateMachine() *fsm.FSM {
	return newTaskStateMachine(t)
}

func (t *Task) UpdateStatus(status string) {
	t.Status = status
	t.UpdatedAt = time.Now()
}

// IsTerminal reports whether the task has reached a settled state from which no
// further execution is expected. A FAILED task is not terminal because it may
// still be retried.
func (t *Task) IsTerminal() bool {
	return slices.Contains(TerminalTaskStates, t.Status)
}

func (t *Task) Start(ctx context.Context) error {
	return t.stateMachine().Event(ctx, TaskActionStart)
}

func (t *Task) Complete(ctx context.Context) error {
	return t.stateMachine().Event(ctx, TaskActionComplete)
}

func (t *Task) Retry(ctx context.Context) error {
	return t.stateMachine().Event(ctx, TaskActionRetry)
}

func (t *Task) ExhaustRetries(ctx context.Context) error {
	return t.stateMachine().Event(ctx, TaskActionExhaustRetries)
}

func (t *Task) GetParams() (map[string]interface{}, error) {
	if t.Params.Status == pgtype.Null {
		return map[string]interface{}{}, nil
	}
	var params map[string]interface{}
	if err := t.Params.AssignTo(&params); err != nil {
		return nil, fmt.Errorf("unmarshal task params: %w", err)
	}
	return params, nil
}

func (t *Task) SetParams(params map[string]interface{}) error {
	if err := t.Params.Set(params); err != nil {
		return fmt.Errorf("set task params: %w", err)
	}
	return nil
}

func (t *Task) SetResult(result map[string]interface{}) error {
	if err := t.Result.Set(result); err != nil {
		return fmt.Errorf("set task result: %w", err)
	}
	return nil
}
