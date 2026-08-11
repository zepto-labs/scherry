package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/zepto-labs/scherry"
)

const bonusTaskName = "bonus_payout_task"

// bonusBatchJobExecutor splits all employees into batches and stamps the bonus
// percentage (received from the HTTP trigger via metadata) into every task's
// params so the task executor can apply it independently.
type bonusBatchJobExecutor struct {
	store     *employeeStore
	batchSize int
	logger    scherry.Logger
}

func (e *bonusBatchJobExecutor) Execute(ctx context.Context, metadata map[string]interface{}) ([]scherry.TaskData, error) {
	bonusPct, ok := metadata["bonus_pct"].(float64)
	if !ok || bonusPct <= 0 {
		return nil, fmt.Errorf("bonus_pct must be a positive number, got: %v", metadata["bonus_pct"])
	}

	employees, err := e.store.ListEmployees(ctx)
	if err != nil {
		return nil, err
	}
	if len(employees) == 0 {
		e.logger.Warn("no employees found")
		return nil, nil
	}

	batches := chunkEmployees(employees, e.batchSize)
	e.logger.Info("created bonus batches", "count", len(batches), "bonus_pct", bonusPct)

	tasks := make([]scherry.TaskData, 0, len(batches))
	for i, batch := range batches {
		ids := make([]string, len(batch))
		for j, emp := range batch {
			ids[j] = emp.ID
		}
		payload := bonusTaskPayload{
			EmployeeIDs:  ids,
			BonusPct:     bonusPct,
			BatchNumber:  i + 1,
			TotalBatches: len(batches),
		}
		params, err := payload.toMap()
		if err != nil {
			return nil, fmt.Errorf("marshal batch %d: %w", i+1, err)
		}
		tasks = append(tasks, scherry.TaskData{
			Name:            bonusTaskName,
			Params:          params,
			DistributionKey: strings.Join(ids, "_"),
		})
	}
	return tasks, nil
}

// bonusPayoutTaskExecutor processes one employee batch: reads bonus_pct from
// params and records a simulated bonus transfer per employee.
type bonusPayoutTaskExecutor struct {
	store  *employeeStore
	logger scherry.Logger
}

func (e *bonusPayoutTaskExecutor) Execute(ctx context.Context, params map[string]interface{}) (map[string]interface{}, error) {
	payload, err := parseBonusTaskPayload(params)
	if err != nil {
		return nil, err
	}
	e.logger.Info("processing bonus batch",
		"batch", payload.BatchNumber, "total", payload.TotalBatches,
		"employees", len(payload.EmployeeIDs), "bonus_pct", payload.BonusPct)

	for _, id := range payload.EmployeeIDs {
		emp, ok := e.store.GetEmployee(id)
		if !ok {
			return nil, fmt.Errorf("employee not found: %s", id)
		}
		if err := e.store.ProcessBonus(ctx, emp, payload.BonusPct); err != nil {
			return nil, fmt.Errorf("bonus for employee %s: %w", id, err)
		}
	}
	return map[string]interface{}{
		"processed_employees": len(payload.EmployeeIDs),
		"bonus_pct":           payload.BonusPct,
		"batch_number":        payload.BatchNumber,
		"total_batches":       payload.TotalBatches,
	}, nil
}

type bonusTaskPayload struct {
	EmployeeIDs  []string `json:"employee_ids"`
	BonusPct     float64  `json:"bonus_pct"`
	BatchNumber  int      `json:"batch_number"`
	TotalBatches int      `json:"total_batches"`
}

func (p bonusTaskPayload) toMap() (map[string]interface{}, error) {
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func parseBonusTaskPayload(params map[string]interface{}) (bonusTaskPayload, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return bonusTaskPayload{}, err
	}
	var payload bonusTaskPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return bonusTaskPayload{}, err
	}
	return payload, nil
}
