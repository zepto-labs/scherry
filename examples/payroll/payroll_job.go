package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/zepto-labs/scherry"
)

const payrollTaskName = "payroll_payout_task"

type payrollBatchJobExecutor struct {
	store     *employeeStore
	batchSize int
	logger    scherry.Logger
}

func (e *payrollBatchJobExecutor) Execute(ctx context.Context, _ map[string]interface{}) ([]scherry.TaskData, error) {
	employees, err := e.store.ListEmployees(ctx)
	if err != nil {
		return nil, err
	}
	if len(employees) == 0 {
		e.logger.Warn("no employees found")
		return nil, nil
	}

	payPeriod := previousMonthPeriod(time.Now())
	batches := chunkEmployees(employees, e.batchSize)
	e.logger.Info("created employee batches", "count", len(batches), "pay_period", payPeriod)

	tasks := make([]scherry.TaskData, 0, len(batches))
	for i, batch := range batches {
		ids := make([]string, len(batch))
		for j, emp := range batch {
			ids[j] = emp.ID
		}
		payload := taskPayload{
			Name: payrollTaskName, EmployeeIDs: ids, PayPeriod: payPeriod,
			BatchNumber: i + 1, TotalBatches: len(batches),
		}
		params, err := payload.toMap()
		if err != nil {
			return nil, fmt.Errorf("marshal batch %d: %w", i+1, err)
		}
		tasks = append(tasks, scherry.TaskData{
			Name:            payrollTaskName,
			Params:          params,
			DistributionKey: strings.Join(ids, "_"),
		})
	}
	return tasks, nil
}

type payrollPayoutTaskExecutor struct {
	store  *employeeStore
	logger scherry.Logger
}

func (e *payrollPayoutTaskExecutor) Execute(ctx context.Context, params map[string]interface{}) (map[string]interface{}, error) {
	payload, err := parseTaskPayload(params)
	if err != nil {
		return nil, err
	}
	e.logger.Info("processing payroll batch",
		"batch", payload.BatchNumber, "total", payload.TotalBatches,
		"employees", len(payload.EmployeeIDs), "pay_period", payload.PayPeriod)

	for _, id := range payload.EmployeeIDs {
		emp, ok := e.store.GetEmployee(id)
		if !ok {
			return nil, fmt.Errorf("employee not found: %s", id)
		}
		if err := e.store.ProcessPayout(ctx, emp, payload.PayPeriod); err != nil {
			return nil, fmt.Errorf("payout employee %s: %w", id, err)
		}
	}
	return map[string]interface{}{
		"processed_employees": len(payload.EmployeeIDs),
		"pay_period":          payload.PayPeriod,
		"batch_number":        payload.BatchNumber,
		"total_batches":       payload.TotalBatches,
	}, nil
}

type taskPayload struct {
	Name         string   `json:"name"`
	EmployeeIDs  []string `json:"employee_ids"`
	PayPeriod    string   `json:"pay_period"`
	BatchNumber  int      `json:"batch_number"`
	TotalBatches int      `json:"total_batches"`
}

func (p taskPayload) toMap() (map[string]interface{}, error) {
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

func parseTaskPayload(params map[string]interface{}) (taskPayload, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return taskPayload{}, err
	}
	var payload taskPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return taskPayload{}, err
	}
	return payload, nil
}

func previousMonthPeriod(now time.Time) string {
	firstOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	prev := firstOfMonth.AddDate(0, -1, 0)
	return prev.Format("2006-01")
}

func chunkEmployees(items []employee, size int) [][]employee {
	if size <= 0 {
		size = 1
	}
	var batches [][]employee
	for i := 0; i < len(items); i += size {
		end := i + size
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}
	return batches
}
