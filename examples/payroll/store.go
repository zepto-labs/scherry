package main

import (
	"context"
	"fmt"
	"sync"
)

type employee struct {
	ID            string
	Name          string
	MonthlySalary float64
}

type employeeStore struct {
	mu        sync.Mutex
	employees []employee
	payouts   []string
}

func newEmployeeStore() *employeeStore {
	return &employeeStore{
		employees: []employee{
			{ID: "emp-001", Name: "Alice", MonthlySalary: 5000},
			{ID: "emp-002", Name: "Bob", MonthlySalary: 6200},
			{ID: "emp-003", Name: "Carol", MonthlySalary: 4800},
			{ID: "emp-004", Name: "Dave", MonthlySalary: 7100},
			{ID: "emp-005", Name: "Eve", MonthlySalary: 5500},
		},
	}
}

func (s *employeeStore) ListEmployees(_ context.Context) ([]employee, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]employee, len(s.employees))
	copy(out, s.employees)
	return out, nil
}

func (s *employeeStore) GetEmployee(id string) (employee, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, emp := range s.employees {
		if emp.ID == id {
			return emp, true
		}
	}
	return employee{}, false
}

func (s *employeeStore) ProcessPayout(_ context.Context, emp employee, payPeriod string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.payouts = append(s.payouts, fmt.Sprintf(
		"payout employee=%s name=%s amount=%.2f period=%s",
		emp.ID, emp.Name, emp.MonthlySalary, payPeriod,
	))
	return nil
}

func (s *employeeStore) ProcessBonus(_ context.Context, emp employee, bonusPct float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	amount := emp.MonthlySalary * bonusPct / 100
	s.payouts = append(s.payouts, fmt.Sprintf(
		"bonus employee=%s name=%s salary=%.2f bonus_pct=%.2f%% amount=%.2f",
		emp.ID, emp.Name, emp.MonthlySalary, bonusPct, amount,
	))
	return nil
}

func (s *employeeStore) Payouts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.payouts))
	copy(out, s.payouts)
	return out
}
