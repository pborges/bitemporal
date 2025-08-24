package model

import (
	"context"
	"fmt"

	"github.com/pborges/bitemporal"
)

func init() {
	bitemporal.Schema = append(bitemporal.Schema, bitemporal.Table{
		Name: "salaries",
		Columns: []string{
			"emp_no",
			"salary",
		},
	})
}

type Salary struct {
	EmpNo  int64 `json:"emp_no"`
	Salary int64 `json:"salary"`
	bitemporal.BitemporalEntity
}

func (s Salary) String() string {
	return fmt.Sprintf("Salary{EmpNo: %d, Amount: %d}", s.EmpNo, s.Salary)
}

func NewSalaryRepository(repo *bitemporal.TemporalDB) *SalaryRepository {
	return &SalaryRepository{
		repo: repo,
	}
}

type SalaryRepository struct {
	repo *bitemporal.TemporalDB
}

func (r SalaryRepository) ForEmployee(ctx context.Context, empNo int64) ([]Salary, error) {
	rows, err := r.repo.Query(ctx, "SELECT emp_no, salary, valid_to, valid_from, transaction_from, transaction_to FROM salaries$ WHERE emp_no=? ORDER BY transaction_from, valid_to", empNo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var salaries []Salary
	for rows.Next() {
		salary := Salary{}
		err := rows.Scan(&salary.EmpNo, &salary.Salary, &salary.ValidTo, &salary.ValidFrom, &salary.TransactionFrom, &salary.TransactionEnd)
		if err != nil {
			return nil, err
		}
		salaries = append(salaries, salary)
	}
	return salaries, nil
}
