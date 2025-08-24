package model

import (
	"context"
	"fmt"

	"github.com/pborges/bitemporal"
)

func init() {
	bitemporal.Schema = append(bitemporal.Schema, bitemporal.Table{
		Name: "departments",
		Columns: []string{
			"dept_no",
			"dept_name",
		},
	})
}

type Department struct {
	DeptNo   string `json:"dept_no"`
	DeptName string `json:"dept_name"`
	bitemporal.BitemporalEntity
}

func (d Department) String() string {
	return fmt.Sprintf("Department{DeptNo: %s, Name: %s}", d.DeptNo, d.DeptName)
}

func NewDepartmentRepository(repo *bitemporal.TemporalDB) *DepartmentRepository {
	return &DepartmentRepository{
		repo: repo,
	}
}

type DepartmentRepository struct {
	repo *bitemporal.TemporalDB
}

func (r DepartmentRepository) ById(ctx context.Context, deptNo string) (Department, error) {
	row := r.repo.QueryRow(ctx, "SELECT dept_no, dept_name, valid_to, valid_from, transaction_from, transaction_to FROM departments$ WHERE dept_no=? ORDER BY transaction_from, valid_to", deptNo)
	if row.Err() != nil {
		return Department{}, row.Err()
	}
	department := Department{}
	err := row.Scan(&department.DeptNo, &department.DeptName, &department.ValidTo, &department.ValidFrom, &department.TransactionFrom, &department.TransactionEnd)
	if err != nil {
		return Department{}, err
	}
	return department, nil
}
