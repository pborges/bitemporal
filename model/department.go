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
	bitemporal.Entity
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
	row := r.repo.QueryRow(ctx, "SELECT dept_no, dept_name, valid_close, valid_open, txn_open, txn_close FROM departments$ WHERE dept_no=@dept_no ORDER BY txn_open, valid_close", map[string]any{"dept_no": deptNo})
	if row.Err() != nil {
		return Department{}, row.Err()
	}
	department := Department{}
	err := row.Scan(&department.DeptNo, &department.DeptName, &department.ValidClose, &department.ValidOpen, &department.TxnOpen, &department.TxnClose)
	if err != nil {
		return Department{}, err
	}
	return department, nil
}

func (r DepartmentRepository) AllRecords(ctx context.Context, deptNo string) ([]Department, error) {
	rows, err := r.repo.Query(ctx, "SELECT dept_no, dept_name, valid_open, valid_close, txn_open, txn_close FROM departments WHERE dept_no=@emp_no ORDER BY txn_open, valid_open", map[string]any{"emp_no": deptNo})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var departments []Department
	for rows.Next() {
		var dept Department
		err = rows.Scan(&dept.DeptNo, &dept.DeptName, &dept.ValidOpen, &dept.ValidClose, &dept.TxnOpen, &dept.TxnClose)
		if err != nil {
			return nil, err
		}
		departments = append(departments, dept)
	}
	return departments, nil
}
