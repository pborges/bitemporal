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
	row := r.repo.QueryRow(ctx, "SELECT dept_no, dept_name, valid_to, valid_from, transaction_from, transaction_to FROM departments$ WHERE dept_no=@dept_no ORDER BY transaction_from, valid_to", map[string]any{"dept_no": deptNo})
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

func (r DepartmentRepository) AllRecords(ctx context.Context, deptNo string) ([]Department, error) {
	rows, err := r.repo.Query(ctx, "SELECT dept_no, dept_name, valid_from, valid_to, transaction_from, transaction_to FROM departments WHERE dept_no=@emp_no ORDER BY transaction_from, valid_from", map[string]any{"emp_no": deptNo})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var departments []Department
	for rows.Next() {
		var dept Department
		err = rows.Scan(&dept.DeptNo, &dept.DeptName, &dept.ValidFrom, &dept.ValidTo, &dept.TransactionFrom, &dept.TransactionEnd)
		if err != nil {
			return nil, err
		}
		departments = append(departments, dept)
	}
	return departments, nil
}
