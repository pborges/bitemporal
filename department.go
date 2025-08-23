package bitemporal

import (
	"context"
	"fmt"
)

func init() {
	schema = append(schema, Table{
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
	BitemporalEntity
}

func (d Department) String() string {
	return fmt.Sprintf("Department{DeptNo: %s, Name: %s}", d.DeptNo, d.DeptName)
}

func NewDepartmentRepository(repo *Repository) *DepartmentRepository {
	return &DepartmentRepository{
		repo: repo,
	}
}

type DepartmentRepository struct {
	repo *Repository
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
