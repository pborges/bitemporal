package model

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pborges/bitemporal"
)

func init() {
	bitemporal.Schema = append(bitemporal.Schema, bitemporal.Table{
		Name: "employees",
		Columns: []string{
			"emp_no",
			"birth_date",
			"first_name",
			"last_name",
			"gender",
			"hire_date",
		},
	})
}

type Employee struct {
	EmpNo     int64     `json:"emp_no"`
	BirthDate time.Time `json:"birth_date"`
	FirstName string    `json:"first_name"`
	LastName  string    `json:"last_name"`
	Gender    string    `json:"gender"`
	HireDate  time.Time `json:"hire_date"`
	bitemporal.BitemporalEntity
}

func (e Employee) String() string {
	return fmt.Sprintf("Employee{EmpNo: %d, Name: %s %s, Gender: %s, HireDate: %s}",
		e.EmpNo, e.FirstName, e.LastName, e.Gender, e.HireDate.Format("2006-01-02"))
}

func NewEmployeeRepository(repo *bitemporal.TemporalDB) *EmployeeRepository {
	return &EmployeeRepository{
		repo: repo,
	}
}

type EmployeeRepository struct {
	repo *bitemporal.TemporalDB
}

func (r EmployeeRepository) ById(ctx context.Context, empNo int64) (Employee, error) {
	row := r.repo.QueryRow(ctx, "SELECT emp_no, birth_date, first_name, last_name, gender, hire_date, valid_to, valid_from, transaction_from, transaction_to FROM employees$ WHERE emp_no=? ORDER BY transaction_from, valid_to", empNo)
	if row.Err() != nil {
		return Employee{}, row.Err()
	}
	employee := Employee{}
	err := row.Scan(
		&employee.EmpNo,
		&employee.BirthDate,
		&employee.FirstName,
		&employee.LastName,
		&employee.Gender,
		&employee.HireDate,
		&employee.ValidTo,
		&employee.ValidFrom,
		&employee.TransactionFrom,
		&employee.TransactionEnd,
	)
	if err != nil {
		return Employee{}, err
	}
	return employee, nil
}

func (r EmployeeRepository) AllRecords(ctx context.Context, empNo int64) ([]Employee, error) {
	rows, err := r.repo.Query(ctx, "SELECT emp_no, birth_date, first_name, last_name, gender, hire_date, valid_from, valid_to, transaction_from, transaction_to FROM employees WHERE emp_no=? ORDER BY transaction_from, valid_from", empNo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var employees []Employee
	for rows.Next() {
		var emp Employee
		err = rows.Scan(&emp.EmpNo, &emp.BirthDate, &emp.FirstName, &emp.LastName, &emp.Gender, &emp.HireDate,
			&emp.ValidFrom, &emp.ValidTo, &emp.TransactionFrom, &emp.TransactionEnd)
		if err != nil {
			return nil, err
		}
		employees = append(employees, emp)
	}
	return employees, nil
}

func (r EmployeeRepository) Save(m Employee, from, to time.Time) error {
	sql := `
-- Step 1: Close any overlapping records by setting their valid_to to the start of new period
UPDATE employees
SET valid_to = @new_valid_from, transaction_to = CURRENT_TIMESTAMP
WHERE emp_no = @emp_no
  AND valid_from < @new_valid_from
  AND valid_to > @new_valid_from
  AND transaction_to = '9999-12-31 23:59:59';
-- Only update current records

-- Step 2: Handle records that are completely contained within the new period
-- (These will be superseded by the new record)
UPDATE employees
SET valid_to = @new_valid_from, transaction_to = CURRENT_TIMESTAMP
WHERE emp_no = @emp_no
  AND valid_from >= @new_valid_from
  AND valid_to <= @new_valid_to
  AND transaction_to = '9999-12-31 23:59:59';

-- Step 3: Handle records that start before and end after the new period
-- Split them: close the first part and create a continuation after
INSERT INTO employees (emp_no, birth_date, first_name, last_name, gender, hire_date,
                       valid_from, valid_to, transaction_from, transaction_to)
SELECT emp_no,
       birth_date,
       first_name,
       last_name,
       gender,
       hire_date,
       @new_valid_to,
       valid_to,
       CURRENT_TIMESTAMP,
       '9999-12-31 23:59:59'
FROM employees
WHERE emp_no = @emp_no
  AND valid_from < @new_valid_from
  AND valid_to > @new_valid_to
  AND transaction_to = '9999-12-31 23:59:59';

-- Update the original record to end at new period start
UPDATE employees
SET valid_to = @new_valid_from, transaction_to = CURRENT_TIMESTAMP
WHERE emp_no = @emp_no
  AND valid_from < @new_valid_from
  AND valid_to > @new_valid_to
  AND transaction_to = '9999-12-31 23:59:59';

-- Step 4: Insert the new record for the specified time period
INSERT INTO employees (emp_no, birth_date, first_name, last_name, gender, hire_date,
                       valid_from, valid_to, transaction_from, transaction_to)
VALUES (@emp_no, @birth_date, @first_name, @last_name, @gender, @hire_date,
        @new_valid_from, @new_valid_to, CURRENT_TIMESTAMP, '9999-12-31 23:59:59');`
	sql = strings.ReplaceAll(sql, "@emp_no", fmt.Sprintf("%d", m.EmpNo))
	sql = strings.ReplaceAll(sql, "@first_name", fmt.Sprintf("'%s'", m.FirstName))
	sql = strings.ReplaceAll(sql, "@last_name", fmt.Sprintf("'%s'", m.LastName))
	sql = strings.ReplaceAll(sql, "@gender", fmt.Sprintf("'%s'", m.Gender))
	sql = strings.ReplaceAll(sql, "@birth_date", fmt.Sprintf("'%s'", m.BirthDate.Format(time.DateTime)))
	sql = strings.ReplaceAll(sql, "@hire_date", fmt.Sprintf("'%s'", m.HireDate.Format(time.DateTime)))

	sql = strings.ReplaceAll(sql, "@new_valid_from", "'"+from.Format(time.DateTime)+"'")
	sql = strings.ReplaceAll(sql, "@new_valid_to", "'"+to.Format(time.DateTime)+"'")
	fmt.Println(sql)
	return nil
}
