-- Bitemporal Department-Employee Assignment Queries

-- Get current department assignment for employee
-- name: GetEmployeeDepartmentCurrent :one
SELECT * FROM dept_emp
WHERE emp_no = ? 
  AND valid_from <= ? 
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get department assignment as it was known at specific transaction time
-- name: GetEmployeeDepartmentAsOfTransaction :one
SELECT * FROM dept_emp
WHERE emp_no = ?
  AND transaction_time <= ?
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get complete history of department assignments for employee
-- name: GetEmployeeDepartmentHistory :many
SELECT * FROM dept_emp
WHERE emp_no = ?
ORDER BY valid_from, transaction_time;

-- Get all employees in department (current assignments)
-- name: GetDepartmentEmployeesCurrent :many
SELECT * FROM dept_emp
WHERE dept_no = ?
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY emp_no;

-- Get department assignments as they existed at specific transaction time
-- name: ListDeptEmpAsOfTransaction :many
SELECT DISTINCT emp_no, dept_no, valid_from, valid_to, transaction_time
FROM dept_emp de1
WHERE de1.transaction_time <= ?
  AND de1.valid_from <= ?
  AND de1.valid_to > ?
  AND de1.transaction_time = (
    SELECT MAX(de2.transaction_time)
    FROM dept_emp de2
    WHERE de2.emp_no = de1.emp_no
      AND de2.dept_no = de1.dept_no
      AND de2.transaction_time <= ?
  )
ORDER BY emp_no, dept_no;

-- Create new department assignment
-- name: CreateDeptEmp :one
INSERT INTO dept_emp (
  emp_no, dept_no, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?
)
RETURNING *;

-- Close current department assignment (used before transfer)
-- name: CloseDeptEmpValidPeriod :exec
UPDATE dept_emp 
SET valid_to = ?
WHERE emp_no = ?
  AND dept_no = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Close all current assignments for employee (used before new assignment)
-- name: CloseAllEmployeeAssignments :exec
UPDATE dept_emp 
SET valid_to = ?
WHERE emp_no = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Insert updated department assignment
-- name: InsertDeptEmpUpdate :one
INSERT INTO dept_emp (
  emp_no, dept_no, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?
)
RETURNING *;

-- Transfer employee to new department (close old, create new)
-- name: TransferEmployee :exec
UPDATE dept_emp 
SET valid_to = ?
WHERE emp_no = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Get employees who changed departments in date range
-- name: GetDepartmentTransfers :many
SELECT de1.emp_no, de1.dept_no as old_dept, de2.dept_no as new_dept,
       de1.valid_to as transfer_date
FROM dept_emp de1
JOIN dept_emp de2 ON de1.emp_no = de2.emp_no
WHERE de1.valid_to BETWEEN ? AND ?
  AND de2.valid_from = de1.valid_to
  AND de1.dept_no != de2.dept_no
ORDER BY de1.valid_to, de1.emp_no;