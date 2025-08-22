-- Bitemporal Department Queries

-- Get current version of department (as of specific valid time)
-- name: GetDepartmentCurrent :one
SELECT * FROM departments
WHERE dept_no = ? 
  AND valid_from <= ? 
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get department as it was known at a specific transaction time (time travel)
-- name: GetDepartmentAsOfTransaction :one
SELECT * FROM departments
WHERE dept_no = ?
  AND transaction_time <= ?
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get complete history of department changes
-- name: GetDepartmentHistory :many
SELECT * FROM departments
WHERE dept_no = ?
ORDER BY valid_from, transaction_time;

-- Get all current departments (as of specific valid time)
-- name: ListDepartmentsCurrent :many
SELECT * FROM departments
WHERE valid_from <= ? 
  AND valid_to > ?
ORDER BY dept_no;

-- Get departments as they existed at a specific transaction time
-- name: ListDepartmentsAsOfTransaction :many
SELECT DISTINCT dept_no, dept_name, valid_from, valid_to, transaction_time
FROM departments d1
WHERE d1.transaction_time <= ?
  AND d1.valid_from <= ?
  AND d1.valid_to > ?
  AND d1.transaction_time = (
    SELECT MAX(d2.transaction_time)
    FROM departments d2
    WHERE d2.dept_no = d1.dept_no
      AND d2.transaction_time <= ?
  )
ORDER BY dept_no;

-- Create new department
-- name: CreateDepartment :one
INSERT INTO departments (
  dept_no, dept_name, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?
)
RETURNING *;

-- Close current valid period for department (used before update)
-- name: CloseDepartmentValidPeriod :exec
UPDATE departments 
SET valid_to = ?
WHERE dept_no = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Insert updated department record
-- name: InsertDepartmentUpdate :one
INSERT INTO departments (
  dept_no, dept_name, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?
)
RETURNING *;

-- Logical delete (close valid period)
-- name: DeleteDepartment :exec
UPDATE departments 
SET valid_to = ?
WHERE dept_no = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Search departments by name
-- name: SearchDepartmentsByName :many
SELECT * FROM departments
WHERE dept_name LIKE ?
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY dept_name;