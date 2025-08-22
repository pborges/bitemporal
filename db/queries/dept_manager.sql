-- Bitemporal Department Manager Queries

-- Get current manager for department
-- name: GetDepartmentManagerCurrent :one
SELECT * FROM dept_manager
WHERE dept_no = ? 
  AND valid_from <= ? 
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get department manager as it was known at specific transaction time
-- name: GetDepartmentManagerAsOfTransaction :one
SELECT * FROM dept_manager
WHERE dept_no = ?
  AND transaction_time <= ?
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get complete history of managers for department
-- name: GetDepartmentManagerHistory :many
SELECT * FROM dept_manager
WHERE dept_no = ?
ORDER BY valid_from, transaction_time;

-- Get all departments managed by employee (current)
-- name: GetEmployeeManagementCurrent :many
SELECT * FROM dept_manager
WHERE emp_no = ?
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY dept_no;

-- Get all current department managers
-- name: ListCurrentManagers :many
SELECT * FROM dept_manager
WHERE valid_from <= ?
  AND valid_to > ?
ORDER BY dept_no;

-- Get department managers as they existed at specific transaction time
-- name: ListManagersAsOfTransaction :many
SELECT DISTINCT emp_no, dept_no, valid_from, valid_to, transaction_time
FROM dept_manager dm1
WHERE dm1.transaction_time <= ?
  AND dm1.valid_from <= ?
  AND dm1.valid_to > ?
  AND dm1.transaction_time = (
    SELECT MAX(dm2.transaction_time)
    FROM dept_manager dm2
    WHERE dm2.emp_no = dm1.emp_no
      AND dm2.dept_no = dm1.dept_no
      AND dm2.transaction_time <= ?
  )
ORDER BY dept_no;

-- Create new department manager assignment
-- name: CreateDeptManager :one
INSERT INTO dept_manager (
  emp_no, dept_no, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?
)
RETURNING *;

-- Close current manager assignment (used before replacement)
-- name: CloseDeptManagerValidPeriod :exec
UPDATE dept_manager 
SET valid_to = ?
WHERE dept_no = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Close specific manager assignment
-- name: CloseSpecificManagerAssignment :exec
UPDATE dept_manager 
SET valid_to = ?
WHERE emp_no = ?
  AND dept_no = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Insert updated manager assignment
-- name: InsertDeptManagerUpdate :one
INSERT INTO dept_manager (
  emp_no, dept_no, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?
)
RETURNING *;

-- Get manager changes in date range
-- name: GetManagerChanges :many
SELECT dm1.dept_no, dm1.emp_no as old_manager, dm2.emp_no as new_manager,
       dm1.valid_to as change_date
FROM dept_manager dm1
JOIN dept_manager dm2 ON dm1.dept_no = dm2.dept_no
WHERE dm1.valid_to BETWEEN ? AND ?
  AND dm2.valid_from = dm1.valid_to
  AND dm1.emp_no != dm2.emp_no
ORDER BY dm1.valid_to, dm1.dept_no;

-- Get departments with no current manager
-- name: GetDepartmentsWithoutManager :many
SELECT d.*
FROM departments d
WHERE d.valid_from <= ?
  AND d.valid_to > ?
  AND NOT EXISTS (
    SELECT 1 FROM dept_manager dm
    WHERE dm.dept_no = d.dept_no
      AND dm.valid_from <= ?
      AND dm.valid_to > ?
  );