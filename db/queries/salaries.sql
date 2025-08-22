-- Bitemporal Employee Salary Queries

-- Get current salary for employee
-- name: GetEmployeeSalaryCurrent :one
SELECT * FROM salaries
WHERE emp_no = ? 
  AND valid_from <= ? 
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get employee salary as it was known at specific transaction time
-- name: GetEmployeeSalaryAsOfTransaction :one
SELECT * FROM salaries
WHERE emp_no = ?
  AND transaction_time <= ?
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get complete salary history for employee
-- name: GetEmployeeSalaryHistory :many
SELECT * FROM salaries
WHERE emp_no = ?
ORDER BY valid_from, transaction_time;

-- Get all current salaries with employee info
-- name: ListCurrentSalariesWithEmployees :many
SELECT s.*, e.first_name, e.last_name 
FROM salaries s
JOIN employees e ON s.emp_no = e.emp_no
WHERE s.valid_from <= ? 
  AND s.valid_to > ?
  AND e.valid_from <= ?
  AND e.valid_to > ?
ORDER BY s.salary DESC;

-- Get salaries as they existed at specific transaction time
-- name: ListSalariesAsOfTransaction :many
SELECT DISTINCT emp_no, salary, valid_from, valid_to, transaction_time
FROM salaries s1
WHERE s1.transaction_time <= ?
  AND s1.valid_from <= ?
  AND s1.valid_to > ?
  AND s1.transaction_time = (
    SELECT MAX(s2.transaction_time)
    FROM salaries s2
    WHERE s2.emp_no = s1.emp_no
      AND s2.transaction_time <= ?
  )
ORDER BY emp_no;

-- Create new salary record
-- name: CreateSalary :one
INSERT INTO salaries (
  emp_no, salary, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?
)
RETURNING *;

-- Close current salary (used before salary change)
-- name: CloseEmployeeSalaryValidPeriod :exec
UPDATE salaries 
SET valid_to = ?
WHERE emp_no = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Insert updated salary (raise/cut)
-- name: InsertSalaryUpdate :one
INSERT INTO salaries (
  emp_no, salary, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?
)
RETURNING *;

-- Get salary changes in date range
-- name: GetSalaryChanges :many
SELECT s1.emp_no, s1.salary as old_salary, s2.salary as new_salary,
       s1.valid_to as change_date,
       ROUND(((s2.salary - s1.salary) * 100.0 / s1.salary), 2) as percent_change
FROM salaries s1
JOIN salaries s2 ON s1.emp_no = s2.emp_no
WHERE s1.valid_to BETWEEN ? AND ?
  AND s2.valid_from = s1.valid_to
  AND s1.salary != s2.salary
ORDER BY s1.valid_to, s1.emp_no;

-- Get employees with salary raises above percentage
-- name: GetSalaryRaisesAbovePercent :many
SELECT s1.emp_no, e.first_name, e.last_name,
       s1.salary as old_salary, s2.salary as new_salary,
       s2.valid_from as raise_date,
       ROUND(((s2.salary - s1.salary) * 100.0 / s1.salary), 2) as percent_increase
FROM salaries s1
JOIN salaries s2 ON s1.emp_no = s2.emp_no
JOIN employees e ON s1.emp_no = e.emp_no
WHERE s2.valid_from BETWEEN ? AND ?
  AND s1.valid_to = s2.valid_from
  AND s2.salary > s1.salary
  AND ((s2.salary - s1.salary) * 100.0 / s1.salary) >= ?
  AND e.valid_from <= s2.valid_from
  AND e.valid_to > s2.valid_from
ORDER BY percent_increase DESC;

-- Get salary statistics for date range
-- name: GetSalaryStatistics :one
SELECT 
  COUNT(*) as employee_count,
  MIN(salary) as min_salary,
  MAX(salary) as max_salary,
  ROUND(AVG(salary), 2) as avg_salary
FROM salaries
WHERE valid_from <= ?
  AND valid_to > ?;

-- Get top earners (current)
-- name: GetTopEarners :many
SELECT s.emp_no, e.first_name, e.last_name, s.salary
FROM salaries s
JOIN employees e ON s.emp_no = e.emp_no
WHERE s.valid_from <= ?
  AND s.valid_to > ?
  AND e.valid_from <= ?
  AND e.valid_to > ?
ORDER BY s.salary DESC
LIMIT ?;