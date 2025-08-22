-- Bitemporal Employee Queries

-- Get current version of employee (as of specific valid time)
-- name: GetEmployeeCurrent :one
SELECT * FROM employees
WHERE emp_no = ? 
  AND valid_from <= ? 
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get employee as it was known at a specific transaction time (time travel)
-- name: GetEmployeeAsOfTransaction :one
SELECT * FROM employees
WHERE emp_no = ?
  AND transaction_time <= ?
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get complete history of employee changes
-- name: GetEmployeeHistory :many
SELECT * FROM employees
WHERE emp_no = ?
ORDER BY valid_from, transaction_time;

-- Get all current employees (as of specific valid time)
-- name: ListEmployeesCurrent :many
SELECT * FROM employees
WHERE valid_from <= ? 
  AND valid_to > ?
ORDER BY emp_no;

-- Get employees as they existed at a specific transaction time
-- name: ListEmployeesAsOfTransaction :many
SELECT DISTINCT emp_no, birth_date, first_name, last_name, gender, hire_date, 
       valid_from, valid_to, transaction_time
FROM employees e1
WHERE e1.transaction_time <= ?
  AND e1.valid_from <= ?
  AND e1.valid_to > ?
  AND e1.transaction_time = (
    SELECT MAX(e2.transaction_time)
    FROM employees e2
    WHERE e2.emp_no = e1.emp_no
      AND e2.transaction_time <= ?
  )
ORDER BY emp_no;

-- Create new employee (insert with current transaction time)
-- name: CreateEmployee :one
INSERT INTO employees (
  emp_no, birth_date, first_name, last_name, gender, hire_date, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?, ?, ?, ?, ?
)
RETURNING *;

-- Update employee (bitemporal update - close current record and insert new)
-- This is typically done in application logic, but this closes the current valid period
-- name: CloseEmployeeValidPeriod :exec
UPDATE employees 
SET valid_to = ?
WHERE emp_no = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Insert updated employee record (used after closing previous record)
-- name: InsertEmployeeUpdate :one
INSERT INTO employees (
  emp_no, birth_date, first_name, last_name, gender, hire_date, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?, ?, ?, ?, ?
)
RETURNING *;

-- Logical delete (close valid period)
-- name: DeleteEmployee :exec
UPDATE employees 
SET valid_to = ?
WHERE emp_no = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Search employees by name (current versions only)
-- name: SearchEmployeesByName :many
SELECT * FROM employees
WHERE (first_name LIKE ? OR last_name LIKE ?)
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY last_name, first_name;

-- Get employees hired in date range
-- name: GetEmployeesByHireDate :many
SELECT * FROM employees
WHERE hire_date BETWEEN ? AND ?
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY hire_date;

-- Get employee with current salary, title, and department
-- name: GetEmployeeWithCurrentDetails :one
SELECT 
  e.emp_no, e.first_name, e.last_name, e.birth_date, e.gender, e.hire_date,
  s.salary,
  t.title,
  d.dept_no, d.dept_name,
  CASE WHEN dm.emp_no IS NOT NULL THEN true ELSE false END as is_manager
FROM employees e
LEFT JOIN salaries s ON e.emp_no = s.emp_no
  AND s.valid_from <= ? AND s.valid_to > ?
LEFT JOIN titles t ON e.emp_no = t.emp_no
  AND t.valid_from <= ? AND t.valid_to > ?
LEFT JOIN dept_emp de ON e.emp_no = de.emp_no
  AND de.valid_from <= ? AND de.valid_to > ?
LEFT JOIN departments d ON de.dept_no = d.dept_no
  AND d.valid_from <= ? AND d.valid_to > ?
LEFT JOIN dept_manager dm ON e.emp_no = dm.emp_no
  AND dm.valid_from <= ? AND dm.valid_to > ?
WHERE e.emp_no = ?
  AND e.valid_from <= ? AND e.valid_to > ?
ORDER BY s.transaction_time DESC, t.transaction_time DESC, de.transaction_time DESC
LIMIT 1;

-- Get all employees with their current salary, title, and department
-- name: ListEmployeesWithCurrentDetails :many
SELECT 
  e.emp_no, e.first_name, e.last_name, e.birth_date, e.gender, e.hire_date,
  COALESCE(s.salary, 0) as salary,
  COALESCE(t.title, 'No Title') as title,
  COALESCE(d.dept_no, '') as dept_no,
  COALESCE(d.dept_name, 'No Department') as dept_name,
  CASE WHEN dm.emp_no IS NOT NULL THEN true ELSE false END as is_manager
FROM employees e
LEFT JOIN salaries s ON e.emp_no = s.emp_no
  AND s.valid_from <= ? AND s.valid_to > ?
LEFT JOIN titles t ON e.emp_no = t.emp_no
  AND t.valid_from <= ? AND t.valid_to > ?
LEFT JOIN dept_emp de ON e.emp_no = de.emp_no
  AND de.valid_from <= ? AND de.valid_to > ?
LEFT JOIN departments d ON de.dept_no = d.dept_no
  AND d.valid_from <= ? AND d.valid_to > ?
LEFT JOIN dept_manager dm ON e.emp_no = dm.emp_no
  AND dm.valid_from <= ? AND dm.valid_to > ?
WHERE e.valid_from <= ? AND e.valid_to > ?
ORDER BY e.emp_no
LIMIT ?;

-- Get employees by department with salary and title info
-- name: GetEmployeesByDepartmentWithDetails :many
SELECT 
  e.emp_no, e.first_name, e.last_name,
  s.salary,
  t.title,
  d.dept_name,
  CASE WHEN dm.emp_no IS NOT NULL THEN true ELSE false END as is_manager
FROM employees e
JOIN dept_emp de ON e.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
LEFT JOIN salaries s ON e.emp_no = s.emp_no
  AND s.valid_from <= ? AND s.valid_to > ?
LEFT JOIN titles t ON e.emp_no = t.emp_no
  AND t.valid_from <= ? AND t.valid_to > ?
LEFT JOIN dept_manager dm ON e.emp_no = dm.emp_no
  AND dm.valid_from <= ? AND dm.valid_to > ?
WHERE d.dept_no = ?
  AND e.valid_from <= ? AND e.valid_to > ?
  AND de.valid_from <= ? AND de.valid_to > ?
  AND d.valid_from <= ? AND d.valid_to > ?
ORDER BY s.salary DESC;

-- Get employees by title with salary and department info
-- name: GetEmployeesByTitleWithDetails :many
SELECT 
  e.emp_no, e.first_name, e.last_name,
  s.salary,
  t.title,
  d.dept_name,
  CASE WHEN dm.emp_no IS NOT NULL THEN true ELSE false END as is_manager
FROM employees e
JOIN titles t ON e.emp_no = t.emp_no
LEFT JOIN dept_emp de ON e.emp_no = de.emp_no
  AND de.valid_from <= ? AND de.valid_to > ?
LEFT JOIN departments d ON de.dept_no = d.dept_no
  AND d.valid_from <= ? AND d.valid_to > ?
LEFT JOIN salaries s ON e.emp_no = s.emp_no
  AND s.valid_from <= ? AND s.valid_to > ?
LEFT JOIN dept_manager dm ON e.emp_no = dm.emp_no
  AND dm.valid_from <= ? AND dm.valid_to > ?
WHERE t.title = ?
  AND e.valid_from <= ? AND e.valid_to > ?
  AND t.valid_from <= ? AND t.valid_to > ?
ORDER BY s.salary DESC;

-- Get employees by salary range with title and department info
-- name: GetEmployeesBySalaryRangeWithDetails :many
SELECT 
  e.emp_no, e.first_name, e.last_name,
  s.salary,
  t.title,
  d.dept_name,
  CASE WHEN dm.emp_no IS NOT NULL THEN true ELSE false END as is_manager
FROM employees e
JOIN salaries s ON e.emp_no = s.emp_no
LEFT JOIN titles t ON e.emp_no = t.emp_no
  AND t.valid_from <= ? AND t.valid_to > ?
LEFT JOIN dept_emp de ON e.emp_no = de.emp_no
  AND de.valid_from <= ? AND de.valid_to > ?
LEFT JOIN departments d ON de.dept_no = d.dept_no
  AND d.valid_from <= ? AND d.valid_to > ?
LEFT JOIN dept_manager dm ON e.emp_no = dm.emp_no
  AND dm.valid_from <= ? AND dm.valid_to > ?
WHERE s.salary BETWEEN ? AND ?
  AND e.valid_from <= ? AND e.valid_to > ?
  AND s.valid_from <= ? AND s.valid_to > ?
ORDER BY s.salary DESC;

-- Get managers with their department info and salary
-- name: GetManagersWithDetails :many
SELECT 
  e.emp_no, e.first_name, e.last_name,
  s.salary,
  t.title,
  d.dept_no, d.dept_name,
  dm.valid_from as manager_since
FROM employees e
JOIN dept_manager dm ON e.emp_no = dm.emp_no
JOIN departments d ON dm.dept_no = d.dept_no
LEFT JOIN salaries s ON e.emp_no = s.emp_no
  AND s.valid_from <= ? AND s.valid_to > ?
LEFT JOIN titles t ON e.emp_no = t.emp_no
  AND t.valid_from <= ? AND t.valid_to > ?
WHERE e.valid_from <= ? AND e.valid_to > ?
  AND dm.valid_from <= ? AND dm.valid_to > ?
  AND d.valid_from <= ? AND d.valid_to > ?
ORDER BY d.dept_no;

-- Get employee salary history
-- name: GetEmployeeSalaryTimeline :many
SELECT 
  'salary' as change_type,
  s.valid_from as change_date,
  'Salary: $' || s.salary as description,
  s.transaction_time
FROM salaries s
WHERE s.emp_no = ?
ORDER BY s.valid_from DESC, s.transaction_time DESC;

-- Get employee title history  
-- name: GetEmployeeTitleTimeline :many
SELECT 
  'title' as change_type,
  t.valid_from as change_date,
  t.valid_to as end_date,
  'Title: ' || t.title as description,
  t.transaction_time
FROM titles t
WHERE t.emp_no = ?
ORDER BY t.valid_from DESC, t.transaction_time DESC;

-- Get employee department history
-- name: GetEmployeeDepartmentTimeline :many
SELECT 
  'department' as change_type,
  de.valid_from as change_date,
  'Department: ' || d.dept_name as description,
  de.transaction_time
FROM dept_emp de
JOIN departments d ON de.dept_no = d.dept_no
WHERE de.emp_no = ?
ORDER BY de.valid_from DESC, de.transaction_time DESC;

-- Get complete employee timeline with all changes (using JOINs)
-- name: GetEmployeeTimeline :many
SELECT 
  e.emp_no,
  e.first_name,
  e.last_name,
  s.salary,
  s.valid_from as salary_valid_from,
  s.valid_to as salary_valid_to,
  t.title,
  t.valid_from as title_valid_from,
  t.valid_to as title_valid_to,
  d.dept_name,
  de.valid_from as dept_valid_from,
  de.valid_to as dept_valid_to,
  CASE WHEN dm.emp_no IS NOT NULL THEN d.dept_name ELSE NULL END as managed_dept,
  dm.valid_from as manager_valid_from,
  dm.valid_to as manager_valid_to
FROM employees e
LEFT JOIN salaries s ON e.emp_no = s.emp_no
LEFT JOIN titles t ON e.emp_no = t.emp_no
LEFT JOIN dept_emp de ON e.emp_no = de.emp_no
LEFT JOIN departments d ON de.dept_no = d.dept_no
LEFT JOIN dept_manager dm ON e.emp_no = dm.emp_no
WHERE e.emp_no = ?
  AND e.valid_from <= ? AND e.valid_to > ?
ORDER BY 
  COALESCE(s.valid_from, '1900-01-01') DESC,
  COALESCE(t.valid_from, '1900-01-01') DESC,
  COALESCE(de.valid_from, '1900-01-01') DESC,
  COALESCE(dm.valid_from, '1900-01-01') DESC;