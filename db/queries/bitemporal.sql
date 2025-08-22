-- Advanced Bitemporal Queries Across Multiple Tables

-- Get complete employee profile with current department, title, and salary
-- name: GetEmployeeProfile :one
SELECT 
  e.emp_no, e.first_name, e.last_name, e.birth_date, e.gender, e.hire_date,
  d.dept_no, d.dept_name,
  t.title,
  s.salary,
  CASE WHEN dm.emp_no IS NOT NULL THEN 1 ELSE 0 END as is_manager
FROM employees e
LEFT JOIN dept_emp de ON e.emp_no = de.emp_no 
  AND de.valid_from <= ? AND de.valid_to > ?
LEFT JOIN departments d ON de.dept_no = d.dept_no
  AND d.valid_from <= ? AND d.valid_to > ?
LEFT JOIN titles t ON e.emp_no = t.emp_no
  AND t.valid_from <= ? AND t.valid_to > ?
LEFT JOIN salaries s ON e.emp_no = s.emp_no
  AND s.valid_from <= ? AND s.valid_to > ?
LEFT JOIN dept_manager dm ON e.emp_no = dm.emp_no
  AND dm.valid_from <= ? AND dm.valid_to > ?
WHERE e.emp_no = ?
  AND e.valid_from <= ? AND e.valid_to > ?
ORDER BY de.transaction_time DESC, t.transaction_time DESC, s.transaction_time DESC
LIMIT 1;

-- Get employee profile as it existed at specific transaction time (time travel)
-- name: GetEmployeeProfileTimeTravel :one
SELECT 
  e.emp_no, e.first_name, e.last_name, e.birth_date, e.gender, e.hire_date,
  d.dept_no, d.dept_name,
  t.title,
  s.salary,
  CASE WHEN dm.emp_no IS NOT NULL THEN 1 ELSE 0 END as is_manager
FROM employees e
LEFT JOIN dept_emp de ON e.emp_no = de.emp_no 
  AND de.transaction_time <= ? AND de.valid_from <= ? AND de.valid_to > ?
LEFT JOIN departments d ON de.dept_no = d.dept_no
  AND d.transaction_time <= ? AND d.valid_from <= ? AND d.valid_to > ?
LEFT JOIN titles t ON e.emp_no = t.emp_no
  AND t.transaction_time <= ? AND t.valid_from <= ? AND t.valid_to > ?
LEFT JOIN salaries s ON e.emp_no = s.emp_no
  AND s.transaction_time <= ? AND s.valid_from <= ? AND s.valid_to > ?
LEFT JOIN dept_manager dm ON e.emp_no = dm.emp_no
  AND dm.transaction_time <= ? AND dm.valid_from <= ? AND dm.valid_to > ?
WHERE e.emp_no = ?
  AND e.transaction_time <= ? AND e.valid_from <= ? AND e.valid_to > ?
ORDER BY e.transaction_time DESC, de.transaction_time DESC, t.transaction_time DESC, s.transaction_time DESC
LIMIT 1;

-- Get department organization chart (current managers and employee count)
-- name: GetDepartmentOrgChart :many
SELECT 
  d.dept_no, d.dept_name,
  e.emp_no as manager_emp_no, e.first_name as manager_first_name, e.last_name as manager_last_name,
  COUNT(DISTINCT de.emp_no) as employee_count,
  ROUND(AVG(s.salary), 2) as avg_salary
FROM departments d
LEFT JOIN dept_manager dm ON d.dept_no = dm.dept_no
  AND dm.valid_from <= ? AND dm.valid_to > ?
LEFT JOIN employees e ON dm.emp_no = e.emp_no
  AND e.valid_from <= ? AND e.valid_to > ?
LEFT JOIN dept_emp de ON d.dept_no = de.dept_no
  AND de.valid_from <= ? AND de.valid_to > ?
LEFT JOIN salaries s ON de.emp_no = s.emp_no
  AND s.valid_from <= ? AND s.valid_to > ?
WHERE d.valid_from <= ? AND d.valid_to > ?
GROUP BY d.dept_no, d.dept_name, e.emp_no, e.first_name, e.last_name
ORDER BY d.dept_no;

-- Get all changes for an employee (audit trail)
-- name: GetEmployeeAuditTrail :many
SELECT 
  'employee' as change_type,
  transaction_time,
  valid_from,
  valid_to,
  first_name || ' ' || last_name as description
FROM employees 
WHERE employees.emp_no = ?
UNION ALL
SELECT 
  'department' as change_type,
  de.transaction_time,
  de.valid_from,
  de.valid_to,
  'Assigned to ' || d.dept_name as description
FROM dept_emp de
JOIN departments d ON de.dept_no = d.dept_no
WHERE de.emp_no = ?
UNION ALL
SELECT 
  'title' as change_type,
  transaction_time,
  valid_from,
  valid_to,
  'Title: ' || title as description
FROM titles
WHERE titles.emp_no = ?
UNION ALL
SELECT 
  'salary' as change_type,
  transaction_time,
  valid_from,
  valid_to,
  'Salary: $' || salary as description
FROM salaries
WHERE salaries.emp_no = ?
UNION ALL
SELECT 
  'management' as change_type,
  dm.transaction_time,
  dm.valid_from,
  dm.valid_to,
  'Manager of ' || d.dept_name as description
FROM dept_manager dm
JOIN departments d ON dm.dept_no = d.dept_no
WHERE dm.emp_no = ?
ORDER BY transaction_time DESC, valid_from DESC;

-- Get payroll summary for department at specific date
-- name: GetDepartmentPayroll :many
SELECT 
  e.emp_no, e.first_name, e.last_name,
  t.title,
  s.salary,
  CASE WHEN dm.emp_no IS NOT NULL THEN 'Manager' ELSE 'Employee' END as role
FROM employees e
JOIN dept_emp de ON e.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
JOIN titles t ON e.emp_no = t.emp_no
JOIN salaries s ON e.emp_no = s.emp_no
LEFT JOIN dept_manager dm ON e.emp_no = dm.emp_no AND de.dept_no = dm.dept_no
WHERE d.dept_no = ?
  AND e.valid_from <= ? AND e.valid_to > ?
  AND de.valid_from <= ? AND de.valid_to > ?
  AND t.valid_from <= ? AND t.valid_to > ?
  AND s.valid_from <= ? AND s.valid_to > ?
  AND d.valid_from <= ? AND d.valid_to > ?
  AND (dm.emp_no IS NULL OR (dm.valid_from <= ? AND dm.valid_to > ?))
ORDER BY s.salary DESC;

-- Find employees who held multiple positions simultaneously
-- name: GetEmployeesWithOverlappingRoles :many
SELECT DISTINCT
  e.emp_no, e.first_name, e.last_name,
  COUNT(DISTINCT de.dept_no) as dept_count,
  COUNT(DISTINCT t.title) as title_count
FROM employees e
JOIN dept_emp de ON e.emp_no = de.emp_no
JOIN titles t ON e.emp_no = t.emp_no
WHERE de.valid_from <= ? AND de.valid_to > ?
  AND t.valid_from <= ? AND t.valid_to > ?
  AND e.valid_from <= ? AND e.valid_to > ?
GROUP BY e.emp_no, e.first_name, e.last_name
HAVING COUNT(DISTINCT de.dept_no) > 1 OR COUNT(DISTINCT t.title) > 1
ORDER BY dept_count DESC, title_count DESC;