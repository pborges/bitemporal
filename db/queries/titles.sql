-- Bitemporal Employee Title Queries

-- Get current title for employee
-- name: GetEmployeeTitleCurrent :one
SELECT * FROM titles
WHERE emp_no = ? 
  AND valid_from <= ? 
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get employee title as it was known at specific transaction time
-- name: GetEmployeeTitleAsOfTransaction :one
SELECT * FROM titles
WHERE emp_no = ?
  AND transaction_time <= ?
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY transaction_time DESC
LIMIT 1;

-- Get complete history of titles for employee
-- name: GetEmployeeTitleHistory :many
SELECT * FROM titles
WHERE emp_no = ?
ORDER BY valid_from, transaction_time;

-- Get all employees with specific title (current)
-- name: GetEmployeesByTitleCurrent :many
SELECT * FROM titles
WHERE title = ?
  AND valid_from <= ?
  AND valid_to > ?
ORDER BY emp_no;

-- Get all current titles
-- name: ListCurrentTitles :many
SELECT DISTINCT title FROM titles
WHERE valid_from <= ?
  AND valid_to > ?
ORDER BY title;

-- Get title assignments as they existed at specific transaction time
-- name: ListTitlesAsOfTransaction :many
SELECT DISTINCT emp_no, title, valid_from, valid_to, transaction_time
FROM titles t1
WHERE t1.transaction_time <= ?
  AND t1.valid_from <= ?
  AND t1.valid_to > ?
  AND t1.transaction_time = (
    SELECT MAX(t2.transaction_time)
    FROM titles t2
    WHERE t2.emp_no = t1.emp_no
      AND t2.title = t1.title
      AND t2.transaction_time <= ?
  )
ORDER BY emp_no, title;

-- Create new title assignment
-- name: CreateTitle :one
INSERT INTO titles (
  emp_no, title, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?
)
RETURNING *;

-- Close current title (used before promotion/change)
-- name: CloseEmployeeTitleValidPeriod :exec
UPDATE titles 
SET valid_to = ?
WHERE emp_no = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Close specific title assignment
-- name: CloseSpecificTitleAssignment :exec
UPDATE titles 
SET valid_to = ?
WHERE emp_no = ?
  AND title = ?
  AND valid_to = '9999-12-31 23:59:59';

-- Insert updated title assignment (promotion/demotion)
-- name: InsertTitleUpdate :one
INSERT INTO titles (
  emp_no, title, valid_from, valid_to
) VALUES (
  ?, ?, ?, ?
)
RETURNING *;

-- Get title changes (promotions/demotions) in date range
-- name: GetTitleChanges :many
SELECT t1.emp_no, t1.title as old_title, t2.title as new_title,
       t1.valid_to as change_date
FROM titles t1
JOIN titles t2 ON t1.emp_no = t2.emp_no
WHERE t1.valid_to BETWEEN ? AND ?
  AND t2.valid_from = t1.valid_to
  AND t1.title != t2.title
ORDER BY t1.valid_to, t1.emp_no;

-- Get employees promoted to specific title in date range
-- name: GetPromotionsToTitle :many
SELECT t2.emp_no, t1.title as from_title, t2.title as to_title,
       t2.valid_from as promotion_date
FROM titles t1
JOIN titles t2 ON t1.emp_no = t2.emp_no
WHERE t2.title = ?
  AND t2.valid_from BETWEEN ? AND ?
  AND t1.valid_to = t2.valid_from
  AND t1.title != t2.title
ORDER BY t2.valid_from, t2.emp_no;

-- Count employees by title (current)
-- name: CountEmployeesByTitle :many
SELECT title, COUNT(*) as employee_count
FROM titles
WHERE valid_from <= ?
  AND valid_to > ?
GROUP BY title
ORDER BY employee_count DESC;