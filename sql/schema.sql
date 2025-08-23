-- Employees table with bitemporal fields
CREATE TABLE IF NOT EXISTS employees
(
    row_id           INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    emp_no           INTEGER  NOT NULL,
    birth_date       DATE     NOT NULL,
    first_name       TEXT     NOT NULL,
    last_name        TEXT     NOT NULL,
    gender           TEXT     NOT NULL CHECK (gender IN ('M', 'F', 'O')),
    hire_date        DATE     NOT NULL,
    -- Bitemporal fields
    valid_from       DATETIME NOT NULL,
    valid_to         DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transaction_to   DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59'
);

-- Departments table with bitemporal fields
CREATE TABLE IF NOT EXISTS departments
(
    row_id           INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    dept_no          TEXT     NOT NULL,
    dept_name        TEXT     NOT NULL,
    -- Bitemporal fields
    valid_from       DATETIME NOT NULL,
    valid_to         DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transaction_to   DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59'
);

-- Department managers with bitemporal fields
CREATE TABLE IF NOT EXISTS dept_manager
(
    row_id           INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    emp_no           INTEGER  NOT NULL,
    dept_no          TEXT     NOT NULL,
    -- Bitemporal fields
    valid_from       DATETIME NOT NULL,
    valid_to         DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transaction_to   DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59'
);

-- Department employees with bitemporal fields
CREATE TABLE IF NOT EXISTS dept_emp
(
    row_id           INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    emp_no           INTEGER  NOT NULL,
    dept_no          TEXT     NOT NULL,
    -- Bitemporal fields
    valid_from       DATETIME NOT NULL,
    valid_to         DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transaction_to   DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59'
);

-- Titles with bitemporal fields
CREATE TABLE IF NOT EXISTS titles
(
    row_id           INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    emp_no           INTEGER  NOT NULL,
    title            TEXT     NOT NULL,
    -- Bitemporal fields
    valid_from       DATETIME NOT NULL,
    valid_to         DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transaction_to   DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59'
);

-- Salaries with bitemporal fields
CREATE TABLE IF NOT EXISTS salaries
(
    row_id           INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,
    emp_no           INTEGER  NOT NULL,
    salary           INTEGER  NOT NULL,
    -- Bitemporal fields
    valid_from       DATETIME NOT NULL,
    valid_to         DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_from DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    transaction_to   DATETIME NOT NULL DEFAULT '9999-12-31 23:59:59'
);

-- Indexes for bitemporal queries
CREATE INDEX IF NOT EXISTS idx_employees_bitemporal ON employees (emp_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_employees_transaction ON employees (emp_no, transaction_from, transaction_to);

CREATE INDEX IF NOT EXISTS idx_departments_bitemporal ON departments (dept_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_departments_transaction ON departments (dept_no, transaction_from, transaction_to);

CREATE INDEX IF NOT EXISTS idx_dept_manager_bitemporal ON dept_manager (emp_no, dept_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_dept_manager_transaction ON dept_manager (emp_no, dept_no, transaction_from, transaction_to);

CREATE INDEX IF NOT EXISTS idx_dept_emp_bitemporal ON dept_emp (emp_no, dept_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_dept_emp_transaction ON dept_emp (emp_no, dept_no, transaction_from, transaction_to);

CREATE INDEX IF NOT EXISTS idx_titles_bitemporal ON titles (emp_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_titles_transaction ON titles (emp_no, transaction_from, transaction_to);

CREATE INDEX IF NOT EXISTS idx_salaries_bitemporal ON salaries (emp_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_salaries_transaction ON salaries (emp_no, transaction_from, transaction_to);