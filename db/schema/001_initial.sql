-- Employees table with bitemporal fields
CREATE TABLE IF NOT EXISTS employees (
    emp_no INTEGER PRIMARY KEY,
    birth_date DATE NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender TEXT NOT NULL CHECK (gender IN ('M', 'F')),
    hire_date DATE NOT NULL,
    -- Bitemporal fields
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_time DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Departments table with bitemporal fields
CREATE TABLE IF NOT EXISTS departments (
    dept_no TEXT PRIMARY KEY,
    dept_name TEXT NOT NULL,
    -- Bitemporal fields
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_time DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Department managers with bitemporal fields
CREATE TABLE IF NOT EXISTS dept_manager (
    emp_no INTEGER NOT NULL,
    dept_no TEXT NOT NULL,
    -- Bitemporal fields
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (emp_no, dept_no, valid_from)
);

-- Department employees with bitemporal fields
CREATE TABLE IF NOT EXISTS dept_emp (
    emp_no INTEGER NOT NULL,
    dept_no TEXT NOT NULL,
    -- Bitemporal fields
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (emp_no, dept_no, valid_from)
);

-- Titles with bitemporal fields
CREATE TABLE IF NOT EXISTS titles (
    emp_no INTEGER NOT NULL,
    title TEXT NOT NULL,
    -- Bitemporal fields
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (emp_no, title, valid_from)
);

-- Salaries with bitemporal fields
CREATE TABLE IF NOT EXISTS salaries (
    emp_no INTEGER NOT NULL,
    salary INTEGER NOT NULL,
    -- Bitemporal fields
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31 23:59:59',
    transaction_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (emp_no, valid_from)
);

-- Indexes for bitemporal queries
CREATE INDEX IF NOT EXISTS idx_employees_bitemporal ON employees (emp_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_employees_transaction ON employees (emp_no, transaction_time);

CREATE INDEX IF NOT EXISTS idx_departments_bitemporal ON departments (dept_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_departments_transaction ON departments (dept_no, transaction_time);

CREATE INDEX IF NOT EXISTS idx_dept_manager_bitemporal ON dept_manager (emp_no, dept_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_dept_manager_transaction ON dept_manager (emp_no, dept_no, transaction_time);

CREATE INDEX IF NOT EXISTS idx_dept_emp_bitemporal ON dept_emp (emp_no, dept_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_dept_emp_transaction ON dept_emp (emp_no, dept_no, transaction_time);

CREATE INDEX IF NOT EXISTS idx_titles_bitemporal ON titles (emp_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_titles_transaction ON titles (emp_no, transaction_time);

CREATE INDEX IF NOT EXISTS idx_salaries_bitemporal ON salaries (emp_no, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_salaries_transaction ON salaries (emp_no, transaction_time);