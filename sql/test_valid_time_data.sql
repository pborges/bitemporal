-- Sample data for an employee who changes their last name
-- Demonstrates bitemporal data patterns with valid time vs transaction time

-- Employee 12345: Jane Smith who gets married and changes name to Jane Johnson
-- Marriage date: 2023-06-15 (valid time - when the name change actually became effective)
-- HR recorded the change: 2023-07-01 (transaction time - when HR updated the system)

-- Initial record: Jane Smith
INSERT INTO employees (
    emp_no, birth_date, first_name, last_name, gender, hire_date,
    valid_open, valid_close, txn_open, txn_close
) VALUES (
    12345, '1990-03-15', 'Jane', 'Smith', 'F', '2020-01-15',
    '2020-01-15', '2023-06-15', '2020-01-15 09:00:00', '2023-07-01 14:30:00'
);

-- Updated record: Jane Johnson (after marriage)
-- Valid from marriage date, but recorded later by HR
INSERT INTO employees (
    emp_no, birth_date, first_name, last_name, gender, hire_date,
    valid_open, valid_close, txn_open, txn_close
) VALUES (
    12345, '1990-03-15', 'Jane', 'Johnson', 'F', '2020-01-15',
    '2023-06-15', '9999-12-31 23:59:59', '2023-07-01 14:30:00', '9999-12-31 23:59:59'
);

-- Late correction: HR discovers they recorded wrong marriage date
-- Actually got married on 2023-06-10, not 2023-06-15
-- This correction is recorded on 2023-08-15

-- Close the previous "Smith" record early (valid until actual marriage date)
INSERT INTO employees (
    emp_no, birth_date, first_name, last_name, gender, hire_date,
    valid_open, valid_close, txn_open, txn_close
) VALUES (
    12345, '1990-03-15', 'Jane', 'Smith', 'F', '2020-01-15',
    '2020-01-15', '2023-06-10', '2023-08-15 10:15:00', '9999-12-31 23:59:59'
);

-- Create corrected "Johnson" record (valid from actual marriage date)
INSERT INTO employees (
    emp_no, birth_date, first_name, last_name, gender, hire_date,
    valid_open, valid_close, txn_open, txn_close
) VALUES (
    12345, '1990-03-15', 'Jane', 'Johnson', 'F', '2020-01-15',
    '2023-06-10', '9999-12-31 23:59:59', '2023-08-15 10:15:00', '9999-12-31 23:59:59'
);
