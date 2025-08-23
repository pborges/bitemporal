update salaries
set transaction_to = current_timestamp
where emp_no = 10009
  AND valid_to >= '1990-01-01'
  AND valid_from < '1995-01-01'
  AND transaction_from <= current_timestamp
  AND transaction_to > current_timestamp;

insert into salaries (emp_no, salary, valid_from, valid_to, transaction_from, transaction_to)
select emp_no,
       salary,
       min(valid_from),
       max(valid_to),
       min(transaction_from),
       max(transaction_to)
FROM (select emp_no,
             salary,
             valid_from,
             '1990-01-01'          valid_to,
             current_timestamp     transaction_from,
             '9999-12-31 23:59:59' transaction_to
      from salaries
      where emp_no = 10009
        AND valid_to > '1990-01-01'
        AND valid_from < '1990-01-01'
        AND transaction_from <= current_timestamp
        AND transaction_to >= current_timestamp
      UNION ALL
      select emp_no,
             69                      salary,
             case
                 when valid_from < '1990-01-01'
                     then '1990-01-01'
                 else valid_from end valid_from,
             case
                 when valid_to >= '1995-01-01'
                     then '1995-01-01'
                 else valid_to end   valid_to,
             current_timestamp       transaction_from,
             '9999-12-31 23:59:59'   transaction_to
      FROM salaries
      where emp_no = 10009
        AND valid_to > '1990-01-01'
        AND valid_from < '1995-01-01'
        AND transaction_from <= current_timestamp
        AND transaction_to >= current_timestamp
      UNION ALL
      select emp_no,
             salary,
             '1995-01-01'          valid_from,
             valid_to,
             current_timestamp     transaction_from,
             '9999-12-31 23:59:59' transaction_to
      from salaries
      where emp_no = 10009
        AND valid_to > '1995-01-01'
        AND valid_from < '1995-01-01'
        AND transaction_from <= current_timestamp
        AND transaction_to >= current_timestamp)
group by emp_no, salary;

-- as of in the past
select *
from salaries
where emp_no = 10009
  and transaction_from <= '2025-08-23 08:31:02.651245-07:00'
  and transaction_to > '2025-08-23 08:31:02.651245-07:00'
order by valid_from;

-- as of now
select *
from salaries
where emp_no = 10009
  and transaction_from <= current_timestamp
  and transaction_to > current_timestamp
order by valid_from;

-- look for gaps
-- Find gaps in valid time coverage for employees
WITH salaries_periods AS (
    SELECT
        emp_no,
        valid_from,
        valid_to,
        LAG(valid_to) OVER (PARTITION BY emp_no ORDER BY
            valid_from) as prev_valid_to
    FROM salaries
    WHERE transaction_to <= '2025-08-23 08:31:02.651245-07:00'
),
     gaps AS (
         SELECT
             emp_no,
             prev_valid_to as gap_start,
             valid_from as gap_end,
             julianday(valid_from) - julianday(prev_valid_to)
                           as gap_days
         FROM salaries_periods
         WHERE prev_valid_to IS NOT NULL
           AND prev_valid_to < valid_from
     )
SELECT
    emp_no,
    gap_start,
    gap_end,
    gap_days
FROM gaps
ORDER BY emp_no, gap_start;
