package model

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"text/template"

	_ "github.com/mattn/go-sqlite3"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
)

const debug = true

var tempDataQuery = `
DROP TABLE IF EXISTS salaries;
CREATE TABLE salaries (
    row_id INTEGER,
    emp_no INTEGER,
    salary INTEGER,
    valid_from DATETIME,
    valid_to DATETIME,
    transaction_from DATETIME,
    transaction_to DATETIME
);

INSERT INTO salaries VALUES
    (89,10009,60929,'1985-02-18','1986-02-18','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (90,10009,64604,'1986-02-18','1987-02-18','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (91,10009,64780,'1987-02-18','1988-02-18','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (92,10009,66302,'1988-02-18','1989-02-17','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (93,10009,69042,'1989-02-17','1990-02-17','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (94,10009,70889,'1990-02-17','1991-02-17','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (95,10009,71434,'1991-02-17','1992-02-17','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (96,10009,74612,'1992-02-17','1993-02-16','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (97,10009,76518,'1993-02-16','1994-02-16','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (98,10009,78335,'1994-02-16','1995-02-16','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (99,10009,80944,'1995-02-16','1996-02-16','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (100,10009,82507,'1996-02-16','1997-02-15','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (101,10009,85875,'1997-02-15','1998-02-15','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (102,10009,89324,'1998-02-15','1999-02-15','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (103,10009,90668,'1999-02-15','2000-02-15','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (104,10009,93507,'2000-02-15','2001-02-14','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (105,10009,94443,'2001-02-14','2002-02-14','2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59')),
    (106,10009,94409,'2002-02-14',DATETIME('9999-12-31 23:59:59'),'2025-08-23 08:55:49.371425-07:00',DATETIME('9999-12-31 23:59:59'))
`

type model struct {
	EmpNo     int64
	Salary    int64
	ValidFrom string
	ValidTo   string
}

type SalaryRow struct {
	EmpNo           int64
	Salary          int64
	ValidFrom       string
	ValidTo         string
	TransactionFrom string
	TransactionTo   string
}

var updateTempl = `
-- Before segment: only create if there's actual time before updateStart
select emp_no,
       salary,
       DATETIME(valid_from),
       DATETIME('{{ .ValidFrom }}')           valid_to,
       DATETIME(current_timestamp)      transaction_from,
       DATETIME('9999-12-31 23:59:59') transaction_to
from salaries
where emp_no = {{ .EmpNo }}
  AND valid_from < DATETIME('{{ .ValidFrom }}')
  AND valid_to > DATETIME('{{ .ValidFrom }}')
  AND DATETIME(valid_from) < DATETIME('{{ .ValidFrom }}')  -- Ensure non-zero duration
  AND transaction_from <= current_timestamp
  AND transaction_to >= current_timestamp
UNION ALL
-- Update segment: the new salary for the update window
select emp_no,
       {{ .Salary }}                      salary,
       DATETIME(case
           when valid_from < DATETIME('{{ .ValidFrom }}')
               then DATETIME('{{ .ValidFrom }}')
           else valid_from end) valid_from,
       DATETIME(case
           when valid_to >= DATETIME('{{ .ValidTo }}')
               then DATETIME('{{ .ValidTo }}')
           else valid_to end)   valid_to,
       DATETIME(current_timestamp)       transaction_from,
       DATETIME('9999-12-31 23:59:59')   transaction_to
FROM salaries
where emp_no = {{ .EmpNo }}
  AND valid_from < DATETIME('{{ .ValidTo }}')
  AND valid_to > DATETIME('{{ .ValidFrom }}')
  AND transaction_from <= current_timestamp
  AND transaction_to >= current_timestamp
  -- Ensure the calculated period has positive duration
  AND DATETIME(case when valid_from < DATETIME('{{ .ValidFrom }}') then DATETIME('{{ .ValidFrom }}') else valid_from end) 
      < DATETIME(case when valid_to >= DATETIME('{{ .ValidTo }}') then DATETIME('{{ .ValidTo }}') else valid_to end)
UNION ALL
-- After segment: preserve portions of records that extend beyond updateEnd
-- Only for records that START BEFORE updateEnd but extend beyond it
select emp_no,
       salary,
       DATETIME('{{ .ValidTo }}')           valid_from,
       DATETIME(valid_to),
       DATETIME(current_timestamp)      transaction_from,
       DATETIME('9999-12-31 23:59:59') transaction_to
from salaries
where emp_no = {{ .EmpNo }}
  AND valid_from < DATETIME('{{ .ValidTo }}')    -- Must start BEFORE updateEnd
  AND valid_to > DATETIME('{{ .ValidTo }}')      -- Must end AFTER updateEnd  
  AND DATETIME('{{ .ValidTo }}') < DATETIME(valid_to)  -- Ensure positive duration
  -- Explicit exclusion: do not include records that start exactly at updateEnd
  AND DATETIME(valid_from) != DATETIME('{{ .ValidTo }}')
  AND transaction_from <= current_timestamp
  AND transaction_to >= current_timestamp
UNION ALL
-- New period segment: create new record when update window has no overlap with existing data
-- This handles cases where the update is entirely before or after existing data
SELECT {{ .EmpNo }} as emp_no,
       {{ .Salary }} as salary,
       DATETIME('{{ .ValidFrom }}') as valid_from,
       DATETIME('{{ .ValidTo }}') as valid_to,
       DATETIME(current_timestamp) as transaction_from,
       DATETIME('9999-12-31 23:59:59') as transaction_to
WHERE NOT EXISTS (
    SELECT 1 FROM salaries 
    WHERE emp_no = {{ .EmpNo }}
      AND valid_from < DATETIME('{{ .ValidTo }}')
      AND valid_to > DATETIME('{{ .ValidFrom }}')
      AND transaction_from <= current_timestamp
      AND transaction_to >= current_timestamp
)
UNION ALL
-- Extension segment: create record for portion of update window before earliest existing data
SELECT {{ .EmpNo }} as emp_no,
       {{ .Salary }} as salary,
       DATETIME('{{ .ValidFrom }}') as valid_from,
       (SELECT MIN(DATETIME(valid_from)) FROM salaries 
        WHERE emp_no = {{ .EmpNo }}
          AND transaction_from <= current_timestamp 
          AND transaction_to >= current_timestamp) as valid_to,
       DATETIME(current_timestamp) as transaction_from,
       DATETIME('9999-12-31 23:59:59') as transaction_to
WHERE DATETIME('{{ .ValidFrom }}') < (
    SELECT MIN(valid_from) FROM salaries 
    WHERE emp_no = {{ .EmpNo }}
      AND transaction_from <= current_timestamp 
      AND transaction_to >= current_timestamp
)
AND EXISTS (
    SELECT 1 FROM salaries 
    WHERE emp_no = {{ .EmpNo }}
      AND valid_from < DATETIME('{{ .ValidTo }}')
      AND valid_to > DATETIME('{{ .ValidFrom }}')
      AND transaction_from <= current_timestamp
      AND transaction_to >= current_timestamp
)
ORDER BY valid_from;
`

func updateWindowQuery(empNo int64, salary int64, validFrom, validTo string) string {
	m := model{
		EmpNo:     empNo,
		Salary:    salary,
		ValidFrom: validFrom,
		ValidTo:   validTo,
	}

	tmpl, err := template.New("update").Parse(updateTempl)
	if err != nil {
		panic(err)
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, m)
	if err != nil {
		panic(err)
	}

	return buf.String()
}

func highlight(validTo string, validFrom string, salary int64, str string) string {
	if strings.HasPrefix(str, validTo) {
		return fmt.Sprintf("\033[31m%s\033[0m", str)
	}
	if strings.HasPrefix(str, validFrom) {
		return fmt.Sprintf("\033[32m%s\033[0m", str)
	}
	if str == strconv.Itoa(int(salary)) {
		return fmt.Sprintf("\033[34m%s\033[0m", str)
	}
	return str
}

func printTable(salary int64, validFrom string, validTo string, rows []SalaryRow) {
	table := tablewriter.NewTable(os.Stdout,
		tablewriter.WithConfig(tablewriter.Config{
			Footer: tw.CellConfig{
				Formatting: tw.CellFormatting{MergeMode: tw.MergeHorizontal},
				Alignment:  tw.CellAlignment{Global: tw.AlignLeft},
			},
		}),
	)
	table.Header([]string{"EmpNo", "Salary", "ValidFrom", "ValidTo", "TransactionFrom", "TransactionTo"})
	footer := fmt.Sprintf("Salary: %s\nFrom  : %s\nTo    : %s",
		highlight(validTo, validFrom, salary, strconv.Itoa(int(salary))),
		highlight(validTo, validFrom, salary, validFrom),
		highlight(validTo, validFrom, salary, validTo),
	)
	table.Footer(footer, footer, footer, footer, footer, footer)

	for _, row := range rows {
		table.Append([]string{
			fmt.Sprintf("%d", row.EmpNo),
			highlight(validTo, validFrom, salary, strconv.Itoa(int(row.Salary))),
			highlight(validTo, validFrom, row.Salary, row.ValidFrom),
			highlight(validTo, validFrom, row.Salary, row.ValidTo),
			row.TransactionFrom,
			row.TransactionTo,
		})
	}

	table.Render()
}

func getUpdateWindow(t *testing.T, db *sql.DB, empNo int64, salary int64, validFrom, validTo string) []SalaryRow {
	var query = updateWindowQuery(empNo, salary, validFrom, validTo)
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	var salaryRows []SalaryRow
	for rows.Next() {
		var row SalaryRow
		err = rows.Scan(&row.EmpNo, &row.Salary, &row.ValidFrom, &row.ValidTo, &row.TransactionFrom, &row.TransactionTo)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		salaryRows = append(salaryRows, row)
	}
	//if debug == false {
	printTable(salary, validFrom, validTo, salaryRows)
	//}
	return salaryRows
}

func createTestDB(t *testing.T) (*sql.DB, func()) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}

	_, err = db.Exec(tempDataQuery)
	if err != nil {
		t.Fatalf("Failed to execute tempDataQuery: %v", err)
	}
	return db, func() {
		db.Close()
	}
}

func validateTable(t *testing.T, rows []SalaryRow) {
	if len(rows) == 0 {
		return // Empty table is valid
	}

	// Check for zero-duration records (validFrom == validTo)
	for i, row := range rows {
		if row.ValidFrom == row.ValidTo {
			t.Errorf("Row %d has zero duration: ValidFrom=%s equals ValidTo=%s",
				i, row.ValidFrom, row.ValidTo)
		}
	}

	// Verify rows are ordered by validFrom
	for i := 1; i < len(rows); i++ {
		if rows[i].ValidFrom < rows[i-1].ValidFrom {
			t.Errorf("Rows not ordered by ValidFrom: row %d (%s) comes before row %d (%s)",
				i, rows[i].ValidFrom, i-1, rows[i-1].ValidFrom)
		}
	}

	// Check for gaps between consecutive rows
	for i := 1; i < len(rows); i++ {
		prevValidTo := rows[i-1].ValidTo
		currValidFrom := rows[i].ValidFrom

		if prevValidTo != currValidFrom {
			t.Errorf("Gap found between row %d and %d: row %d ends at %s but row %d starts at %s",
				i-1, i, i-1, prevValidTo, i, currValidFrom)
		}
	}
}

func TestValidFromAndValidTooNotOnExistingPeriodBoundaries(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	updateStart := "1995-01-01"
	updateEnd := "2000-01-01"

	rows := getUpdateWindow(t, db, 10009, 69, updateStart, updateEnd)
	validateTable(t, rows)

	for _, row := range rows {
		if row.ValidFrom >= updateStart && row.ValidTo <= updateEnd {
			if row.Salary != 69 {
				t.Errorf("Expected salary 69 for row with ValidFrom=%s ValidTo=%s, got %d",
					row.ValidFrom, row.ValidTo, row.Salary)
			}
		}
	}

	// Verify first row ends at updateStart with existing salary
	if len(rows) > 0 {
		firstRow := rows[0]
		expectedValidTo := updateStart + " 00:00:00"
		if firstRow.ValidTo != expectedValidTo {
			t.Errorf("Expected first row ValidTo to be %s, got %s", expectedValidTo, firstRow.ValidTo)
		}
		if firstRow.Salary != 78335 {
			t.Errorf("Expected first row to have existing salary, got %d", firstRow.Salary)
		}
	}

	// Verify last row starts at updateEnd with existing salary
	if len(rows) > 0 {
		lastRow := rows[len(rows)-1]
		expectedValidFrom := updateEnd + " 00:00:00"
		if lastRow.ValidFrom != expectedValidFrom {
			t.Errorf("Expected last row ValidFrom to be %s, got %s", expectedValidFrom, lastRow.ValidFrom)
		}
		if lastRow.Salary != 90668 {
			t.Errorf("Expected last row to have existing salary, got %d", lastRow.Salary)
		}
	}

	validateTable(t, rows)
}

func TestUpdateStartOnExistingPeriodBoundary(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// updateStart is 1986-02-18, which is an existing period boundary (end of one period, start of next)
	updateStart := "1986-02-18"
	updateEnd := "1990-01-01"

	rows := getUpdateWindow(t, db, 10009, 42, updateStart, updateEnd)

	// Verify that no row should end exactly at updateStart since it's already a boundary
	for _, row := range rows {
		if row.ValidTo == updateStart+" 00:00:00" {
			t.Errorf("Found row ending at existing boundary %s, which shouldn't happen", updateStart)
		}
	}

	// Verify rows within the update window have the new salary
	for _, row := range rows {
		if row.ValidFrom >= updateStart+" 00:00:00" && row.ValidTo <= updateEnd+" 00:00:00" {
			if row.Salary != 42 {
				t.Errorf("Expected salary 42 for row with ValidFrom=%s ValidTo=%s, got %d",
					row.ValidFrom, row.ValidTo, row.Salary)
			}
		}
	}

	validateTable(t, rows)
}

func TestUpdateEndOnExistingPeriodBoundary(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// updateEnd is 2000-02-15, which is an existing period boundary (end of one period)
	updateStart := "1995-01-01"
	updateEnd := "2000-02-15"

	rows := getUpdateWindow(t, db, 10009, 55, updateStart, updateEnd)

	// When updateEnd coincides with an existing boundary, it's correct to have
	// NO record starting at that boundary because it's outside the update window
	for _, row := range rows {
		if row.ValidFrom == updateEnd+" 00:00:00" {
			t.Errorf("Found unexpected record starting at boundary %s - records starting at updateEnd should not be included", updateEnd)
		}
	}

	// Verify rows within the update window have the new salary
	for _, row := range rows {
		if row.ValidFrom >= updateStart+" 00:00:00" && row.ValidTo <= updateEnd+" 00:00:00" {
			if row.Salary != 55 {
				t.Errorf("Expected salary 55 for row with ValidFrom=%s ValidTo=%s, got %d",
					row.ValidFrom, row.ValidTo, row.Salary)
			}
		}
	}

	validateTable(t, rows)
}

func TestUpdateStartBeforeEarliestRecord(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// updateStart is 1980-01-01, which is before the earliest record (1985-02-18)
	updateStart := "1980-01-01"
	updateEnd := "1990-01-01"

	rows := getUpdateWindow(t, db, 10009, 33, updateStart, updateEnd)

	// Since updateStart is before any existing data, the first row should start at updateStart
	// to cover the entire requested period, not just the overlapping portion
	if len(rows) > 0 {
		firstRow := rows[0]
		expectedValidFrom := updateStart + " 00:00:00" // Should start at requested updateStart
		if firstRow.ValidFrom != expectedValidFrom {
			t.Errorf("Expected first row ValidFrom to be %s (requested start), got %s", expectedValidFrom, firstRow.ValidFrom)
		}
		if firstRow.Salary != 33 {
			t.Errorf("Expected first row to have new salary 33, got %d", firstRow.Salary)
		}
	}

	// Verify rows within the update window have the new salary
	for _, row := range rows {
		if row.ValidFrom >= updateStart+" 00:00:00" && row.ValidTo <= updateEnd+" 00:00:00" {
			if row.Salary != 33 {
				t.Errorf("Expected salary 33 for row with ValidFrom=%s ValidTo=%s, got %d",
					row.ValidFrom, row.ValidTo, row.Salary)
			}
		}
	}

	validateTable(t, rows)
}

func TestUpdateWindowEntirelyBeforeExistingData(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	// Both updateStart and updateEnd are before any existing data (earliest is 1985-02-18)
	updateStart := "1980-01-01"
	updateEnd := "1984-01-01"

	rows := getUpdateWindow(t, db, 10009, 77, updateStart, updateEnd)

	// Should return 1 row representing the requested period with the requested salary
	expectedRows := 1
	if len(rows) != expectedRows {
		t.Errorf("Expected %d row for update window before existing data, got %d rows", expectedRows, len(rows))
	}

	// Verify the single row has the correct salary and time period
	if len(rows) == 1 {
		row := rows[0]
		expectedValidFrom := updateStart + " 00:00:00"
		expectedValidTo := updateEnd + " 00:00:00"

		if row.ValidFrom != expectedValidFrom {
			t.Errorf("Expected ValidFrom %s, got %s", expectedValidFrom, row.ValidFrom)
		}
		if row.ValidTo != expectedValidTo {
			t.Errorf("Expected ValidTo %s, got %s", expectedValidTo, row.ValidTo)
		}
		if row.Salary != 77 {
			t.Errorf("Expected salary 77, got %d", row.Salary)
		}
	}

	validateTable(t, rows)
}

// disabling this test for now as the sample data end date is "the end of time"
// this test would be most useful if a salary is "deleted" by ending its last validTo before "end of time"
// Disabled
//func TestUpdateWindowEntirelyAfterExistingData(t *testing.T) {
//	db, cleanup := createTestDB(t)
//	defer cleanup()
//
//	// Both updateStart and updateEnd are after all existing data (latest ends ~2002)
//	updateStart := "2010-01-01"
//	updateEnd := "2015-01-01"
//
//	rows := getUpdateWindow(t, db, 10009, 88, updateStart, updateEnd)
//
//	// Should return 3 rows: before segment, update segment, and after segment
//	// This happens because the last existing record has valid_to = '9999-12-31 23:59:59'
//	// so it overlaps with our update window
//	expectedRows := 3
//	if len(rows) != expectedRows {
//		t.Errorf("Expected %d rows for update window after existing data (extends timeline), got %d rows", expectedRows, len(rows))
//	}
//
//	// Verify the update segment has the new salary
//	for _, row := range rows {
//		if row.ValidFrom >= updateStart+" 00:00:00" && row.ValidTo <= updateEnd+" 00:00:00" {
//			if row.Salary != 88 {
//				t.Errorf("Expected salary 88 for row with ValidFrom=%s ValidTo=%s, got %d",
//					row.ValidFrom, row.ValidTo, row.Salary)
//			}
//		}
//	}
//
//	validateTable(t, rows)
//}
