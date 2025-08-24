package bitemporal

import (
	"database/sql"
	_ "embed"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const debug = true

//go:embed sql/test_data.sql
var tempDataQuery string

type SalaryRow struct {
	EmpNo           int64
	Salary          int64
	ValidFrom       string
	ValidTo         string
	TransactionFrom string
	TransactionTo   string
}

func getSalariesUpdateWindow(t *testing.T, db *sql.DB, empNo int64, salary int64, validFrom, validTo string) []SalaryRow {
	validToT, err := time.Parse(time.DateOnly, validTo)
	if err != nil {
		t.Error(err)
	}
	validFromT, err := time.Parse(time.DateOnly, validFrom)
	if err != nil {
		t.Error(err)
	}

	frag, err := CreatePeriodsQuery(UpdateWindow{
		Table:     "salaries",
		Select:    []string{"emp_no", "salary"},
		FilterBy:  []string{"emp_no"},
		ValidFrom: validFromT,
		ValidTo:   validToT,
		Values:    map[string]interface{}{"emp_no": empNo, "salary": salary},
	})
	if err != nil {
		t.Error(err)
	}

	rows, err := db.Query(frag.Query, frag.Args()...)
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
	if debug {
		PrintTable(salary, validFrom, validTo, salaryRows)
	}
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

func TestCreateTestDB(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	rows, err := db.Query("SELECT emp_no, salary, valid_from, valid_to, transaction_from, transaction_to FROM salaries ORDER BY valid_from")
	if err != nil {
		t.Fatalf("Failed to query salaries: %v", err)
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

	expectedCount := 18
	if len(salaryRows) != expectedCount {
		t.Errorf("Expected %d rows, got %d", expectedCount, len(salaryRows))
	}

	if debug {
		PrintTable(0, "", "", salaryRows)
	}
}

func TestValidFromAndValidTooNotOnExistingPeriodBoundaries(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	updateStart := "1995-01-01"
	updateEnd := "2000-01-01"

	rows := getSalariesUpdateWindow(t, db, 10009, 69, updateStart, updateEnd)
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

	// validFrom is 1986-02-18, which is an existing period boundary (end of one period, start of next)
	validFrom := "1986-02-18"
	validTo := "1990-01-01"

	rows := getSalariesUpdateWindow(t, db, 10009, 42, validFrom, validTo)

	// Verify that no row should end exactly at validFrom since it's already a boundary
	for _, row := range rows {
		if row.ValidTo == validFrom+" 00:00:00" {
			t.Errorf("Found row ending at existing boundary %s, which shouldn't happen", validFrom)
		}
	}

	// Verify rows within the update window have the new salary
	for _, row := range rows {
		if row.ValidFrom >= validFrom+" 00:00:00" && row.ValidTo <= validTo+" 00:00:00" {
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

	rows := getSalariesUpdateWindow(t, db, 10009, 55, updateStart, updateEnd)

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

	rows := getSalariesUpdateWindow(t, db, 10009, 33, updateStart, updateEnd)

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

	rows := getSalariesUpdateWindow(t, db, 10009, 77, updateStart, updateEnd)

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
//	rows := getSalariesUpdateWindow(t, db, 10009, 88, updateStart, updateEnd)
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
