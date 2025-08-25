package bitemporal_test

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/olekukonko/tablewriter"
	"github.com/pborges/bitemporal"
)

const debug = true

//go:embed sql/schema.sql
var schema string

//go:embed sql/test_window_data.sql
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

	frag, err := bitemporal.CreatePeriodsQuery(bitemporal.UpdateWindow{
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

	fmt.Println(frag.Query)

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
		PrintSalaryTable(salary, validFrom, validTo, salaryRows)
	}
	return salaryRows
}

func createTestDB(t *testing.T) (*sql.DB, func()) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}
	_, err = db.Exec(schema)
	if err != nil {
		t.Fatalf("Failed to execute schema: %v", err)
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
		PrintSalaryTable(0, "", "", salaryRows)
	}
}

func TestValidFromAndValidTooNotOnExistingPeriodBoundaries(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	updateStart := "1995-01-01"
	updateEnd := "2000-01-01"

	rows := getSalariesUpdateWindow(t, db, 10009, 42, updateStart, updateEnd)

	for _, row := range rows {
		if row.ValidFrom >= updateStart && row.ValidTo <= updateEnd {
			if row.Salary != 42 {
				t.Errorf("Expected salary 42 for row with ValidFrom=%s ValidTo=%s, got %d",
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

// TestRowCollapse verifies that the
func TestRowCollapse(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	var empNo = 10009
	var salary int64 = 42
	var validFrom = "1995-01-01"
	var validTo = "2000-01-01"

	frag, err := bitemporal.CreatePeriodsQuery(bitemporal.UpdateWindow{
		Table:     "salaries",
		Select:    []string{"emp_no", "salary"},
		FilterBy:  []string{"emp_no"},
		ValidFrom: bitemporal.AsTime(validFrom),
		ValidTo:   bitemporal.AsTime(validTo),
		Values:    map[string]interface{}{"emp_no": empNo, "salary": salary},
	})
	if err != nil {
		t.Error(err)
	}

	query := fmt.Sprintf("SELECT emp_no,salary,min(valid_from),max(valid_to),transaction_from,transaction_to FROM (%s) GROUP BY emp_no,salary ORDER BY valid_from", frag.Query)

	rows, err := db.Query(query, frag.Args()...)
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
		PrintSalaryTable(salary, validFrom, validTo, salaryRows)
	}

	// Assert that consecutive rows with same salary get collapsed into a single row
	// The update window (1995-01-01 to 2000-01-01) should create multiple rows with salary 42
	// that get collapsed into one row spanning the entire period
	expectedRows := 1 // Should collapse to a single row for the update period
	actualRows := 0

	for _, row := range salaryRows {
		if row.Salary == salary && row.ValidFrom >= validFrom+" 00:00:00" && row.ValidTo <= validTo+" 00:00:00" {
			actualRows++
		}
	}

	if actualRows != expectedRows {
		t.Errorf("Expected %d collapsed row(s) for salary %d in update window, got %d", expectedRows, salary, actualRows)
	}

	// Verify the collapsed row spans the entire update window
	if len(salaryRows) > 0 {
		for _, row := range salaryRows {
			if row.Salary == salary {
				expectedValidFrom := validFrom + " 00:00:00"
				expectedValidTo := validTo + " 00:00:00"

				if row.ValidFrom != expectedValidFrom {
					t.Errorf("Expected collapsed row ValidFrom to be %s, got %s", expectedValidFrom, row.ValidFrom)
				}
				if row.ValidTo != expectedValidTo {
					t.Errorf("Expected collapsed row ValidTo to be %s, got %s", expectedValidTo, row.ValidTo)
				}
			}
		}
	}
}

func rowsToMaps(rows *sql.Rows) ([]string, []map[string]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var results []map[string]any

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, err
		}

		row := make(map[string]any)
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	return columns, results, nil
}

func printMap(columns []string, rows []map[string]any) {
	table := tablewriter.NewWriter(os.Stdout)
	table.Header(columns)

	for _, row := range rows {
		rowData := make([]string, len(columns))
		for i, col := range columns {
			if val, ok := row[col]; ok {
				rowData[i] = fmt.Sprintf("%v", val)
			} else {
				rowData[i] = ""
			}
		}
		table.Append(rowData)
	}

	table.Render()
}

func TestUpdateErasingHistory(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	args := []any{
		sql.Named("emp_no", 10009),
		sql.Named("salary", 42),
		sql.Named("valid_from", "1995-01-01"),
		sql.Named("valid_to", "2000-01-01"),
		sql.Named("txn_moment", time.Now()),
		sql.Named("infinity", bitemporal.EndOfTime),
	}

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}

	// View current snapshot
	rows, err := tx.Query("SELECT * "+
		"FROM salaries WHERE "+
		"emp_no = @emp_no "+
		"AND ("+
		"    (@valid_from BETWEEN valid_from AND valid_to)"+
		" OR (valid_from >= @valid_from AND valid_to <= @valid_to)"+
		" OR (@valid_to BETWEEN valid_from AND valid_to)"+
		") "+
		"AND @txn_moment >= transaction_from AND @txn_moment < transaction_to "+
		"ORDER BY valid_from",
		args...,
	)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()
	columns, res, err := rowsToMaps(rows)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Before Update")
	printMap(columns, res)

	// Close the transaction periods of the rows to be affected
	_, err = tx.Exec("UPDATE salaries "+
		"SET transaction_to = @txn_moment "+
		"WHERE emp_no = @emp_no "+
		"AND ("+
		"    (@valid_from BETWEEN valid_from AND valid_to)"+
		" OR (valid_from >= @valid_from AND valid_to <=	 @valid_to)"+
		" OR (@valid_to BETWEEN valid_from AND valid_to)"+
		") "+
		"AND @txn_moment >= transaction_from AND @txn_moment < transaction_to",
		args...)
	if err != nil {
		t.Fatal(err)
	}

	// Insert any history that exists outside the requested period
	_, err = tx.Exec("INSERT INTO salaries (emp_no, salary, valid_from, valid_to, transaction_from, transaction_to) "+
		"SELECT emp_no, salary, valid_from, @valid_from valid_to, @txn_moment, @infinity FROM salaries WHERE emp_no = @emp_no AND @valid_from > valid_from AND @valid_from < valid_to "+
		"UNION ALL "+
		"SELECT emp_no, salary, @valid_to valid_from, valid_to, @txn_moment, @infinity FROM salaries WHERE emp_no = @emp_no AND @valid_to > valid_from AND @valid_to < valid_to",
		args...)
	if err != nil {
		t.Fatal(err)
	}

	// Insert new history
	_, err = tx.Exec("INSERT INTO salaries (emp_no, salary, valid_from, valid_to, transaction_from, transaction_to) VALUES (@emp_no, @salary, @valid_from, @valid_to, @txn_moment, @infinity)",
		args...)
	if err != nil {
		t.Fatal(err)
	}

	// View current snapshot
	rows, err = tx.Query("SELECT * "+
		"FROM salaries WHERE "+
		"emp_no = @emp_no "+
		"AND ("+
		"    (@valid_from BETWEEN valid_from AND valid_to)"+
		" OR (valid_from >= @valid_from AND valid_to <= @valid_to)"+
		" OR (@valid_to BETWEEN valid_from AND valid_to)"+
		") "+
		"AND @txn_moment >= transaction_from AND @txn_moment < transaction_to "+
		"ORDER BY valid_from",
		args...,
	)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()
	columns, res, err = rowsToMaps(rows)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("After Update")
	printMap(columns, res)
}

func TestUpdatePreservingHistory(t *testing.T) {
	db, cleanup := createTestDB(t)
	defer cleanup()

	args := []any{
		sql.Named("emp_no", 10009),
		sql.Named("salary", 42),
		sql.Named("valid_from", "1995-01-01"),
		sql.Named("valid_to", "2000-01-01"),
		sql.Named("txn_moment", time.Now()),
		sql.Named("infinity", bitemporal.EndOfTime),
	}

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}

	// View current snapshot
	rows, err := tx.Query("SELECT * "+
		"FROM salaries WHERE "+
		"emp_no = @emp_no "+
		"AND ("+
		"    (@valid_from BETWEEN valid_from AND valid_to)"+
		" OR (valid_from >= @valid_from AND valid_to <= @valid_to)"+
		" OR (@valid_to BETWEEN valid_from AND valid_to)"+
		") "+
		"AND @txn_moment >= transaction_from AND @txn_moment < transaction_to "+
		"ORDER BY valid_from",
		args...,
	)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()
	columns, res, err := rowsToMaps(rows)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Before Update")
	printMap(columns, res)

	// Close the transaction periods of the rows to be affected
	_, err = tx.Exec("UPDATE salaries "+
		"SET transaction_to = @txn_moment "+
		"WHERE emp_no = @emp_no "+
		"AND ("+
		"    (@valid_from BETWEEN valid_from AND valid_to)"+
		" OR (valid_from >= @valid_from AND valid_to <=	 @valid_to)"+
		" OR (@valid_to BETWEEN valid_from AND valid_to)"+
		") "+
		"AND @txn_moment >= transaction_from AND @txn_moment < transaction_to",
		args...)
	if err != nil {
		t.Fatal(err)
	}

	// Insert any history that exists outside the requested period
	_, err = tx.Exec("INSERT INTO salaries (emp_no, salary, valid_from, valid_to, transaction_from, transaction_to) "+
		"SELECT emp_no, salary, valid_from, @valid_from valid_to, @txn_moment, @infinity FROM salaries WHERE emp_no = @emp_no AND @valid_from > valid_from AND @valid_from < valid_to "+
		"UNION ALL "+
		"SELECT emp_no, salary, CASE WHEN valid_from <= @valid_from THEN valid_from ELSE @valid_from END valid_from, CASE WHEN valid_to <= @REQ_CLOSE THEN valid_to ELSE @valid_to END valid_to, @txn_moment transaction_from, @infinity transaction_to FROM salaries WHERE emp_no = @emp_no AND (valid_from >= @valid_from AND valid_to <= @valid_to) AND @txn_moment >= transaction_from AND @txn_moment = transaction_to "+
		"UNION ALL "+
		"SELECT emp_no, salary, @valid_to valid_from, valid_to, @txn_moment, @infinity FROM salaries WHERE emp_no = @emp_no AND @valid_to > valid_from AND @valid_to < valid_to",
		args...)
	if err != nil {
		t.Fatal(err)
	}

	// Update new history
	_, err = tx.Exec("UPDATE salaries SET salary = 42 WHERE salary BETWEEN 82507 AND 85875 AND emp_no = @emp_no AND @txn_moment >= transaction_from AND @txn_moment < transaction_to",
		args...)
	if err != nil {
		t.Fatal(err)
	}

	// View current snapshot
	rows, err = tx.Query("SELECT * "+
		"FROM salaries WHERE "+
		"emp_no = @emp_no "+
		"AND ("+
		"    (@valid_from BETWEEN valid_from AND valid_to)"+
		" OR (valid_from >= @valid_from AND valid_to <= @valid_to)"+
		" OR (@valid_to BETWEEN valid_from AND valid_to)"+
		") "+
		"AND @txn_moment >= transaction_from AND @txn_moment < transaction_to "+
		"ORDER BY valid_from",
		args...,
	)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()
	columns, res, err = rowsToMaps(rows)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("After Update")
	printMap(columns, res)
}
