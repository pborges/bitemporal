package bitemporal

import (
	"database/sql"
	_ "embed"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const debugTemporal = true

//go:embed sql/schema.sql
var temporalSchema string

//go:embed sql/test_valid_time_data.sql
var validTimeData string

type EmployeeRow struct {
	EmpNo           int64
	BirthDate       string
	FirstName       string
	LastName        string
	Gender          string
	HireDate        string
	ValidFrom       string
	ValidTo         string
	TransactionFrom string
	TransactionTo   string
}

func createTemporalTestDB(t *testing.T) (*sql.DB, func()) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}

	_, err = db.Exec(temporalSchema)
	if err != nil {
		t.Fatalf("Failed to execute schema: %v", err)
	}

	_, err = db.Exec(validTimeData)
	if err != nil {
		t.Fatalf("Failed to execute test data: %v", err)
	}

	return db, func() {
		db.Close()
	}
}

func queryEmployeeAtTime(t *testing.T, db *sql.DB, empNo int64, validTime, transactionTime string) []EmployeeRow {
	query := `
		SELECT emp_no, birth_date, first_name, last_name, gender, hire_date,
		       valid_from, valid_to, transaction_from, transaction_to
		FROM employees 
		WHERE emp_no = ?
		  AND valid_from <= ?
		  AND valid_to > ?
		  AND transaction_from <= ?
		  AND transaction_to > ?
		ORDER BY transaction_from, valid_from`

	rows, err := db.Query(query, empNo, validTime, validTime, transactionTime, transactionTime)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	var employees []EmployeeRow
	for rows.Next() {
		var emp EmployeeRow
		err = rows.Scan(&emp.EmpNo, &emp.BirthDate, &emp.FirstName, &emp.LastName, &emp.Gender, &emp.HireDate,
			&emp.ValidFrom, &emp.ValidTo, &emp.TransactionFrom, &emp.TransactionTo)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		employees = append(employees, emp)
	}

	if debugTemporal && len(employees) > 0 {
		t.Logf("Query: emp_no=%d, valid_time=%s, transaction_time=%s", empNo, validTime, transactionTime)
		for _, emp := range employees {
			t.Logf("Result: %s %s (valid: %s to %s, tx: %s to %s)",
				emp.FirstName, emp.LastName, emp.ValidFrom, emp.ValidTo, emp.TransactionFrom, emp.TransactionTo)
		}
	}

	return employees
}

func TestNameChangeAtSpecificTime(t *testing.T) {
	db, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: What was Jane's name on 2023-06-16 according to what we knew on 2023-07-05?
	// On 2023-07-05, HR had recorded marriage starting 2023-06-15
	// So on 2023-06-16 (after marriage date), she was Johnson
	employees := queryEmployeeAtTime(t, db, 12345, "2023-06-16", "2023-07-05 23:59:59")

	if len(employees) != 1 {
		t.Errorf("Expected 1 employee record, got %d", len(employees))
		return
	}

	emp := employees[0]
	if emp.LastName != "Johnson" {
		t.Errorf("Expected last name 'Johnson' (after recorded marriage date), got '%s'", emp.LastName)
	}
	if emp.FirstName != "Jane" {
		t.Errorf("Expected first name 'Jane', got '%s'", emp.FirstName)
	}
}

func TestNameChangeBeforeCorrectionKnowledge(t *testing.T) {
	db, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: What was Jane's name on 2023-06-12 according to what we knew on 2023-06-20?
	// At this time, HR had recorded marriage starting 2023-06-15, correction not made yet
	// So on 2023-06-12 (before the recorded marriage date), she was still Smith
	employees := queryEmployeeAtTime(t, db, 12345, "2023-06-12", "2023-06-20 23:59:59")

	if len(employees) != 1 {
		t.Errorf("Expected 1 employee record, got %d", len(employees))
		return
	}

	emp := employees[0]
	if emp.LastName != "Smith" {
		t.Errorf("Expected last name 'Smith' (before recorded marriage date), got '%s'", emp.LastName)
	}
}

func TestNameChangeAfterCorrection(t *testing.T) {
	db, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: What was Jane's name on 2023-06-12 according to current knowledge?
	// After correction: marriage was actually on 2023-06-10, so on 2023-06-12 she was Johnson
	now := time.Now().Format("2006-01-02 15:04:05")
	employees := queryEmployeeAtTime(t, db, 12345, "2023-06-12", now)

	if len(employees) != 1 {
		t.Errorf("Expected 1 employee record, got %d", len(employees))
		return
	}

	emp := employees[0]
	if emp.LastName != "Johnson" {
		t.Errorf("Expected last name 'Johnson' (corrected marriage was 2023-06-10), got '%s'", emp.LastName)
	}
}

func TestNameOnActualMarriageDate(t *testing.T) {
	db, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: What was Jane's name on the actual marriage date (2023-06-10) with current knowledge?
	now := time.Now().Format("2006-01-02 15:04:05")
	employees := queryEmployeeAtTime(t, db, 12345, "2023-06-10", now)

	if len(employees) != 1 {
		t.Errorf("Expected 1 employee record, got %d", len(employees))
		return
	}

	emp := employees[0]
	if emp.LastName != "Johnson" {
		t.Errorf("Expected last name 'Johnson' on marriage date, got '%s'", emp.LastName)
	}
}

func TestNameBeforeMarriage(t *testing.T) {
	db, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: What was Jane's name before marriage (2023-06-09) with current knowledge?
	now := time.Now().Format("2006-01-02 15:04:05")
	employees := queryEmployeeAtTime(t, db, 12345, "2023-06-09", now)

	if len(employees) != 1 {
		t.Errorf("Expected 1 employee record, got %d", len(employees))
		return
	}

	emp := employees[0]
	if emp.LastName != "Smith" {
		t.Errorf("Expected last name 'Smith' before marriage, got '%s'", emp.LastName)
	}
}

func TestCompleteAuditTrail(t *testing.T) {
	db, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: Show complete audit trail for emp_no 12345
	query := `
		SELECT emp_no, birth_date, first_name, last_name, gender, hire_date,
		       valid_from, valid_to, transaction_from, transaction_to 
		FROM employees 
		WHERE emp_no = 12345 
		ORDER BY transaction_from, valid_from`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Failed to execute audit trail query: %v", err)
	}
	defer rows.Close()

	var auditTrail []EmployeeRow
	for rows.Next() {
		var emp EmployeeRow
		err = rows.Scan(&emp.EmpNo, &emp.BirthDate, &emp.FirstName, &emp.LastName, &emp.Gender, &emp.HireDate,
			&emp.ValidFrom, &emp.ValidTo, &emp.TransactionFrom, &emp.TransactionTo)
		if err != nil {
			t.Fatalf("Failed to scan audit row: %v", err)
		}
		auditTrail = append(auditTrail, emp)
	}

	// Should have 4 records total (initial + update + 2 corrections)
	expectedRecords := 4
	if len(auditTrail) != expectedRecords {
		t.Errorf("Expected %d audit trail records, got %d", expectedRecords, len(auditTrail))
	}

	if debugTemporal {
		t.Log("Complete Audit Trail for Jane Smith/Johnson:")
		for i, emp := range auditTrail {
			t.Logf("Record %d: %s %s | Valid: %s to %s | Transaction: %s to %s",
				i+1, emp.FirstName, emp.LastName, emp.ValidFrom, emp.ValidTo, emp.TransactionFrom, emp.TransactionTo)
		}
	}

	// Verify the audit trail sequence
	if len(auditTrail) >= 4 {
		// First record: Original Smith record
		if auditTrail[0].LastName != "Smith" {
			t.Errorf("First audit record should be original Smith record, got %s", auditTrail[0].LastName)
		}

		// Second record: Johnson record (first time HR recorded marriage)
		if auditTrail[1].LastName != "Johnson" {
			t.Errorf("Second audit record should be Johnson record, got %s", auditTrail[1].LastName)
		}

		// Third record: Corrected Smith record (closes early due to correction)
		if auditTrail[2].LastName != "Smith" {
			t.Errorf("Third audit record should be corrected Smith record, got %s", auditTrail[2].LastName)
		}

		// Fourth record: Corrected Johnson record (starts from actual marriage date)
		if auditTrail[3].LastName != "Johnson" {
			t.Errorf("Fourth audit record should be corrected Johnson record, got %s", auditTrail[3].LastName)
		}
	}
}

func TestTransactionTimeProgression(t *testing.T) {
	db, cleanup := createTemporalTestDB(t)
	defer cleanup()

	testCases := []struct {
		name            string
		validTime       string
		transactionTime string
		expectedName    string
		description     string
	}{
		{
			name:            "Before HR knew about marriage",
			validTime:       "2023-06-12",
			transactionTime: "2023-06-25 12:00:00",
			expectedName:    "Smith",
			description:     "HR hadn't recorded the marriage yet",
		},
		{
			name:            "After HR recorded marriage",
			validTime:       "2023-06-16",
			transactionTime: "2023-07-05 12:00:00",
			expectedName:    "Johnson",
			description:     "HR recorded marriage starting 2023-06-15, so 2023-06-16 shows Johnson",
		},
		{
			name:            "After correction made",
			validTime:       "2023-06-12",
			transactionTime: "2023-08-20 12:00:00",
			expectedName:    "Johnson",
			description:     "HR corrected the marriage date to 2023-06-10, so June 12 shows Johnson",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			employees := queryEmployeeAtTime(t, db, 12345, tc.validTime, tc.transactionTime)

			if len(employees) != 1 {
				t.Errorf("Expected 1 employee record, got %d", len(employees))
				return
			}

			if employees[0].LastName != tc.expectedName {
				t.Errorf("Expected last name '%s' (%s), got '%s'",
					tc.expectedName, tc.description, employees[0].LastName)
			}
		})
	}
}
