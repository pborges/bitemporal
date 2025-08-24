package bitemporal_test

import (
	"context"
	"database/sql"
	_ "embed"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pborges/bitemporal"
	"github.com/pborges/bitemporal/model"
)

const debugTemporal = true

//go:embed sql/schema.sql
var temporalSchema string

//go:embed sql/test_valid_time_data.sql
var validTimeData string

func createTemporalTestDB(t *testing.T) (*bitemporal.TemporalDB, *model.EmployeeRepository, func()) {
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

	temporalDB, err := bitemporal.NewTemporalDB(db)
	if err != nil {
		t.Fatalf("Failed to create TemporalDB: %v", err)
	}

	employeeRepo := model.NewEmployeeRepository(temporalDB)

	return temporalDB, employeeRepo, func() {
		db.Close()
	}
}

func queryEmployeeAtTime(t *testing.T, repo *model.EmployeeRepository, empNo int64, validTime, transactionTime time.Time) model.Employee {
	// Create context with temporal moments
	ctx := context.Background()
	ctx = bitemporal.WithValidTime(ctx, validTime)
	ctx = bitemporal.WithSystemMoment(ctx, transactionTime)

	// Query using the repository
	employee, err := repo.ById(ctx, empNo)
	if err != nil {
		t.Fatalf("Failed to query employee: %v", err)
	}

	if debugTemporal {
		t.Logf("Query: emp_no=%d, valid_time=%s, transaction_time=%s", empNo, validTime, transactionTime)
		t.Logf("Result: %s %s (valid: %s to %s, tx: %s to %s)",
			employee.FirstName, employee.LastName,
			employee.ValidFrom.Format("2006-01-02 15:04:05"), employee.ValidTo.Format("2006-01-02 15:04:05"),
			employee.TransactionFrom.Format("2006-01-02 15:04:05"), employee.TransactionTo.Format("2006-01-02 15:04:05"))
	}

	return employee
}

func TestNameChangeAtSpecificTime(t *testing.T) {
	_, repo, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: What was Jane's name on 2023-06-16 according to what we knew on 2023-07-05?
	// On 2023-07-05, HR had recorded marriage starting 2023-06-15
	// So on 2023-06-16 (after marriage date), she was Johnson
	employee := queryEmployeeAtTime(t, repo, 12345, bitemporal.AsTime("2023-06-16"), bitemporal.AsTime("2023-07-05 23:59:59"))

	if employee.LastName != "Johnson" {
		t.Errorf("Expected last name 'Johnson' (after recorded marriage date), got '%s'", employee.LastName)
	}
	if employee.FirstName != "Jane" {
		t.Errorf("Expected first name 'Jane', got '%s'", employee.FirstName)
	}
}

func TestNameChangeBeforeCorrectionKnowledge(t *testing.T) {
	_, repo, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: What was Jane's name on 2023-06-12 according to what we knew on 2023-06-20?
	// At this time, HR had recorded marriage starting 2023-06-15, correction not made yet
	// So on 2023-06-12 (before the recorded marriage date), she was still Smith
	employee := queryEmployeeAtTime(t, repo, 12345, bitemporal.AsTime("2023-06-12"), bitemporal.AsTime("2023-06-20 23:59:59"))

	if employee.LastName != "Smith" {
		t.Errorf("Expected last name 'Smith' (before recorded marriage date), got '%s'", employee.LastName)
	}
}

func TestNameChangeAfterCorrection(t *testing.T) {
	_, repo, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: What was Jane's name on 2023-06-12 according to current knowledge?
	// After correction: marriage was actually on 2023-06-10, so on 2023-06-12 she was Johnson
	employee := queryEmployeeAtTime(t, repo, 12345, bitemporal.AsTime("2023-06-12"), time.Now())

	if employee.LastName != "Johnson" {
		t.Errorf("Expected last name 'Johnson' (corrected marriage was 2023-06-10), got '%s'", employee.LastName)
	}
}

func TestNameOnActualMarriageDate(t *testing.T) {
	_, repo, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: What was Jane's name on the actual marriage date (2023-06-10) with current knowledge?
	employee := queryEmployeeAtTime(t, repo, 12345, bitemporal.AsTime("2023-06-10"), time.Now())

	if employee.LastName != "Johnson" {
		t.Errorf("Expected last name 'Johnson' on marriage date, got '%s'", employee.LastName)
	}
}

func TestNameBeforeMarriage(t *testing.T) {
	_, repo, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: What was Jane's name before marriage (2023-06-09) with current knowledge?
	employee := queryEmployeeAtTime(t, repo, 12345, bitemporal.AsTime("2023-06-09"), time.Now())

	if employee.LastName != "Smith" {
		t.Errorf("Expected last name 'Smith' before marriage, got '%s'", employee.LastName)
	}
}

func TestCompleteAuditTrail(t *testing.T) {
	_, repo, cleanup := createTemporalTestDB(t)
	defer cleanup()

	// Test: Show complete audit trail for emp_no 12345
	// Use context without temporal filtering to get all records
	ctx := context.Background()
	auditTrail, err := repo.AllRecords(ctx, 12345)
	if err != nil {
		t.Fatalf("Failed to query complete audit trail: %v", err)
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
				i+1, emp.FirstName, emp.LastName,
				emp.ValidFrom.Format("2006-01-02 15:04:05"), emp.ValidTo.Format("2006-01-02 15:04:05"),
				emp.TransactionFrom.Format("2006-01-02 15:04:05"), emp.TransactionTo.Format("2006-01-02 15:04:05"))
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
	_, repo, cleanup := createTemporalTestDB(t)
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
			description:     "HR corrected the marriage date to 2023-06-10, so 2023-06-12 shows Johnson",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			employee := queryEmployeeAtTime(t, repo, 12345, bitemporal.AsTime(tc.validTime), bitemporal.AsTime(tc.transactionTime))

			if employee.LastName != tc.expectedName {
				t.Errorf("Expected last name '%s' (%s), got '%s'",
					tc.expectedName, tc.description, employee.LastName)
			}
		})
	}
}
