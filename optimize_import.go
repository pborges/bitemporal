// This is a temporary file to show the transaction pattern for the remaining functions
// You would apply this pattern to importDeptEmp, importDeptManager, importTitles, and importSalaries

// Example for importDeptEmp:
func importDeptEmp(db *sql.DB) error {
	file, err := os.Open(filepath.Join(testDbDir, "load_dept_emp.dump"))
	if err != nil {
		return err
	}
	defer file.Close()

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO dept_emp (emp_no, dept_no, valid_from, valid_to, transaction_time)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// ... rest of function logic ...

	// Commit transaction at the end
	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Imported %d dept_emp records", count)
	return scanner.Err()
}