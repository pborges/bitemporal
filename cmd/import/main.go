package main

import (
	"bufio"
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	dbPath     = "./bitemporal.db"
	schemaFile = "sql/schema.sql"
	testDbDir  = "test_db-1.0.7"
)

func main() {
	os.Remove(dbPath)

	startTime := time.Now()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	schemaStart := time.Now()
	if err := initializeSchema(db); err != nil {
		log.Fatal(err)
	}
	log.Printf("Schema initialization completed in %v", time.Since(schemaStart))

	optimizeStart := time.Now()
	if err := optimizeDatabase(db); err != nil {
		log.Fatal(err)
	}
	log.Printf("Database optimization completed in %v", time.Since(optimizeStart))

	if err := importEmployees(db); err != nil {
		log.Fatal(err)
	}

	if err := importDepartments(db); err != nil {
		log.Fatal(err)
	}

	if err := importDeptEmp(db); err != nil {
		log.Fatal(err)
	}

	if err := importDeptManager(db); err != nil {
		log.Fatal(err)
	}

	if err := importTitles(db); err != nil {
		log.Fatal(err)
	}

	if err := importSalaries(db); err != nil {
		log.Fatal(err)
	}

	totalElapsed := time.Since(startTime)
	log.Printf("Import completed successfully in %v", totalElapsed)
}

func initializeSchema(db *sql.DB) error {
	log.Printf("Executing schema file: %s", schemaFile)

	schema, err := os.ReadFile(schemaFile)
	if err != nil {
		return err
	}

	_, err = db.Exec(string(schema))
	if err != nil {
		return err
	}

	return nil
}

func importEmployees(db *sql.DB) error {
	startTime := time.Now()
	file, err := os.Open(filepath.Join(testDbDir, "load_employees.dump"))
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

	// Prepare the insert statement
	stmt, err := tx.Prepare(`
		INSERT INTO employees (emp_no, first_name, last_name, hire_date, birth_date, gender, valid_from, valid_to, transaction_from, transaction_to)
		VALUES (?, ?, ?, ?, ?, ?, ?, '9999-12-31 23:59:59', ?, '9999-12-31 23:59:59')
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Parse the MySQL INSERT statements
	re := regexp.MustCompile(`\((\d+),'([^']+)','([^']+)','([^']+)','([MF])','([^']+)'\)`)

	scanner := bufio.NewScanner(file)
	count := 0
	now := time.Now()

	for scanner.Scan() {
		line := scanner.Text()

		matches := re.FindAllStringSubmatch(line, -1)
		for _, match := range matches {
			if len(match) != 7 {
				continue
			}

			empNo := match[1]
			birthDate := match[2]
			firstName := match[3]
			lastName := match[4]
			gender := match[5]
			hireDate := match[6]

			// For bitemporal, we set valid_from to hire_date and transaction_time to now
			_, err := stmt.Exec(empNo, firstName, lastName, hireDate, birthDate, gender, hireDate, now)
			if err != nil {
				log.Printf("Error inserting employee %s: %v", empNo, err)
				continue
			}
			count++
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Imported %d employees in %v", count, time.Since(startTime))
	return scanner.Err()
}

func importDepartments(db *sql.DB) error {
	startTime := time.Now()
	file, err := os.Open(filepath.Join(testDbDir, "load_departments.dump"))
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
		INSERT INTO departments (dept_no, dept_name, valid_from, valid_to, transaction_from, transaction_to)
		VALUES (?, ?, '1985-01-01', '9999-12-31 23:59:59', ?, '9999-12-31 23:59:59')
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	re := regexp.MustCompile(`\('([^']+)','([^']+)'\)`)
	scanner := bufio.NewScanner(file)
	count := 0
	now := time.Now()

	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindAllStringSubmatch(line, -1)
		for _, match := range matches {
			if len(match) != 3 {
				continue
			}

			deptNo := match[1]
			deptName := match[2]

			_, err := stmt.Exec(deptNo, deptName, now)
			if err != nil {
				log.Printf("Error inserting department %s: %v", deptNo, err)
				continue
			}
			count++
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Imported %d departments in %v", count, time.Since(startTime))
	return scanner.Err()
}

func importDeptEmp(db *sql.DB) error {
	startTime := time.Now()
	file, err := os.Open(filepath.Join(testDbDir, "load_dept_emp.dump"))
	if err != nil {
		return err
	}
	defer file.Close()

	stmt, err := db.Prepare(`
		INSERT INTO dept_emp (emp_no, dept_no, valid_from, valid_to, transaction_from, transaction_to)
		VALUES (?, ?, ?, ?, ?, '9999-12-31 23:59:59')
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	re := regexp.MustCompile(`\((\d+),'([^']+)','([^']+)','([^']+)'\)`)
	scanner := bufio.NewScanner(file)
	count := 0
	now := time.Now()

	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindAllStringSubmatch(line, -1)
		for _, match := range matches {
			if len(match) != 5 {
				continue
			}

			empNo := match[1]
			deptNo := match[2]
			fromDate := match[3]
			toDate := match[4]

			// Convert 9999-01-01 to our end-of-time format
			if toDate == "9999-01-01" {
				toDate = "9999-12-31 23:59:59"
			}

			_, err := stmt.Exec(empNo, deptNo, fromDate, toDate, now)
			if err != nil {
				log.Printf("Error inserting dept_emp %s-%s: %v", empNo, deptNo, err)
				continue
			}
			count++
		}
	}

	log.Printf("Imported %d dept_emp records in %v", count, time.Since(startTime))
	return scanner.Err()
}

func importDeptManager(db *sql.DB) error {
	startTime := time.Now()
	file, err := os.Open(filepath.Join(testDbDir, "load_dept_manager.dump"))
	if err != nil {
		return err
	}
	defer file.Close()

	stmt, err := db.Prepare(`
		INSERT INTO dept_manager (emp_no, dept_no, valid_from, valid_to, transaction_from, transaction_to)
		VALUES (?, ?, ?, ?, ?, '9999-12-31 23:59:59')
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	re := regexp.MustCompile(`\((\d+),'([^']+)','([^']+)','([^']+)'\)`)
	scanner := bufio.NewScanner(file)
	count := 0
	now := time.Now()

	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindAllStringSubmatch(line, -1)
		for _, match := range matches {
			if len(match) != 5 {
				continue
			}

			empNo := match[1]
			deptNo := match[2]
			fromDate := match[3]
			toDate := match[4]

			if toDate == "9999-01-01" {
				toDate = "9999-12-31 23:59:59"
			}

			_, err := stmt.Exec(empNo, deptNo, fromDate, toDate, now)
			if err != nil {
				log.Printf("Error inserting dept_manager %s-%s: %v", empNo, deptNo, err)
				continue
			}
			count++
		}
	}

	log.Printf("Imported %d dept_manager records in %v", count, time.Since(startTime))
	return scanner.Err()
}

func importTitles(db *sql.DB) error {
	startTime := time.Now()
	file, err := os.Open(filepath.Join(testDbDir, "load_titles.dump"))
	if err != nil {
		return err
	}
	defer file.Close()

	stmt, err := db.Prepare(`
		INSERT INTO titles (emp_no, title, valid_from, valid_to, transaction_from, transaction_to)
		VALUES (?, ?, ?, ?, ?, '9999-12-31 23:59:59')
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	re := regexp.MustCompile(`\((\d+),'([^']+)','([^']+)','([^']+)'\)`)
	scanner := bufio.NewScanner(file)
	count := 0
	now := time.Now()

	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindAllStringSubmatch(line, -1)
		for _, match := range matches {
			if len(match) != 5 {
				continue
			}

			empNo := match[1]
			title := match[2]
			fromDate := match[3]
			toDate := match[4]

			if toDate == "9999-01-01" {
				toDate = "9999-12-31 23:59:59"
			}

			_, err := stmt.Exec(empNo, title, fromDate, toDate, now)
			if err != nil {
				log.Printf("Error inserting title %s-%s: %v", empNo, title, err)
				continue
			}
			count++
		}
	}

	log.Printf("Imported %d titles records in %v", count, time.Since(startTime))
	return scanner.Err()
}

func importSalaries(db *sql.DB) error {
	startTime := time.Now()
	files := []string{
		filepath.Join(testDbDir, "load_salaries1.dump"),
		filepath.Join(testDbDir, "load_salaries2.dump"),
		filepath.Join(testDbDir, "load_salaries3.dump"),
	}

	stmt, err := db.Prepare(`
		INSERT INTO salaries (emp_no, salary, valid_from, valid_to, transaction_from, transaction_to)
		VALUES (?, ?, ?, ?, ?, '9999-12-31 23:59:59')
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	re := regexp.MustCompile(`\((\d+),(\d+),'([^']+)','([^']+)'\)`)
	totalCount := 0
	now := time.Now()

	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(file)
		count := 0

		for scanner.Scan() {
			line := scanner.Text()
			matches := re.FindAllStringSubmatch(line, -1)
			for _, match := range matches {
				if len(match) != 5 {
					continue
				}

				empNo := match[1]
				salary := match[2]
				fromDate := match[3]
				toDate := match[4]

				if toDate == "9999-01-01" {
					toDate = "9999-12-31 23:59:59"
				}

				_, err := stmt.Exec(empNo, salary, fromDate, toDate, now)
				if err != nil {
					log.Printf("Error inserting salary %s: %v", empNo, err)
					continue
				}
				count++
			}
		}

		file.Close()
		log.Printf("Imported %d salary records from %s", count, filename)
		totalCount += count
	}

	log.Printf("Imported %d total salary records in %v", totalCount, time.Since(startTime))
	return nil
}

func optimizeDatabase(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode = MEMORY",
		"PRAGMA synchronous = OFF",
		"PRAGMA cache_size = 100000",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA locking_mode = EXCLUSIVE",
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return err
		}
	}

	return nil
}
