package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pborges/bitemporal"
	"github.com/pborges/bitemporal/model"
)

func AsTime(s string) time.Time {
	layout := time.DateTime
	if !strings.Contains(s, " ") {
		layout = time.DateOnly
	}

	t, err := time.Parse(layout, strings.TrimSpace(s))
	if err != nil {
		panic(err)
	}
	if t.IsZero() {
		panic(errors.New("time is zero"))
	}
	return t
}

// This file is just a scratch pad for now
func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(os.Stdout)

	database, err := sql.Open("sqlite3", "bitemporal.db")
	if err != nil {
		log.Fatal(err)
	}

	db, err := bitemporal.NewTemporalDB(database)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	employeesRepo := model.NewEmployeeRepository(db)
	salariesRepo := model.NewSalaryRepository(db)

	dump(employeesRepo, salariesRepo, 10009, time.Now())
	dump(employeesRepo, salariesRepo, 10009, AsTime("1993-02-17"))

	fmt.Println("All Salaries")
	ctx := bitemporal.WithValidTime(context.Background(), time.Time{})
	salaries, err := salariesRepo.ForEmployee(ctx, 10009)
	if err != nil {
		log.Fatalln(err)
	}
	for _, salary := range salaries {
		fmt.Printf("  $%d %+v\n", salary.Salary, salary.BitemporalEntity)
	}
}

func dump(employeesRepo *model.EmployeeRepository, salariesRepo *model.SalaryRepository, empNo int64, asOfValid time.Time) {
	fmt.Println("* DUMPING AS OF: ", asOfValid.Format(time.DateTime))
	ctx := bitemporal.InitializeContext(context.Background())
	ctx = bitemporal.WithValidTime(ctx, asOfValid)
	employee, err := employeesRepo.ById(ctx, empNo)
	if err != nil {
		log.Fatalln(err)
	}
	salaries, err := salariesRepo.ForEmployee(ctx, employee.EmpNo)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("Employee:\n  %s %s %s\nSalaries:\n", employee.FirstName, employee.LastName, employee.BitemporalEntity)
	for _, salary := range salaries {
		fmt.Printf("  $%d %+v\n", salary.Salary, salary.BitemporalEntity)
	}
}
