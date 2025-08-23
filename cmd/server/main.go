package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pborges/bitemporal"
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

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(os.Stdout)

	repo, err := bitemporal.NewRepository("bitemporal.db")
	if err != nil {
		log.Fatalln(err)
	}
	defer repo.Close()

	employeesRepo := bitemporal.NewEmployeeRepository(repo)
	salariesRepo := bitemporal.NewSalaryRepository(repo)

	dump(employeesRepo, salariesRepo, 1001, time.Now())
	dump(employeesRepo, salariesRepo, 1001, AsTime("1993-02-17"))

	fmt.Println("All Salaries")
	ctx := bitemporal.WithValidTime(context.Background(), time.Time{})
	salaries, err := salariesRepo.ForEmployee(ctx, 1001)
	if err != nil {
		log.Fatalln(err)
	}
	for _, salary := range salaries {
		fmt.Printf("  $%d %+v\n", salary.Salary, salary.BitemporalEntity)
	}

}

func dump(employeesRepo *bitemporal.EmployeeRepository, salariesRepo *bitemporal.SalaryRepository, empNo int64, t time.Time) {
	fmt.Println("* DUMPING AS OF: ", t.Format(time.DateTime))
	ctx := bitemporal.InitializeContext(context.Background())
	ctx = bitemporal.WithValidTime(ctx, t)
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
