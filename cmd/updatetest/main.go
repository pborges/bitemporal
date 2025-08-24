package main

import (
	"database/sql"
	"errors"
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

	repo, err := bitemporal.NewTemporalDB(database)
	if err != nil {
		log.Fatalln(err)
	}
	defer repo.Close()

	employeesRepo := model.NewEmployeeRepository(repo)

	err = employeesRepo.Save(model.Employee{
		EmpNo:     100,
		FirstName: "John",
		LastName:  "Smith",
		Gender:    "M",
		BirthDate: AsTime("1990-01-01"),
		HireDate:  AsTime("2000-01-01"),
	}, AsTime("2000-01-01"), bitemporal.EndOfTime)
	if err != nil {
		log.Fatalln(err)
	}

	//err = employeesRepo.Save(model.Employee{
	//	EmpNo:     100,
	//	FirstName: "John",
	//	LastName:  "Smythe",
	//	Gender:    "M",
	//	BirthDate: AsTime("1990-01-01"),
	//	HireDate:  AsTime("2000-01-01"),
	//}, AsTime("2010-01-01"), bitemporal.EndOfTime)
	//if err != nil {
	//	log.Fatalln(err)
	//}
	//
	//ctx := bitemporal.WithValidTime(context.Background(), AsTime("2020-01-01"))
	//emp1, err := employeesRepo.ById(ctx, 100)
	//if err != nil {
	//	log.Fatalln(err)
	//}
	//fmt.Printf("%+v\n", emp1)
	//
	//ctx = bitemporal.WithValidTime(context.Background(), AsTime("2005-01-01"))
	//emp2, err := employeesRepo.ById(ctx, 100)
	//if err != nil {
	//	log.Fatalln(err)
	//}
	//fmt.Printf("%+v\n", emp2)
}
