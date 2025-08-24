package main

import (
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

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(os.Stdout)

	repo, err := bitemporal.NewTemporalDB("bitemporal.db")
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
	}, time.Now().Add(24*time.Hour*365*-1), bitemporal.EndOfTime)
	if err != nil {
		log.Fatalln(err)
	}

	err = employeesRepo.Save(model.Employee{
		EmpNo:     100,
		FirstName: "John",
		LastName:  "Smythe",
		Gender:    "M",
		BirthDate: AsTime("1990-01-01"),
		HireDate:  AsTime("2001-01-01"),
	}, time.Now().Add(24*time.Hour*365*-1), bitemporal.EndOfTime)
	if err != nil {
		log.Fatalln(err)
	}
}
