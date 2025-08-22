package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/olekukonko/tablewriter"
	"github.com/pborges/bitemporal/internal/db"
)

func main() {
	database, err := sql.Open("sqlite3", "bitemporal.db")
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	if err := database.Ping(); err != nil {
		log.Fatal(err)
	}

	queries := db.New(database)

	log.Println("Successfully initialized SQLite database with sqlc")

	ctx := context.Background()
	var empNo int64 = 10009

	titles, err := queries.GetEmployeeTitleTimeline(ctx, empNo)
	if err != nil {
		log.Fatal(err)
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{"Change Type", "Start Date", "End Date", "Description", "Transaction Time"})

	for _, title := range titles {
		description := ""
		if title.Description != nil {
			description = fmt.Sprintf("%v", title.Description)
		}

		transactionTime := ""
		if title.TransactionTime.Valid {
			transactionTime = title.TransactionTime.Time.Format("2006-01-02 15:04:05")
		}

		table.Append([]string{
			title.ChangeType,
			title.ChangeDate.Format("2006-01-02"),
			title.EndDate.Format("2006-01-02"),
			description,
			transactionTime,
		})
	}

	table.Render()
}
