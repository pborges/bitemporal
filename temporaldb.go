package bitemporal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const dumpQueries = true

var pragmas = []string{
	"PRAGMA journal_mode = MEMORY",
	"PRAGMA synchronous = OFF",
	"PRAGMA cache_size = 100000",
	"PRAGMA temp_store = MEMORY",
	"PRAGMA locking_mode = EXCLUSIVE",
}

var Schema []Table

type Table struct {
	Name    string
	Columns []string
}

func NewTemporalDB(database *sql.DB) (*TemporalDB, error) {
	if err := database.Ping(); err != nil {
		return nil, err
	}

	for _, pragma := range pragmas {
		if _, err := database.Exec(pragma); err != nil {
			return nil, err
		}
	}

	return &TemporalDB{database, Schema}, nil
}

type TemporalDB struct {
	db             *sql.DB
	temporalTables []Table
}

func (repo *TemporalDB) Close() error {
	return repo.db.Close()
}

func (repo *TemporalDB) Ping() error {
	return repo.db.Ping()
}

func (repo *TemporalDB) Query(ctx context.Context, query string, args map[string]any) (*sql.Rows, error) {
	fragment := repo.prepareQuery(ctx, QueryFragment{query, args})
	return repo.db.Query(fragment.Query, fragment.Args()...)
}

func (repo *TemporalDB) prepareQuery(ctx context.Context, fragment QueryFragment) QueryFragment {
	validMoment := GetValidMoment(ctx)
	systemMoment := GetSystemMoment(ctx)

	// Add temporal parameters once
	if !validMoment.IsZero() {
		fragment.ArgMap["valid_from"] = validMoment
		fragment.ArgMap["valid_to"] = validMoment
	}
	if !systemMoment.IsZero() {
		fragment.ArgMap["transaction_from"] = systemMoment
		fragment.ArgMap["transaction_to"] = systemMoment
	}

	// relies on the queryPlanner to ignore unused CTEs
	var ctes []string
	for _, table := range repo.temporalTables {
		predicate := ""
		var filters []string
		if !validMoment.IsZero() {
			filters = append(filters, "valid_from <= @valid_from AND @valid_to < valid_to")
		}
		if !systemMoment.IsZero() {
			filters = append(filters, "transaction_from <= @transaction_from AND @transaction_to < transaction_to")
		}
		if !validMoment.IsZero() || !systemMoment.IsZero() {
			predicate = " WHERE (" + strings.Join(filters, " AND ") + ")"
		}
		ctes = append(ctes, fmt.Sprintf("\n%s$ as (SELECT * FROM %s%s)", table.Name, table.Name, predicate))
	}

	fragment.Query = fmt.Sprintf("WITH %s \n%s", strings.Join(ctes, ","), fragment.Query)

	if dumpQueries {
		fmt.Printf("- QUERY [VALID: %s SYS: %s] ----------------------------\n", validMoment.Format(time.Stamp), systemMoment.Format(time.Stamp))
		fmt.Println(fragment.Query)
		for k, v := range fragment.ArgMap {
			fmt.Printf("- %s: %v\n", k, v)
		}
		fmt.Println("--------------------------------------------------------")
	}
	return fragment
}

func (repo *TemporalDB) QueryRow(ctx context.Context, query string, args map[string]any) *sql.Row {
	fragment := repo.prepareQuery(ctx, QueryFragment{query, args})
	return repo.db.QueryRow(fragment.Query, fragment.Args()...)
}
