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

func (repo *TemporalDB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	query, args = repo.prepareQuery(ctx, query, args)
	return repo.db.Query(query, args...)
}

func (repo *TemporalDB) prepareQuery(ctx context.Context, query string, args []any) (string, []any) {
	validMoment := GetValidMoment(ctx)
	systemMoment := GetSystemMoment(ctx)

	// relies on the queryPlanner to ignore unused CTEs
	var ctes []string
	for _, table := range repo.temporalTables {
		predicate := ""
		var filters []string
		var filterArgs []any
		if !validMoment.IsZero() {
			filters = append(filters, "valid_from <= ? AND ? < valid_to")
			filterArgs = append(filterArgs, validMoment, validMoment)
		}
		if !systemMoment.IsZero() {
			filters = append(filters, "transaction_from <= ? AND ? < transaction_to")
			filterArgs = append(filterArgs, systemMoment, systemMoment)
		}
		if !validMoment.IsZero() || !systemMoment.IsZero() {
			predicate = " WHERE (" + strings.Join(filters, " AND ") + ")"
			args = append(filterArgs, args...)
		}
		ctes = append(ctes, fmt.Sprintf("\n%s$ as (SELECT * FROM %s%s)", table.Name, table.Name, predicate))
	}
	query = fmt.Sprintf("WITH %s \n%s", strings.Join(ctes, ","), query)
	if dumpQueries {
		fmt.Printf("- QUERY [VALID: %s SYS: %s] ----------------------------\n", validMoment.Format(time.Stamp), systemMoment.Format(time.Stamp))
		fmt.Println(query)
		for _, arg := range args {
			fmt.Println("-", arg)
		}
		fmt.Println("--------------------------------------------------------")
	}
	return query, args
}

func (repo *TemporalDB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	query, args = repo.prepareQuery(ctx, query, args)
	return repo.db.QueryRow(query, args...)
}
