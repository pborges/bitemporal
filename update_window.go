package bitemporal

import (
	"bytes"
	"database/sql"
	_ "embed"
	"strings"
	"text/template"
	"time"
)

//go:embed update_window.tmpl.sql
var createUpdateWindowQuery string

type QueryFragment struct {
	Query     string
	NamedArgs []sql.NamedArg
}

func (q QueryFragment) Args() []any {
	args := make([]any, len(q.NamedArgs))
	for i, namedArg := range q.NamedArgs {
		args[i] = any(namedArg)
	}
	return args
}

type UpdateWindow struct {
	Table   string
	Columns []string
	Filters []string

	ValidFrom time.Time
	ValidTo   time.Time

	Values map[string]any
}

func (w UpdateWindow) ColumnsString() string {
	return strings.Join(w.Columns, ", ")
}

func (w UpdateWindow) ColumnParamsString() string {
	params := make([]string, len(w.Columns))
	for i := range w.Columns {
		params[i] = "@" + w.Columns[i] + " as " + w.Columns[i]
	}
	return strings.Join(params, ", ")
}

func (w UpdateWindow) FiltersString() string {
	filters := make([]string, len(w.Filters))
	for i := range w.Filters {
		filters[i] = w.Filters[i] + " = @" + w.Filters[i]
	}
	return strings.Join(filters, " AND ")
}

func CreatePeriodsQuery(window UpdateWindow) (QueryFragment, error) {
	tmpl, err := template.New("update").Parse(createUpdateWindowQuery)
	if err != nil {
		return QueryFragment{}, err
	}

	args := []sql.NamedArg{
		sql.Named("valid_from", window.ValidFrom),
		sql.Named("valid_to", window.ValidTo),
	}

	for i := range window.Columns {
		args = append(args, sql.Named(window.Columns[i], window.Values[window.Columns[i]]))
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, window)
	if err != nil {
		return QueryFragment{}, err
	}

	return QueryFragment{
		Query:     buf.String(),
		NamedArgs: args,
	}, nil
}
