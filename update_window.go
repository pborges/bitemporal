package bitemporal

import (
	"bytes"
	"database/sql"
	_ "embed"
	"fmt"
	"strings"
	"text/template"
	"time"
)

//go:embed sql/update_window.tmpl.sql
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
	Table    string
	Select   []string
	FilterBy []string
	Values   map[string]any

	ValidFrom time.Time
	ValidTo   time.Time
}

func (w UpdateWindow) ColumnsString() string {
	return strings.Join(w.Select, ", ")
}

func (w UpdateWindow) ColumnParamsString() string {
	params := make([]string, len(w.Select))
	for i := range w.Select {
		params[i] = "@" + w.Select[i] + " as " + w.Select[i]
	}
	return strings.Join(params, ", ")
}

func (w UpdateWindow) FiltersString() string {
	filters := make([]string, len(w.FilterBy))
	for i := range w.FilterBy {
		filters[i] = w.FilterBy[i] + " = @" + w.FilterBy[i]
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

	for i := range window.Select {
		val, ok := window.Values[window.Select[i]]
		if !ok {
			return QueryFragment{}, fmt.Errorf("value not found for column %q", window.Select[i])
		}
		args = append(args, sql.Named(window.Select[i], val))
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
