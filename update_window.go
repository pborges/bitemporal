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
	Query  string
	ArgMap map[string]any
}

func (q QueryFragment) Args() []any {
	args := make([]any, 0, len(q.ArgMap))
	for k, v := range q.ArgMap {
		args = append(args, sql.Named(k, v))
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

	fragment := QueryFragment{
		ArgMap: map[string]any{
			"valid_from": window.ValidFrom,
			"valid_to":   window.ValidTo,
		},
	}

	for i := range window.Select {
		val, ok := window.Values[window.Select[i]]
		if !ok {
			return QueryFragment{}, fmt.Errorf("value not found for column %q", window.Select[i])
		}
		fragment.ArgMap[window.Select[i]] = val
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, window)
	if err != nil {
		return QueryFragment{}, err
	}

	fragment.Query = buf.String()

	return fragment, nil
}
