package bitemporal_test

import (
	"fmt"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
)

func highlight(highlight map[any]int, val any) string {
	sVal := fmt.Sprintf("%v", val)
	for k, v := range highlight {
		s2 := fmt.Sprintf("%v", k)
		if sVal == s2 || strings.HasPrefix(sVal, s2) {
			return fmt.Sprintf("\033[%vm%s\033[0m", v, sVal)
		}
	}
	return sVal
}

func PrintSalaryTable(salary int64, validFrom string, validTo string, rows []SalaryRow) {
	zeroValues := salary == 0 && validFrom == "" && validTo == ""
	highlightMap := make(map[any]int)
	if !zeroValues {
		highlightMap = map[any]int{
			validTo:   31,
			validFrom: 32,
			salary:    34,
		}
	}

	table := tablewriter.NewTable(os.Stdout,
		tablewriter.WithConfig(tablewriter.Config{
			Footer: tw.CellConfig{
				Formatting: tw.CellFormatting{MergeMode: tw.MergeHorizontal},
				Alignment:  tw.CellAlignment{Global: tw.AlignLeft},
			},
		}),
	)
	table.Header([]string{"EmpNo", "Salary", "ValidFrom", "ValidTo", "TransactionFrom", "TransactionTo"})
	footer := fmt.Sprintf("Salary: %s\nFrom  : %s\nTo    : %s",
		highlight(highlightMap, salary),
		highlight(highlightMap, validFrom),
		highlight(highlightMap, validTo),
	)

	if !zeroValues {
		table.Footer(footer, footer, footer, footer, footer, footer)
	}

	for _, row := range rows {
		table.Append([]string{
			fmt.Sprintf("%d", row.EmpNo),
			highlight(highlightMap, row.Salary),
			highlight(highlightMap, row.ValidFrom),
			highlight(highlightMap, row.ValidTo),
			row.TransactionFrom,
			row.TransactionTo,
		})
	}

	table.Render()
}
