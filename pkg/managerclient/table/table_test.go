// Copyright (C) 2017 ScyllaDB

package table_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/managerclient/table"
	"github.com/scylladb/termtables"
)

func TestTable(t *testing.T) {
	headers := []interface{}{"system_auth", "segment_success", "cause"}
	rows := [][]interface{}{
		{"192.168.100.11", 844, "Cause of the error is something"},
		{"192.168.100.11", 843, "Cause of the error is something very long to fit into the table"},
	}
	tbl := []struct {
		name     string
		maxWidth int
		header   []interface{}
		rows     [][]interface{}
		columns  []int
		expected string
	}{
		{
			"no limited column",
			110,
			headers,
			rows,
			nil,
			`╭────────────────┬─────────────────┬─────────────────────────────────────────────────────────────────╮
│ system_auth    │ segment_success │ cause                                                           │
├────────────────┼─────────────────┼─────────────────────────────────────────────────────────────────┤
│ 192.168.100.11 │             844 │ Cause of the error is something                                 │
│ 192.168.100.11 │             843 │ Cause of the error is something very long to fit into the table │
╰────────────────┴─────────────────┴─────────────────────────────────────────────────────────────────╯
`,
		},
		{
			"limit column",
			70,
			headers,
			rows,
			[]int{2},
			`╭────────────────┬─────────────────┬─────────────────────────────────╮
│ system_auth    │ segment_success │ cause                           │
├────────────────┼─────────────────┼─────────────────────────────────┤
│ 192.168.100.11 │             844 │ Cause of the error is something │
│ 192.168.100.11 │             843 │ Cause of the error is somethin… │
╰────────────────┴─────────────────┴─────────────────────────────────╯
`,
		},
		{
			"limit column, enough room for content",
			110,
			headers,
			rows,
			[]int{2},
			`╭────────────────┬─────────────────┬─────────────────────────────────────────────────────────────────╮
│ system_auth    │ segment_success │ cause                                                           │
├────────────────┼─────────────────┼─────────────────────────────────────────────────────────────────┤
│ 192.168.100.11 │             844 │ Cause of the error is something                                 │
│ 192.168.100.11 │             843 │ Cause of the error is something very long to fit into the table │
╰────────────────┴─────────────────┴─────────────────────────────────────────────────────────────────╯
`,
		},
		{
			"limit column to less characters then header width",
			40,
			headers,
			rows,
			[]int{2},
			`╭────────────────┬─────────────────┬───────╮
│ system_auth    │ segment_success │ cause │
├────────────────┼─────────────────┼───────┤
│ 192.168.100.11 │             844 │ …     │
│ 192.168.100.11 │             843 │ …     │
╰────────────────┴─────────────────┴───────╯
`,
		},
		{
			"limit multiple columns",
			40,
			headers,
			rows,
			[]int{0, 1, 2},
			`╭─────────────┬─────────────────┬────────────╮
│ system_auth │ segment_success │ cause      │
├─────────────┼─────────────────┼────────────┤
│ 192.168.1…  │             844 │ Cause of … │
│ 192.168.1…  │             843 │ Cause of … │
╰─────────────┴─────────────────┴────────────╯
`,
		},
	}

	for _, test := range tbl {
		t.Run(test.name, func(t *testing.T) {
			termtables.MaxColumns = test.maxWidth
			tb := table.New(test.header...)
			tb.LimitColumnLength(test.columns...)
			for i := range test.rows {
				tb.AddRow(test.rows[i]...)
			}
			tb.SetColumnAlignment(termtables.AlignRight, 1)
			if diff := cmp.Diff(tb.String(), test.expected); diff != "" {
				t.Log(tb.String())
				t.Fatal(diff)
			}
		})
	}
}
