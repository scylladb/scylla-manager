// Copyright (C) 2017 ScyllaDB

package client

import (
	"bytes"
	"fmt"

	"github.com/mmatczuk/tablewriter"
)

type table struct {
	table *tablewriter.Table
	buf   *bytes.Buffer
}

func newTable(header ...string) table {
	b := new(bytes.Buffer)
	t := tablewriter.NewWriter(b)
	t.SetHeader(header)
	t.SetAutoFormatHeaders(false)
	t.SetBorder(false)

	return table{
		table: t,
		buf:   b,
	}
}

func (t table) append(row ...interface{}) {
	s := make([]string, len(row))
	for i, e := range row {
		s[i] = fmt.Sprint(e)
	}
	t.table.Append(s)
}

func (t table) String() string {
	defer t.buf.Reset()
	t.table.Render()
	return t.buf.String()
}
