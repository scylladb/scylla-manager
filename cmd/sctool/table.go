// Copyright (C) 2017 ScyllaDB

package main

import (
	"github.com/apcera/termtables"
)

type table struct {
	*termtables.Table
	hasRows bool
}

func (t *table) AddRow(items ...interface{}) *termtables.Row {
	t.hasRows = true
	return t.Table.AddRow(items...)
}

func (t *table) String() string {
	if !t.hasRows {
		return ""
	}
	return t.Render()
}

func newTable(header ...interface{}) *table {
	t := termtables.CreateTable()
	t.UTF8Box()
	t.AddHeaders(header...)
	return &table{Table: t}
}
