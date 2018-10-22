// Copyright (C) 2017 ScyllaDB

package table

import (
	"github.com/apcera/termtables"
)

// Table is a helper type to make it easier to draw tables in the terminal.
type Table struct {
	*termtables.Table
	hasRows bool
}

// AddRow adds another row to the table.
func (t *Table) AddRow(items ...interface{}) *termtables.Row {
	t.hasRows = true
	return t.Table.AddRow(items...)
}

// String representation of a fully drawn table.
func (t *Table) String() string {
	if !t.hasRows {
		return ""
	}
	return t.Render()
}

// New creates a new Table.
func New(header ...interface{}) *Table {
	t := termtables.CreateTable()
	t.UTF8Box()
	t.AddHeaders(header...)
	return &Table{Table: t}
}
