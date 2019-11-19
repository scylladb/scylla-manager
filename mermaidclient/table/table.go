// Copyright (C) 2017 ScyllaDB

package table

import (
	"fmt"
	"os"

	"github.com/scylladb-fork/termtables"
)

func init() {
	// Select drawing character set for termtables to avoid output bugs.
	// See scylladb/mermaid#1381.
	ascii := os.Getenv("SCTOOL_ASCII_TABLES")
	if ascii == "" {
		termtables.EnableUTF8PerLocale()
	}
}

var defaultCellStyle = &termtables.CellStyle{
	Alignment: termtables.AlignLeft,
	ColSpan:   1,
}

type colProps struct {
	limit bool
	width int
}

// Table is a helper type to make it easier to draw tables in the terminal.
type Table struct {
	colProps  map[int]*colProps
	headers   []interface{}
	rows      [][]interface{}
	separator []bool
}

// New creates a new Table.
func New(header ...interface{}) *Table {
	cp := make(map[int]*colProps, len(header))
	for i := range header {
		cw := cellWidth(header[i])
		cp[i] = &colProps{
			width: cw,
		}
	}
	return &Table{
		colProps: cp,
		headers:  header,
	}
}

// AddRow adds another row to the table.
func (t *Table) AddRow(items ...interface{}) *termtables.Row {
	for i := range items {
		if _, ok := t.colProps[i]; !ok {
			t.colProps[i] = &colProps{}
		}
		w := cellWidth(items[i])
		if w > t.colProps[i].width {
			t.colProps[i].width = w
		}
	}
	t.rows = append(t.rows, items)
	t.separator = append(t.separator, false)
	return nil
}

func cellWidth(val interface{}) int {
	c := termtables.CreateCell(val, defaultCellStyle)
	return c.Width()
}

// LimitColumnLength sets column to mask it's content that is overflowing
// length limit by replacing overflow with … character.
// Maximum width is determined by the available space considering total
// terminal width and number of other columns.
func (t *Table) LimitColumnLength(index ...int) {
	if t.colProps == nil {
		t.colProps = make(map[int]*colProps)
	}
	for _, i := range index {
		_, ok := t.colProps[i]
		if !ok {
			t.colProps[i] = &colProps{
				limit: true,
			}
		} else {
			t.colProps[i].limit = true
		}
	}
}

// AddSeparator adds a line to the table content, where the line
// consists of separator characters.
func (t *Table) AddSeparator() {
	t.separator[len(t.separator)-1] = true
}

// Render returns a string representation of a fully rendered table.
func (t *Table) Render() string {
	tbl := termtables.CreateTable()
	if len(t.headers) > 0 {
		tbl.AddHeaders(t.headers...)
	}
	wl := t.widthLimit()
	for i := range t.rows {
		row := make([]interface{}, 0, len(t.rows[i]))
		for j, item := range t.rows[i] {
			if v := t.colProps[j]; v.limit {
				item = limitStr(item, wl)
			}
			row = append(row, item)
		}
		tbl.AddRow(row...)

		if t.separator[i] {
			tbl.AddSeparator()
		}
	}
	return tbl.Render()
}

func (t *Table) widthLimit() int {
	// Init with border and spacing
	fixedTotal := len(t.colProps)*3 + 1
	limited := 0
	for _, prop := range t.colProps {
		if prop.limit {
			limited++
		} else {
			fixedTotal += prop.width
		}
	}
	widthLimit := 0
	if limited > 0 {
		available := termtables.MaxColumns - fixedTotal
		widthLimit = available / limited
	}
	return widthLimit
}

// String representation of a fully drawn table.
func (t *Table) String() string {
	if len(t.rows) == 0 {
		return ""
	}
	return t.Render()
}

func limitStr(v interface{}, limit int) string {
	var val string
	switch v := v.(type) {
	case fmt.Stringer:
		val = v.String()
	case string:
		val = v
	default:
		val = fmt.Sprintf("%v", v)
	}
	if limit >= len(val) {
		return val
	}
	if limit > 0 {
		val = val[:limit-1] + "…"
	}
	return val
}
