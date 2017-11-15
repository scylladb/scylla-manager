// Copyright (C) 2017 ScyllaDB

package main

import (
	"github.com/apcera/termtables"
)

func newTable(header ...interface{}) *termtables.Table {
	t := termtables.CreateTable()
	t.UTF8Box()
	t.AddHeaders(header...)
	return t
}
