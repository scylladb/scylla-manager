// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package main

import (
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/gocqlx/table"
)

// rewriteRows performs a sequential rewrite of all rows in a table.
// Code backported from GocqlX 2.2.0.
func rewriteRows(session *gocql.Session, t *table.Table) error {
	stmt, names := t.Insert()
	insert := gocqlx.Query(session.Query(stmt), names)
	defer insert.Release()

	// Iterate over all rows and reinsert them
	iter := session.Query(qb.Select(t.Name()).ToCql()).Iter()
	m := make(map[string]interface{})
	for iter.MapScan(m) {
		if err := insert.BindMap(m).Exec(); err != nil {
			return err
		}
		m = make(map[string]interface{})
	}
	return iter.Close()
}
