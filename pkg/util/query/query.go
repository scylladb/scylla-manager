// Copyright (C) 2023 ScyllaDB

package query

import (
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
)

// GetAllViews returns set of all views present in cluster.
func GetAllViews(s gocqlx.Session) (*strset.Set, error) {
	q := qb.Select("system_schema.views").
		Columns("keyspace_name", "view_name").
		Query(s)
	defer q.Release()
	iter := q.Iter()

	views := strset.New()
	var keyspace, view string
	for iter.Scan(&keyspace, &view) {
		views.Add(keyspace + "." + view)
	}

	return views, iter.Close()
}
