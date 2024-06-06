// Copyright (C) 2023 ScyllaDB

package query

import (
	"strings"

	"github.com/pkg/errors"
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

// GetTableVersion returns given table version.
func GetTableVersion(s gocqlx.Session, keyspace, table string) (string, error) {
	q := qb.Select("system_schema.tables").
		Columns("id").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(s).
		Bind(keyspace, table)

	defer q.Release()

	var version string
	if err := q.Scan(&version); err != nil {
		return "", err
	}
	// Table's version is stripped of '-' characters
	version = strings.ReplaceAll(version, "-", "")

	return version, nil
}

// DescribedSchema describes output of DESCRIBE SCHEMA WITH INTERNALS.
type DescribedSchema []DescribedSchemaRow

// DescribedSchemaRow describes a single row returned by DESCRIBE SCHEMA WITH INTERNALS.
type DescribedSchemaRow struct {
	Keyspace string `json:"keyspace"`
	Type     string `json:"type"`
	Name     string `json:"name"`
	CQLStmt  string `json:"cql_stmt"`
}

// DescribeSchemaWithInternals returns the output of DESCRIBE SCHEMA WITH INTERNALS query parsed into DescribedSchema.
func DescribeSchemaWithInternals(session gocqlx.Session) (DescribedSchema, error) {
	it := session.Query("DESCRIBE SCHEMA WITH INTERNALS", nil).Iter()
	var ks, t, name, cql string
	var schema DescribedSchema
	for it.Scan(&ks, &t, &name, &cql) {
		schema = append(schema, DescribedSchemaRow{
			Keyspace: ks,
			Type:     t,
			Name:     name,
			CQLStmt:  cql,
		})
	}

	if err := it.Close(); err != nil {
		return DescribedSchema{}, errors.Wrap(err, "describe schema with internals")
	}
	return schema, nil
}
