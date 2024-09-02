// Copyright (C) 2023 ScyllaDB

package query

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
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

// RaftReadBarrier performs read barrier on a random node to which session is connected to.
// When used with single host session, it can be used to ensure that given node
// has already applied all raft commands present in the raft log.
func RaftReadBarrier(session gocqlx.Session) error {
	// As this functionality is not available from Scylla API,
	// SM uses the workaround described in https://github.com/scylladb/scylladb/issues/19213.
	// Read barrier is performed before any schema change, so it's enough to try to apply
	// some dummy schema changing CQL statement.
	// In order to avoid misleading errors, it's good to use the IF EXISTS clause.
	// Still, it might happen that SM does not have the permissions to perform dummy schema change.
	// As described in the issue, this type of error still results in performing read barrier,
	// so SM should treat it as if everything went fine.
	// TODO: swap workaround with proper API call when mentioned issue is fixed.
	// nolint: godox
	tmpName := strings.ReplaceAll(uuid.NewTime().String(), "-", "")
	readBarrierStmt := fmt.Sprintf("DROP TABLE IF EXISTS %q.%q", tmpName, tmpName)
	if err := session.ExecStmt(readBarrierStmt); err != nil && !isGocqlUnauthorizedError(err) {
		return errors.Wrap(err, "exec dummy schema change")
	}
	return nil
}

func isGocqlUnauthorizedError(err error) bool {
	var reqErr gocql.RequestError
	return errors.As(err, &reqErr) && reqErr.Code() == gocql.ErrCodeUnauthorized
}
