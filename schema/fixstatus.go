// Copyright (C) 2017 ScyllaDB

package schema

import (
	"context"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/gocqlx/table"
	"github.com/scylladb/mermaid/sched/runner"
)

// FixRunStatus updates database after a hard reset where there data in database
// can be stale. It iterates over all partitions of t. For every partition checks
// first row's `status` field. If the status is StatusRunning or StatusStopping
// it's updated to StatusAborted with cause `service killed`.
//
// Note the assumption that run id must be the first item in sort key.
func FixRunStatus(ctx context.Context, session *gocql.Session, t *table.Table) error {
	meta := t.Metadata()

	// validate basic assumptions
	if !strings.HasSuffix(t.Name(), "_run") || len(meta.SortKey) == 0 || meta.SortKey[0] != "id" {
		return errors.New("table not supported")
	}

	stmt, names := qb.Select(t.Name()).Distinct(meta.PartKey...).ToCql()
	q := gocqlx.Query(session.Query(stmt), names)
	defer q.Release()

	// g is a query to get status of the first row in a partition
	stmt, names = t.SelectBuilder().Columns(meta.SortKey...).Columns("status").Limit(1).ToCql()
	g := gocqlx.Query(session.Query(stmt), names)
	defer g.Release()

	// u is a query to update status and cause
	stmt, names = t.Update("status", "cause")
	u := gocqlx.Query(session.Query(stmt), names)
	defer u.Release()

	iter := q.Iter()
	for {
		m := make(map[string]interface{})
		if !iter.MapScan(m) {
			break
		}

		if err := g.BindMap(m).MapScan(m); err != nil {
			return err
		}

		s := runner.Status(m["status"].(string))
		if s == runner.StatusStarting || s == runner.StatusRunning || s == runner.StatusStopping {
			m["status"] = runner.StatusAborted
			m["cause"] = "service killed"

			if err := u.BindMap(m).Exec(); err != nil {
				return errors.Wrap(err, "update failed")
			}
		}
	}

	return iter.Close()
}
