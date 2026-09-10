// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	schematable "github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// GetProgress returns all tablet aware restore progress rows of the given run.
// There is nothing to aggregate - a single row describes the progress of a single table.
func GetProgress(session gocqlx.Session, clusterID, taskID, runID uuid.UUID) ([]RunProgress, error) {
	q := schematable.RestoreRunProgressTablet.SelectQuery(session).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
		"run_id":     runID,
	})
	defer q.Release()

	var out []RunProgress
	var pr RunProgress
	iter := q.Iter()
	for iter.StructScan(&pr) {
		out = append(out, pr)
	}
	return out, iter.Close()
}

// getProgressMap converts GetProgress slice output into a map.
func getProgressMap(session gocqlx.Session, clusterID, taskID, runID uuid.UUID) (map[table.CQLTable]RunProgress, error) {
	prs, err := GetProgress(session, clusterID, taskID, runID)
	if err != nil {
		return nil, err
	}
	out := make(map[table.CQLTable]RunProgress, len(prs))
	for i := range prs {
		out[table.CQLTable{Keyspace: prs[i].Keyspace, Name: prs[i].Table}] = prs[i]
	}
	return out, nil
}

// CloneProgress copies resumable progress rows of the previous run under the new run ID.
// Cloned rows describe fully restored tables and tables with ongoing tablet aware restore
// task that SM can try to re-attach to. Rows with recorded error are not cloned, as their
// tables need to be restored from scratch.
func CloneProgress(session gocqlx.Session, clusterID, taskID, prevRunID, runID uuid.UUID) error {
	prev, err := GetProgress(session, clusterID, taskID, prevRunID)
	if err != nil {
		return errors.Wrap(err, "get previous run progress")
	}

	q := schematable.RestoreRunProgressTablet.InsertQuery(session)
	defer q.Release()
	for i := range prev {
		pr := &prev[i]
		if !pr.isSuccess() && !pr.canReattach() {
			continue
		}
		pr.RunID = runID
		if err := q.BindStruct(pr).Exec(); err != nil {
			return errors.Wrapf(err, "clone progress of table %s.%s", pr.Keyspace, pr.Table)
		}
	}
	return nil
}
