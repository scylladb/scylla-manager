// Copyright (C) 2025 ScyllaDB

package tablet

import (
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type progressWorker struct {
	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID
	smSession gocqlx.Session
}

func (s *Service) newProgressWorker(clusterID, taskID, runID uuid.UUID) *progressWorker {
	return &progressWorker{
		clusterID: clusterID,
		taskID:    taskID,
		runID:     runID,
		smSession: s.smSession,
	}
}

func (w *progressWorker) getProgress() (Progress, error) {
	m := qb.M{
		"cluster_id": w.clusterID,
		"task_id":    w.taskID,
		"run_id":     w.runID,
	}
	q := table.TabletRepairRunProgress.SelectQuery(w.smSession).BindMap(m)
	defer q.Release()

	out := Progress{}
	pr := &RunProgress{}
	iter := q.Iter()
	for iter.StructScan(pr) {
		out.Tables = append(out.Tables, TableProgress{
			Keyspace:    pr.Keyspace,
			Table:       pr.Table,
			StartedAt:   pr.StartedAt,
			CompletedAt: pr.CompletedAt,
			Error:       pr.Error,
		})
	}
	return out, iter.Close()
}
