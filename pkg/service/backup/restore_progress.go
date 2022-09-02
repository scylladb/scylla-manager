// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"

	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
)

// ForEachProgress iterates over all RestoreRunProgress that belong to the run.
// NOTE: callback is always called with the same pointer - only the value that it points to changes.
func (w *restoreWorker) ForEachProgress(ctx context.Context, run *RestoreRun, cb func(*RestoreRunProgress)) {
	iter := table.RestoreRunProgress.SelectQuery(w.managerSession).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	}).Iter()
	defer func() {
		if err := iter.Close(); err != nil {
			w.Logger.Error(ctx, "Error while iterating over run progress",
				"cluster_id", run.ClusterID,
				"task_id", run.TaskID,
				"run_id", run.ID,
				"error", err,
			)
		}
	}()

	pr := new(RestoreRunProgress)
	for iter.StructScan(pr) {
		cb(pr)
	}
}

// ForEachTableProgress iterates over all RestoreRunProgress that belong to the run
// with the same manifest, keyspace and table as the run.
// NOTE: callback is always called with the same pointer - only the value that it points to changes.
func (w *restoreWorker) ForEachTableProgress(ctx context.Context, run *RestoreRun, cb func(*RestoreRunProgress)) {
	iter := qb.Select(table.RestoreRunProgress.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
		qb.Eq("run_id"),
		qb.Eq("manifest_path"),
		qb.Eq("keyspace_name"),
		qb.Eq("table_name"),
	).Query(w.managerSession).BindMap(qb.M{
		"cluster_id":    run.ClusterID,
		"task_id":       run.TaskID,
		"run_id":        run.ID,
		"manifest_path": run.ManifestPath,
		"keyspace_name": run.Keyspace,
		"table_name":    run.Table,
	}).Iter()
	defer func() {
		if err := iter.Close(); err != nil {
			w.Logger.Error(ctx, "Error while iterating over table's run progress",
				"cluster_id", run.ClusterID,
				"task_id", run.TaskID,
				"run_id", run.ID,
				"manifest_path", run.ManifestPath,
				"keyspace", run.Keyspace,
				"table", run.Table,
				"error", err,
			)
		}
	}()

	pr := new(RestoreRunProgress)
	for iter.StructScan(pr) {
		cb(pr)
	}
}
