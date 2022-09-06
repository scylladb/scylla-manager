// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"
	"strings"

	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
)

// aggregateProgress returns restore progress information classified by keyspace and tables.
func (w *restoreWorker) aggregateProgress(ctx context.Context, run *RestoreRun) RestoreProgress {
	var (
		p = RestoreProgress{
			SnapshotTag: run.SnapshotTag,
			Stage:       run.Stage,
		}
		tableMap = make(map[tableKey]*RestoreTableProgress)
		key      tableKey
	)

	// Initialize tables and their size
	for _, u := range run.Units {
		key.keyspace = u.Keyspace
		for _, t := range u.Tables {
			key.table = t.Table
			tableMap[key] = &RestoreTableProgress{
				Table: t.Table,
				restoreProgress: restoreProgress{
					Size:        t.Size,
					StartedAt:   &maxTime,
					CompletedAt: &zeroTime,
				},
			}
		}
	}

	// Initialize tables' progress
	w.ForEachProgress(ctx, run, aggregateRestoreTableProgress(tableMap))

	// Aggregate progress
	for _, u := range run.Units {
		kp := RestoreKeyspaceProgress{
			Keyspace: u.Keyspace,
			restoreProgress: restoreProgress{
				StartedAt:   &maxTime,
				CompletedAt: &zeroTime,
			},
		}

		for _, t := range u.Tables {
			tp := tableMap[tableKey{keyspace: u.Keyspace, table: t.Table}]
			tp.restoreProgress.extremeToNil()

			kp.Tables = append(kp.Tables, *tp)
			kp.calcParentProgress(tp.restoreProgress)
		}

		kp.extremeToNil()

		p.Keyspaces = append(p.Keyspaces, kp)
		p.calcParentProgress(kp.restoreProgress)
	}

	p.extremeToNil()

	return p
}

// aggregateRestoreTableProgress returns function that can be used to aggregate
// restore progress per table.
func aggregateRestoreTableProgress(tableMap map[tableKey]*RestoreTableProgress) func(*RestoreRunProgress) {
	return func(pr *RestoreRunProgress) {
		var (
			key = tableKey{
				keyspace: pr.Keyspace,
				table:    pr.Table,
			}
			tab = tableMap[key]
		)

		if pr.RestoreCompletedAt != nil {
			tab.Restored += pr.Downloaded + pr.Skipped
		}
		tab.Downloaded += pr.Downloaded
		tab.Skipped += pr.Skipped
		tab.Failed += pr.Failed

		tab.StartedAt = calcParentStartedAt(tab.StartedAt, pr.DownloadStartedAt)
		tab.CompletedAt = calcParentCompletedAt(tab.CompletedAt, pr.RestoreCompletedAt)

		if tab.Error == "" {
			tab.Error = pr.Error
		} else if pr.Error != "" {
			tab.Error = strings.Join([]string{tab.Error, pr.Error}, "\n")
		}

		tableMap[key] = tab
	}
}

// extremeToNil converts from temporary extreme time values to nil.
func (rp *restoreProgress) extremeToNil() {
	if rp.StartedAt == &maxTime {
		rp.StartedAt = nil
	}
	if rp.CompletedAt == &zeroTime {
		rp.CompletedAt = nil
	}
}

// calcParentProgress returns updated progress for the parent that will include
// child progress.
func (rp *restoreProgress) calcParentProgress(child restoreProgress) {
	rp.Size += child.Size
	rp.Restored += child.Restored
	rp.Downloaded += child.Downloaded
	rp.Skipped += child.Skipped
	rp.Failed += child.Failed

	rp.StartedAt = calcParentStartedAt(rp.StartedAt, child.StartedAt)
	rp.CompletedAt = calcParentCompletedAt(rp.CompletedAt, child.CompletedAt)
}

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
