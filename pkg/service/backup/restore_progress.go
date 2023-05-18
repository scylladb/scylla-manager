// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"
	"strings"
)

// aggregateProgress returns restore progress information classified by keyspace and tables.
func (w *restoreWorkerTools) aggregateProgress(ctx context.Context, run *RestoreRun) RestoreProgress {
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
	w.cacheProvider.ForEachRestoreProgress(ctx, run, aggregateRestoreTableProgress(tableMap))

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

		totalDownloaded := pr.Downloaded + pr.Skipped + pr.VersionedProgress
		if validateTimeIsSet(pr.RestoreCompletedAt) {
			tab.Restored += totalDownloaded
		}
		tab.Downloaded += totalDownloaded
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
	rp.Failed += child.Failed

	rp.StartedAt = calcParentStartedAt(rp.StartedAt, child.StartedAt)
	rp.CompletedAt = calcParentCompletedAt(rp.CompletedAt, child.CompletedAt)
}
