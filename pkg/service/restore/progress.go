// Copyright (C) 2023 ScyllaDB

package restore

import (
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

var (
	zeroTime time.Time
	bigTime  = time.Unix(1<<62-1, 0).UTC()
)

type tableKey struct {
	keyspace string
	table    string
}

// aggregateProgress returns restore progress information classified by keyspace and tables.
func (w *worker) aggregateProgress() (Progress, error) {
	var (
		p = Progress{
			SnapshotTag: w.run.SnapshotTag,
			Stage:       w.run.Stage,
		}
		tableMap     = make(map[tableKey]*TableProgress)
		key          tableKey
		hostProgress = make(map[string]HostProgress)
	)

	// Initialize tables and their size
	for _, u := range w.run.Units {
		key.keyspace = u.Keyspace
		for _, t := range u.Tables {
			key.table = t.Table
			tableMap[key] = &TableProgress{
				Table:       t.Table,
				TombstoneGC: t.TombstoneGC,
				progress: progress{
					Size:        t.Size,
					StartedAt:   &bigTime,
					CompletedAt: &zeroTime,
				},
			}
		}
	}

	// Initialize tables' progress
	atp := aggregateRestoreTableProgress(tableMap)
	err := forEachProgress(w.session, w.run.ClusterID, w.run.TaskID, w.run.ID, func(runProgress *RunProgress) {
		atp(runProgress)
		hp := hostProgress[runProgress.Host]
		hp.Host = runProgress.Host
		hp.ShardCnt = runProgress.ShardCnt
		hp.DownloadedBytes += runProgress.Downloaded
		hp.DownloadDuration += timeSub(runProgress.DownloadStartedAt, runProgress.DownloadCompletedAt).Milliseconds()
		if runProgress.RestoreCompletedAt != nil {
			hp.StreamedBytes += runProgress.Downloaded
			hp.StreamDuration += timeSub(runProgress.RestoreStartedAt, runProgress.RestoreCompletedAt).Milliseconds()
		}
		hostProgress[runProgress.Host] = hp
	})
	if err != nil {
		return p, errors.Wrap(err, "iterate over restore progress")
	}

	// Aggregate progress
	for _, u := range w.run.Units {
		kp := KeyspaceProgress{
			Keyspace: u.Keyspace,
			progress: progress{
				StartedAt:   &bigTime,
				CompletedAt: &zeroTime,
			},
		}

		for _, t := range u.Tables {
			tp := tableMap[tableKey{keyspace: u.Keyspace, table: t.Table}]
			tp.progress.extremeToNil()

			kp.Tables = append(kp.Tables, *tp)
			kp.calcParentProgress(tp.progress)
		}

		kp.extremeToNil()

		p.Keyspaces = append(p.Keyspaces, kp)
		p.calcParentProgress(kp.progress)
	}

	p.extremeToNil()

	p.Views = slices.Clone(w.run.Views)
	for _, hp := range hostProgress {
		p.Hosts = append(p.Hosts, hp)
	}
	return p, nil
}

// aggregateRestoreTableProgress returns function that can be used to aggregate
// restore progress per table.
func aggregateRestoreTableProgress(tableMap map[tableKey]*TableProgress) func(*RunProgress) {
	return func(pr *RunProgress) {
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
			tab.Error = tab.Error + "\n" + pr.Error
		}

		tableMap[key] = tab
	}
}

// extremeToNil converts from temporary extreme time values to nil.
func (rp *progress) extremeToNil() {
	if rp.StartedAt == &bigTime {
		rp.StartedAt = nil
	}
	if rp.CompletedAt == &zeroTime {
		rp.CompletedAt = nil
	}
}

// calcParentProgress returns updated progress for the parent that will include
// child progress.
func (rp *progress) calcParentProgress(child progress) {
	rp.Size += child.Size
	rp.Restored += child.Restored
	rp.Downloaded += child.Downloaded
	rp.Failed += child.Failed

	rp.StartedAt = calcParentStartedAt(rp.StartedAt, child.StartedAt)
	rp.CompletedAt = calcParentCompletedAt(rp.CompletedAt, child.CompletedAt)
}

func calcParentStartedAt(parent, child *time.Time) *time.Time {
	if child != nil {
		// Use child start time as parent start time only if it started before
		// parent.
		if parent == nil || child.Before(*parent) {
			return child
		}
	}
	return parent
}

func calcParentCompletedAt(parent, child *time.Time) *time.Time {
	if child != nil {
		// Use child end time as parent end time only if it ended after parent.
		if parent != nil && child.After(*parent) {
			return child
		}
	} else {
		// Set parent end time to nil if any of its children are ending in nil.
		return nil
	}
	return parent
}

func forEachProgress(s gocqlx.Session, clusterID, taskID, runID uuid.UUID, cb func(*RunProgress)) error {
	q := table.RestoreRunProgress.SelectQuery(s)
	iter := q.BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
		"run_id":     runID,
	}).Iter()

	pr := new(RunProgress)
	for iter.StructScan(pr) {
		cb(pr)
	}

	err := iter.Close()
	q.Release()
	return err
}

// Returns duration between end and start.
// If start is nil, returns 0.
// If end is nil, returns duration between now and start.
func timeSub(start, end *time.Time) time.Duration {
	if start != nil {
		endV := timeutc.Now()
		if end != nil {
			endV = *end
		}
		return endV.Sub(*start)
	}
	return time.Duration(0)
}
