// Copyright (C) 2023 ScyllaDB

package restore

import (
	"iter"
	"slices"
	"time"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func getProgress(run *Run, s gocqlx.Session) (Progress, error) {
	seq := newRunProgressSeq()
	pr := aggregateProgress(run, seq.All(run.ClusterID, run.TaskID, run.ID, s), timeutc.Now())
	if seq.err != nil {
		return Progress{}, seq.err
	}
	return pr, nil
}

// runProgressSeq serves as a body for iter.Seq[*RunProgress].
// Since we can't return error on iteration, it stores it
// inside the err field.
// Users should validate err after using the iterator.
type runProgressSeq struct {
	err error
}

func newRunProgressSeq() *runProgressSeq {
	return &runProgressSeq{}
}

func (seq *runProgressSeq) All(clusterID, taskID, runID uuid.UUID, s gocqlx.Session) iter.Seq[*RunProgress] {
	return func(yield func(rp *RunProgress) bool) {
		q := table.RestoreRunProgress.SelectQuery(s)
		it := q.BindMap(qb.M{
			"cluster_id": clusterID,
			"task_id":    taskID,
			"run_id":     runID,
		}).Iter()
		defer func() {
			seq.err = it.Close()
			q.Release()
		}()

		pr := new(RunProgress)
		for it.StructScan(pr) {
			if !yield(pr) {
				break
			}
		}
	}
}

func aggregateProgress(run *Run, seq iter.Seq[*RunProgress], now time.Time) Progress {
	p := Progress{
		SnapshotTag: run.SnapshotTag,
		Stage:       run.Stage,
	}
	tableProgress := make(map[TableName]TableProgress)
	hostProgress := make(map[string]HostProgress)
	for rp := range seq {
		progressCB(tableProgress, hostProgress, rp, now)
	}

	// Aggregate keyspace progress
	for _, u := range run.Units {
		p.progress.Size += u.Size
		kp := KeyspaceProgress{
			Keyspace: u.Keyspace,
			progress: progress{
				Size: u.Size,
			},
		}

		for _, t := range u.Tables {
			tp := tableProgress[TableName{Keyspace: u.Keyspace, Table: t.Table}]
			tp.Table = t.Table
			tp.TombstoneGC = t.TombstoneGC
			tp.Size = t.Size
			if tp.Restored < tp.Size {
				tp.CompletedAt = nil
			}

			kp.Tables = append(kp.Tables, tp)
			kp.updateParentProgress(tp.progress)
		}

		if kp.Restored < kp.Size {
			kp.CompletedAt = nil
		}
		p.Keyspaces = append(p.Keyspaces, kp)
		p.updateParentProgress(kp.progress)
	}
	if p.Restored < p.Size {
		p.CompletedAt = nil
	}

	p.Views = slices.Clone(run.Views)
	for h, hp := range hostProgress {
		hp.Host = h
		p.Hosts = append(p.Hosts, hp)
	}
	return p
}

func progressCB(tableProgress map[TableName]TableProgress, hostProgress map[string]HostProgress, pr *RunProgress, now time.Time) {
	// Update table progress
	tn := TableName{Keyspace: pr.Keyspace, Table: pr.Table}
	tp := tableProgress[tn]

	tp.Downloaded += pr.Downloaded + pr.VersionedProgress
	tp.Restored += pr.Restored
	tp.Failed += pr.Failed
	tp.StartedAt = minTime(tp.StartedAt, pr.RestoreStartedAt)
	// We need to later validate that the table
	// indeed been completely restored.
	tp.CompletedAt = maxTime(tp.CompletedAt, pr.RestoreCompletedAt)
	if tp.Error == "" {
		tp.Error = pr.Error
	} else if pr.Error != "" {
		tp.Error = tp.Error + "\n" + pr.Error
	}
	tableProgress[tn] = tp

	// Update host progress
	hp := hostProgress[pr.Host]
	hp.ShardCnt = pr.ShardCnt
	hp.DownloadedBytes += pr.Downloaded + pr.VersionedProgress
	// We can update download duration on the fly,
	// but it's not possible with sync load&stream API.
	hp.DownloadDuration += timeSub(pr.DownloadStartedAt, pr.DownloadCompletedAt, now).Milliseconds()
	if validateTimeIsSet(pr.RestoreCompletedAt) {
		hp.RestoredBytes += pr.Restored
		hp.RestoreDuration += timeSub(pr.RestoreStartedAt, pr.RestoreCompletedAt, now).Milliseconds()
		hp.StreamedBytes += pr.Restored
		hp.StreamDuration += timeSub(pr.DownloadCompletedAt, pr.RestoreCompletedAt, now).Milliseconds()
	}
	hostProgress[pr.Host] = hp
}

// minTime chooses the smaller set time.
func minTime(a, b *time.Time) *time.Time {
	if !validateTimeIsSet(a) {
		return b
	}
	if !validateTimeIsSet(b) {
		return a
	}
	if a.Before(*b) {
		return a
	}
	return b
}

// maxTime chooses the bigger set time.
func maxTime(a, b *time.Time) *time.Time {
	if !validateTimeIsSet(a) {
		return b
	}
	if !validateTimeIsSet(b) {
		return a
	}
	if a.After(*b) {
		return a
	}
	return b
}

// updateParentProgress updates parents progress with child.
func (rp *progress) updateParentProgress(child progress) {
	rp.Restored += child.Restored
	rp.Downloaded += child.Downloaded
	rp.Failed += child.Failed
	rp.StartedAt = minTime(rp.StartedAt, child.StartedAt)
	rp.CompletedAt = maxTime(rp.CompletedAt, child.CompletedAt)
}

// Returns duration between end and start.
// If start is nil, returns 0.
// If end is nil, returns duration between now and start.
func timeSub(start, end *time.Time, now time.Time) time.Duration {
	if start != nil {
		if end != nil {
			return end.Sub(*start)
		}
		return now.Sub(*start)
	}
	return time.Duration(0)
}
