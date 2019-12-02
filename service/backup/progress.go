// Copyright (C) 2017 ScyllaDB

package backup

import (
	"sort"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/schema"
)

var (
	zeroTime time.Time
	maxTime  = time.Unix(1<<62-1, 0).UTC()
)

type tableKey struct {
	host     string
	keyspace string
	table    string
}

// aggregateProgress returns progress information classified by host, keyspace,
// and host tables.
func aggregateProgress(run *Run, vis ProgressVisitor) (Progress, error) {
	p := Progress{
		SnapshotTag: run.SnapshotTag,
		DC:          run.DC,
	}

	if len(run.Units) == 0 {
		return p, nil
	}

	tableMap := make(map[tableKey]*TableProgress)
	hosts := strset.New()
	if err := vis.ForEach(aggregateTableProgress(run, tableMap, hosts)); err != nil {
		return p, err
	}
	hostList := hosts.List()
	sort.Strings(hostList)

	for _, h := range hostList {
		host := HostProgress{
			Host: h,
			progress: progress{
				StartedAt:   &maxTime,
				CompletedAt: &zeroTime,
			},
		}
		for _, u := range run.Units {
			ks := KeyspaceProgress{
				Keyspace: u.Keyspace,
				progress: progress{
					StartedAt:   &maxTime,
					CompletedAt: &zeroTime,
				},
			}
			for _, t := range u.Tables {
				tp := tableMap[tableKey{h, u.Keyspace, t}]
				if tp != nil {
					tp.progress = extremeToNil(tp.progress)
				} else {
					tp = &TableProgress{Table: t}
				}
				ks.Tables = append(ks.Tables, *tp)
				ks.progress = calcParentProgress(ks.progress, tp.progress)
			}
			ks.progress = extremeToNil(ks.progress)
			host.Keyspaces = append(host.Keyspaces, ks)
			host.progress = calcParentProgress(host.progress, ks.progress)
		}
		host.progress = extremeToNil(host.progress)
		p.Hosts = append(p.Hosts, host)
		p.progress = calcParentProgress(p.progress, host.progress)
	}

	return p, nil
}

// aggregateTableProgress aggregates provided run progress per host table and
// returns it along with list of all aggregated hosts.
func aggregateTableProgress(run *Run, tableMap map[tableKey]*TableProgress, hosts *strset.Set) func(*RunProgress) {
	return func(pr *RunProgress) {
		tk := tableKey{pr.Host, run.Units[pr.Unit].Keyspace, pr.TableName}
		table, ok := tableMap[tk]
		if !ok {
			table = &TableProgress{
				Table: pr.TableName,
				// To distinguish between set and not set dates.
				progress: progress{
					StartedAt:   &maxTime,
					CompletedAt: &zeroTime,
				},
			}
			tableMap[tk] = table
			hosts.Add(pr.Host)
		}

		// Don't count metadata as progress.
		if pr.FileName == manifest {
			return
		}

		table.Size += pr.Size
		table.Uploaded += pr.Uploaded
		table.Skipped += pr.Skipped
		table.Failed += pr.Failed
		if pr.StartedAt != nil && pr.StartedAt.Before(*table.StartedAt) {
			table.StartedAt = pr.StartedAt
		}
		if pr.CompletedAt != nil {
			if table.CompletedAt != nil && pr.CompletedAt.After(*table.CompletedAt) {
				table.CompletedAt = pr.CompletedAt
			}
		} else {
			table.CompletedAt = nil
		}
		if pr.Error != "" {
			if table.Error == "" {
				table.Error = pr.Error
			} else {
				table.Error += ", " + pr.Error
			}
		}
	}
}

// extremeToNil converts from temporary extreme time values to nil.
func extremeToNil(prog progress) progress {
	if prog.StartedAt == &maxTime {
		prog.StartedAt = nil
	}
	if prog.CompletedAt == &zeroTime {
		prog.CompletedAt = nil
	}
	return prog
}

// calcParentProgress returns updated progress for the parent that will include
// child progress.
func calcParentProgress(parent, child progress) progress {
	parent.Size += child.Size
	parent.Uploaded += child.Uploaded
	parent.Skipped += child.Skipped
	parent.Failed += child.Failed

	if child.StartedAt != nil {
		// Use child start time as parent start time only if it started before
		// parent.
		if parent.StartedAt == nil || child.StartedAt.Before(*parent.StartedAt) {
			parent.StartedAt = child.StartedAt
		}
	}
	if child.CompletedAt != nil {
		// Use child end time as parent end time only if it ended after parent.
		if parent.CompletedAt != nil && child.CompletedAt.After(*parent.CompletedAt) {
			parent.CompletedAt = child.CompletedAt
		}
	} else {
		// Set parent end time to nil if any of its children are ending in nil.
		parent.CompletedAt = nil
	}

	return parent
}

// PercentComplete returns value from 0 to 100 representing percentage of successfully uploaded bytes so far.
func (p *progress) PercentComplete() int {
	if p.Uploaded == 0 {
		return 0
	}

	if p.Uploaded+p.Skipped >= p.Size {
		return 100
	}

	percent := 100 * (p.Uploaded + p.Skipped) / p.Size
	if percent >= 100 {
		percent = 99
	}

	return int(percent)
}

// ByteProgress returns how many bytes are already processed and how many bytes are left to completion.
func (p *progress) ByteProgress() (done, left int64) {
	done = p.Skipped + p.Uploaded
	return done, p.Size - done
}

//AvgUploadBandwidth bandwidth calculated by dividing bytes uploaded by time duration of operation.
func (p *progress) AvgUploadBandwidth() float64 {
	if p.StartedAt == nil {
		return 0
	}

	reference := timeutc.Now()
	if p.CompletedAt != nil {
		reference = *p.CompletedAt
	}

	uploadDuration := reference.Sub(*p.StartedAt)
	return float64(p.Uploaded) / uploadDuration.Seconds()
}

// ProgressVisitor knows how to iterate over list of RunProgress results.
type ProgressVisitor interface {
	ForEach(func(*RunProgress)) error
}

type progressVisitor struct {
	session *gocql.Session
	run     *Run
}

// NewProgressVisitor creates new progress iterator.
func NewProgressVisitor(run *Run, session *gocql.Session) ProgressVisitor {
	return &progressVisitor{
		session: session,
		run:     run,
	}
}

// ForEach iterates over each run progress and runs visit function on it.
// If visit wants to reuse RunProgress it must copy it because memory is reused
// between calls.
func (i *progressVisitor) ForEach(visit func(*RunProgress)) error {
	stmt, names := schema.BackupRunProgress.Select()

	iter := gocqlx.Query(i.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": i.run.ClusterID,
		"task_id":    i.run.TaskID,
		"run_id":     i.run.ID,
	}).Iter()

	pr := &RunProgress{}
	for iter.StructScan(pr) {
		visit(pr)
	}

	return iter.Close()
}
