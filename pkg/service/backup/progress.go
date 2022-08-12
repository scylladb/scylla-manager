// Copyright (C) 2017 ScyllaDB

package backup

import (
	"sort"
	"time"

	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

var (
	zeroTime time.Time
	maxTime  = time.Unix(1<<62-1, 0).UTC()
)

type hostKey struct {
	host string
}

type keyspaceKey struct {
	host     string
	keyspace string
}

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
		Stage:       run.Stage,
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
func aggregateTableProgress(run *Run, tableMap map[tableKey]*TableProgress, hosts *strset.Set) func(*RunProgress) error {
	return func(pr *RunProgress) error {
		table := &TableProgress{}

		table.Table = pr.TableName
		table.Size = pr.Size
		table.Uploaded = pr.Uploaded
		table.Skipped = pr.Skipped
		table.Failed = pr.Failed
		if pr.StartedAt != nil {
			table.StartedAt = pr.StartedAt
		} else {
			table.StartedAt = &maxTime
		}
		if pr.CompletedAt != nil {
			table.CompletedAt = pr.CompletedAt
		} else {
			table.CompletedAt = &zeroTime
		}
		table.Error = pr.Error

		tableMap[tableKey{pr.Host, run.Units[pr.Unit].Keyspace, pr.TableName}] = table

		hosts.Add(pr.Host)

		return nil
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

	parent.StartedAt = calcParentStartedAt(parent.StartedAt, child.StartedAt)
	parent.CompletedAt = calcParentCompletedAt(parent.CompletedAt, child.CompletedAt)

	return parent
}

func calcParentStartedAt(parent *time.Time, child *time.Time) *time.Time {
	if child != nil {
		// Use child start time as parent start time only if it started before
		// parent.
		if parent == nil || child.Before(*parent) {
			return child
		}
	}
	return parent
}

func calcParentCompletedAt(parent *time.Time, child *time.Time) *time.Time {
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

// AvgUploadBandwidth bandwidth calculated by dividing bytes uploaded by time duration of operation.
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
	ForEach(func(*RunProgress) error) error
}

type progressVisitor struct {
	session gocqlx.Session
	run     *Run
}

// NewProgressVisitor creates new progress iterator.
func NewProgressVisitor(run *Run, session gocqlx.Session) ProgressVisitor {
	return &progressVisitor{
		session: session,
		run:     run,
	}
}

// ForEach iterates over each run progress and runs visit function on it.
// If visit wants to reuse RunProgress it must copy it because memory is reused
// between calls.
func (i *progressVisitor) ForEach(visit func(*RunProgress) error) error {
	iter := table.BackupRunProgress.SelectQuery(i.session).BindMap(qb.M{
		"cluster_id": i.run.ClusterID,
		"task_id":    i.run.TaskID,
		"run_id":     i.run.ID,
	}).Iter()

	pr := new(RunProgress)
	for iter.StructScan(pr) {
		if err := visit(pr); err != nil {
			iter.Close()
			return err
		}
	}

	return iter.Close()
}

// aggregateRestoreProgress returns restore progress information classified by host, keyspace,
// and host tables. Host is understood as the host which backed up the data.
// (It can belong to an already non-existent cluster).
func (s *Service) aggregateRestoreProgress(run *RestoreRun) (RestoreProgress, error) {
	var (
		p = RestoreProgress{
			progress: progress{
				StartedAt:   &maxTime,
				CompletedAt: &zeroTime,
			},
			Stage: run.Stage,
		}
		tableMap = make(map[tableKey]*TableProgress)
		IDtoIP   = make(map[string]string)
	)

	if err := s.ForEachRestoreProgressIter(bindForAll(run), aggregateTableRestoreProgress(tableMap, IDtoIP)); err != nil {
		return p, err
	}

	// Swap host representation from nodeID to IP.
	resolvedTables := resolveTableHost(tableMap, IDtoIP)
	ksMap := aggregateKeyspaceRestoreProgress(resolvedTables)
	hostMap := aggregateHostRestoreProgress(ksMap)

	for _, v := range hostMap {
		v.progress = extremeToNil(v.progress)
		p.progress = calcParentProgress(p.progress, v.progress)
		p.Hosts = append(p.Hosts, v)
	}

	p.progress = extremeToNil(p.progress)

	return p, nil
}

// aggregateHostRestoreProgress aggregates restore progress per host based
// on already gathered restore progress per keyspace.
func aggregateHostRestoreProgress(ksMap map[keyspaceKey]KeyspaceProgress) map[hostKey]HostProgress {
	var (
		hostMap = make(map[hostKey]HostProgress)
		pr      HostProgress
		ok      bool
	)

	for k, v := range ksMap {
		v.progress = extremeToNil(v.progress)
		hKey := hostKey{host: k.host}

		if pr, ok = hostMap[hKey]; !ok {
			pr = HostProgress{
				progress: progress{
					StartedAt:   &maxTime,
					CompletedAt: &zeroTime,
				},
				Host: k.host,
			}
		}

		pr.Keyspaces = append(pr.Keyspaces, v)
		pr.progress = calcParentProgress(pr.progress, v.progress)
		hostMap[hKey] = pr
	}

	return hostMap
}

// aggregateHostRestoreProgress aggregates restore progress per keyspace based
// on already gathered restore progress per table.
func aggregateKeyspaceRestoreProgress(tableMap map[tableKey]TableProgress) map[keyspaceKey]KeyspaceProgress {
	var (
		ksMap = make(map[keyspaceKey]KeyspaceProgress)
		pr    KeyspaceProgress
		ok    bool
	)

	for k, v := range tableMap {
		v.progress = extremeToNil(v.progress)
		ksKey := keyspaceKey{
			host:     k.host,
			keyspace: k.keyspace,
		}

		if pr, ok = ksMap[ksKey]; !ok {
			pr = KeyspaceProgress{
				progress: progress{
					StartedAt:   &maxTime,
					CompletedAt: &zeroTime,
				},
				Keyspace: k.keyspace,
			}
		}

		pr.Tables = append(pr.Tables, v)
		pr.progress = calcParentProgress(pr.progress, v.progress)
		ksMap[ksKey] = pr
	}

	return ksMap
}

// resolveTableHost swaps host representation in tableMap form nodeID to IP.
// In case multiple hosts have the same IP,
// their progress information is squashed into one.
func resolveTableHost(tableMap map[tableKey]*TableProgress, IDtoIP map[string]string) map[tableKey]TableProgress {
	var (
		resolved = make(map[tableKey]TableProgress)
		pr       TableProgress
		ok       bool
	)

	for k, v := range tableMap {
		k.host = IDtoIP[k.host]

		if pr, ok = resolved[k]; !ok {
			resolved[k] = *v
		} else {
			pr.progress = calcParentProgress(pr.progress, v.progress)
			resolved[k] = pr
		}
	}

	return resolved
}

// aggregateTableRestoreProgress returns function that can be used to aggregate
// restore progress per host table.
func aggregateTableRestoreProgress(tableMap map[tableKey]*TableProgress, IDtoIP map[string]string) func(*RestoreRunProgress) error {
	return func(pr *RestoreRunProgress) error {
		tk := tableKey{
			host:     pr.NodeID, // For now host is represented by its nodeID.
			keyspace: pr.KeyspaceName,
			table:    pr.TableName,
		}

		var tab *TableProgress
		if tab = tableMap[tk]; tab == nil {
			tab = &TableProgress{
				progress: progress{
					StartedAt:   &maxTime,
					CompletedAt: &zeroTime,
				},
				Table: pr.TableName,
			}
		}

		// Check if progress was created with RecordRestoreSize
		if pr.AgentJobID == 0 {
			tab.Size += pr.Size
			IDtoIP[pr.NodeID] = pr.Host
		} else {
			tab.Uploaded += pr.Uploaded
			tab.Skipped += pr.Skipped
			tab.Failed += pr.Failed

			tab.StartedAt = calcParentStartedAt(tab.StartedAt, pr.StartedAt)
			tab.CompletedAt = calcParentCompletedAt(tab.CompletedAt, pr.CompletedAt)
		}

		if tab.Error == "" {
			tab.Error = pr.Error
		}

		tableMap[tk] = tab

		return nil
	}
}

// bindForAll returns map that can be used to bind gocql query
// which returns all RestoreRunProgress of specified RestoreRun.
func bindForAll(run *RestoreRun) map[string]any {
	return map[string]any{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	}
}

// bindForOngoing returns map that can be used to bind gocql query
// which returns RestoreRunProgress of specified RestoreRun that are still ongoing.
func bindForOngoing(run *RestoreRun) map[string]any {
	b := bindForAll(run)

	b["node_id"] = run.NodeID
	b["keyspace_name"] = run.Keyspace
	b["table_name"] = run.Table

	return b
}

// ForEachRestoreProgressIter iterates over all RestoreRunProgress captured
// by bind and calls callback function on them.
func (s *Service) ForEachRestoreProgressIter(bind map[string]any, cb func(*RestoreRunProgress) error) error {
	iter := table.RestoreRunProgress.SelectQuery(s.session).BindMap(bind).Iter()

	pr := new(RestoreRunProgress)
	for iter.StructScan(pr) {
		if err := cb(pr); err != nil {
			iter.Close()
			return err
		}
	}

	return iter.Close()
}
