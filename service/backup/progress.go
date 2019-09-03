// Copyright (C) 2017 ScyllaDB

package backup

import (
	"sort"
	"time"
)

type tableKey struct {
	host     string
	keyspace string
	table    string
}

// aggregateProgress returns progress information classified by host, keyspace,
// and host tables.
func aggregateProgress(run *Run, prog []*RunProgress) Progress {
	p := Progress{
		DC: run.DC,
	}
	if len(run.Units) == 0 || len(prog) == 0 {
		return p
	}

	zeroTime := &time.Time{}
	t := time.Unix(1<<62-1, 0).UTC()
	maxTime := &t
	hostsMap := make(map[string]struct{})
	tableMap := make(map[tableKey]*TableProgress)
	for _, pr := range prog {
		tk := tableKey{pr.Host, run.Units[pr.Unit].Keyspace, pr.TableName}
		table, ok := tableMap[tk]
		if !ok {
			table = &TableProgress{
				Table: pr.TableName,
				// To distinguish between set and not set dates.
				progress: progress{
					StartedAt:   maxTime,
					CompletedAt: zeroTime,
				},
			}
			tableMap[tk] = table
			hostsMap[pr.Host] = struct{}{}
		}

		// Don't count metadata as progress.
		if pr.FileName == manifest {
			continue
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

	var hosts []string
	for h := range hostsMap {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)

	for _, h := range hosts {
		host := HostProgress{
			Host: h,
			progress: progress{
				StartedAt:   maxTime,
				CompletedAt: zeroTime,
			},
		}
		for _, u := range run.Units {
			ks := KeyspaceProgress{
				Keyspace: u.Keyspace,
				progress: progress{
					StartedAt:   maxTime,
					CompletedAt: zeroTime,
				},
			}
			for _, t := range u.Tables {
				tp := tableMap[tableKey{h, u.Keyspace, t}]
				tp.progress = extremeToNil(tp.progress, zeroTime, maxTime)
				ks.Tables = append(ks.Tables, *tp)
				ks.progress = calcParentProgress(ks.progress, tp.progress)
			}
			host.Keyspaces = append(host.Keyspaces, ks)
			ks.progress = extremeToNil(ks.progress, zeroTime, maxTime)
			host.progress = calcParentProgress(host.progress, ks.progress)
		}
		p.Hosts = append(p.Hosts, host)
		host.progress = extremeToNil(host.progress, zeroTime, maxTime)
		p.progress = calcParentProgress(p.progress, host.progress)
	}

	return p
}

func extremeToNil(prog progress, zero, max *time.Time) progress {
	if prog.StartedAt == max {
		prog.StartedAt = nil
	}
	if prog.CompletedAt == zero {
		prog.CompletedAt = nil
	}
	return prog
}

func calcParentProgress(parent, child progress) progress {
	parent.Size += child.Size
	parent.Uploaded += child.Uploaded
	parent.Skipped += child.Skipped
	parent.Failed += child.Failed

	if child.StartedAt != nil {
		if parent.StartedAt == nil || child.StartedAt.Before(*parent.StartedAt) {
			parent.StartedAt = child.StartedAt
		}
	}
	if child.CompletedAt != nil {
		if parent.CompletedAt != nil && child.CompletedAt.After(*parent.CompletedAt) {
			parent.CompletedAt = child.CompletedAt
		}
	} else {
		parent.CompletedAt = nil
	}

	return parent
}
