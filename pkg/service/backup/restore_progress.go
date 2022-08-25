package backup

import (
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
)

// aggregateProgress returns restore progress information classified by host, keyspace,
// and host tables. Host is understood as the host which backed up the data.
// (It can belong to an already non-existent cluster).
func (w *restoreWorker) aggregateProgress(run *RestoreRun) RestoreProgress {
	var (
		p = RestoreProgress{
			progress: progress{
				StartedAt:   &maxTime,
				CompletedAt: &zeroTime,
			},
			Stage: run.Stage,
		}
		tableMap = make(map[tableKey]TableProgress)
	)

	w.ForEachProgress(run, w.aggregateTableProgress(tableMap))

	// Swap host representation from nodeID to IP.
	ksMap := w.aggregateKeyspaceProgress(tableMap)
	hostMap := w.aggregateHostProgress(ksMap)

	for _, v := range hostMap {
		v.progress = extremeToNil(v.progress)
		p.progress = calcParentProgress(p.progress, v.progress)
		p.Hosts = append(p.Hosts, v)
	}

	p.progress = extremeToNil(p.progress)

	return p
}

// aggregateHostProgress aggregates restore progress per host based
// on already gathered restore progress per keyspace.
func (w *restoreWorker) aggregateHostProgress(ksMap map[keyspaceKey]KeyspaceProgress) map[hostKey]HostProgress {
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

// aggregateHostProgress aggregates restore progress per keyspace based
// on already gathered restore progress per table.
func (w *restoreWorker) aggregateKeyspaceProgress(tableMap map[tableKey]TableProgress) map[keyspaceKey]KeyspaceProgress {
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

// aggregateTableProgress returns function that can be used to aggregate
// restore progress per host table.
func (w *restoreWorker) aggregateTableProgress(tableMap map[tableKey]TableProgress) func(*RestoreRunProgress) {
	return func(pr *RestoreRunProgress) {
		var (
			tk = tableKey{
				host:     pr.ManifestIP,
				keyspace: pr.KeyspaceName,
				table:    pr.TableName,
			}
			tab TableProgress
			ok  bool
		)

		if tab, ok = tableMap[tk]; !ok {
			tab = TableProgress{
				progress: progress{
					StartedAt:   &maxTime,
					CompletedAt: &zeroTime,
				},
				Table: pr.TableName,
			}
		}

		// Check if progress was created with RecordSize
		if pr.AgentJobID == 0 {
			tab.Size += pr.Size
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
	}
}

// ForEachProgress iterates over all RestoreRunProgress that belong to run arg.
func (w *restoreWorker) ForEachProgress(run *RestoreRun, cb func(*RestoreRunProgress)) {
	iter := table.RestoreRunProgress.SelectQuery(w.managerSession).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	}).Iter()
	defer iter.Close()

	pr := new(RestoreRunProgress)
	for iter.StructScan(pr) {
		cb(pr)
	}
}

// ForEachTableProgress iterates over all RestoreRunProgress that belong
// to run, manifest and table specified in run arg.
func (w *restoreWorker) ForEachTableProgress(run *RestoreRun, cb func(*RestoreRunProgress)) {
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
		"keyspace_name": run.KeyspaceName,
		"table_name":    run.TableName,
	}).Iter()
	defer iter.Close()

	pr := new(RestoreRunProgress)
	for iter.StructScan(pr) {
		cb(pr)
	}
}
