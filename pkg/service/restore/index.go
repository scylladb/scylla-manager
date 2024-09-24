// Copyright (C) 2024 ScyllaDB

package restore

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
)

// LocationWorkload represents aggregated restore workload
// in given backup location.
type LocationWorkload struct {
	Location

	Size   int64
	Tables []TableWorkload
}

// TableWorkload represents restore workload
// from many manifests for given table in given backup location.
type TableWorkload struct {
	Location
	TableName

	Size       int64
	RemoteDirs []RemoteDirWorkload
}

// RemoteDirWorkload represents restore workload
// for given table and manifest in given backup location.
type RemoteDirWorkload struct {
	TableName
	*ManifestInfo

	RemoteSSTableDir string
	Size             int64
	SSTables         []RemoteSSTable
}

// RemoteSSTable represents SSTable updated with size and version info from remote.
type RemoteSSTable struct {
	SSTable   // File names might contain versioned snapshot tag extension
	Size      int64
	Versioned bool
}

// SSTable represents files creating a single sstable.
type SSTable struct {
	ID    string
	Files []string
}

// IndexWorkload returns sstables to be restored aggregated by location, table and remote sstable dir.
func (w *tablesWorker) IndexWorkload(ctx context.Context, locations []Location) ([]LocationWorkload, error) {
	var workload []LocationWorkload
	for _, l := range locations {
		lw, err := w.indexLocationWorkload(ctx, l)
		if err != nil {
			return nil, errors.Wrapf(err, "index workload in %s", l)
		}
		workload = append(workload, lw)
	}
	return workload, nil
}

func (w *tablesWorker) indexLocationWorkload(ctx context.Context, location Location) (LocationWorkload, error) {
	rawWorkload, err := w.createRemoteDirWorkloads(ctx, location)
	if err != nil {
		return LocationWorkload{}, errors.Wrap(err, "create remote dir workloads")
	}
	if w.target.Continue {
		rawWorkload, err = w.filterPreviouslyRestoredSStables(rawWorkload)
		if err != nil {
			return LocationWorkload{}, errors.Wrap(err, "filter already restored sstables")
		}
	}
	return aggregateLocationWorkload(rawWorkload), nil
}

func (w *tablesWorker) createRemoteDirWorkloads(ctx context.Context, location Location) ([]RemoteDirWorkload, error) {
	var rawWorkload []RemoteDirWorkload
	err := w.forEachManifest(ctx, location, func(m ManifestInfoWithContent) error {
		return m.ForEachIndexIterWithError(nil, func(fm FilesMeta) error {
			if !unitsContainTable(w.run.Units, fm.Keyspace, fm.Table) {
				return nil
			}

			sstables, err := filesMetaToSSTables(fm)
			if err != nil {
				return errors.Wrapf(err, "convert files meta to sstables")
			}
			sstDir := m.LocationSSTableVersionDir(fm.Keyspace, fm.Table, fm.Version)
			remoteSSTables, err := w.adjustSSTablesWithRemote(ctx, w.randomHostFromLocation(location), sstDir, sstables)
			if err != nil {
				return errors.Wrap(err, "fetch sstables sizes")
			}

			var size int64
			for _, sst := range remoteSSTables {
				size += sst.Size
			}
			t := TableName{
				Keyspace: fm.Keyspace,
				Table:    fm.Table,
			}
			workload := RemoteDirWorkload{
				TableName:        t,
				ManifestInfo:     m.ManifestInfo,
				RemoteSSTableDir: sstDir,
				Size:             size,
				SSTables:         remoteSSTables,
			}
			rawWorkload = append(rawWorkload, workload)
			return nil
		})
	})
	if err != nil {
		return nil, errors.Wrap(err, "iterate over manifests")
	}
	return rawWorkload, nil
}

func (w *tablesWorker) filterPreviouslyRestoredSStables(rawWorkload []RemoteDirWorkload) ([]RemoteDirWorkload, error) {
	remoteSSTableDirToRestoredIDs := make(map[string][]string)
	err := forEachProgress(w.session, w.run.ClusterID, w.run.TaskID, w.run.ID, func(pr *RunProgress) {
		if validateTimeIsSet(pr.RestoreCompletedAt) {
			remoteSSTableDirToRestoredIDs[pr.RemoteSSTableDir] = append(remoteSSTableDirToRestoredIDs[pr.RemoteSSTableDir], pr.SSTableID...)
		}
	})
	if err != nil {
		return nil, errors.Wrap(err, "iterate over prev run progress")
	}
	if len(remoteSSTableDirToRestoredIDs) == 0 {
		return rawWorkload, nil
	}

	var filtered []RemoteDirWorkload
	for _, rw := range rawWorkload {
		var filteredSSTables []RemoteSSTable
		var size int64
		for _, sst := range rw.SSTables {
			if !slices.Contains(remoteSSTableDirToRestoredIDs[rw.RemoteSSTableDir], sst.ID) {
				filteredSSTables = append(filteredSSTables, sst)
				size += sst.Size
			}
		}
		if len(filteredSSTables) > 0 {
			filtered = append(filtered, RemoteDirWorkload{
				TableName:        rw.TableName,
				ManifestInfo:     rw.ManifestInfo,
				RemoteSSTableDir: rw.RemoteSSTableDir,
				Size:             size,
				SSTables:         filteredSSTables,
			})
		}
	}

	return filtered, nil
}

func (w *tablesWorker) initMetrics(workload []LocationWorkload) {
	// For now, the only persistent across task runs metrics are progress and remaining_bytes.
	// The rest: state, view_build_status, batch_size are calculated from scratch.
	w.metrics.ResetClusterMetrics(w.run.ClusterID)

	// Init remaining bytes
	for _, wl := range workload {
		for _, twl := range wl.Tables {
			for _, rdwl := range twl.RemoteDirs {
				w.metrics.SetRemainingBytes(metrics.RestoreBytesLabels{
					ClusterID:   rdwl.ClusterID.String(),
					SnapshotTag: rdwl.SnapshotTag,
					Location:    rdwl.Location.String(),
					DC:          rdwl.DC,
					Node:        rdwl.NodeID,
					Keyspace:    rdwl.Keyspace,
					Table:       rdwl.Table,
				}, rdwl.Size)
			}
		}
	}

	// Init progress
	var totalSize int64
	for _, u := range w.run.Units {
		totalSize += u.Size
	}
	var workloadSize int64
	for _, wl := range workload {
		workloadSize += wl.Size
	}
	w.metrics.SetProgress(metrics.RestoreProgressLabels{
		ClusterID:   w.run.ClusterID.String(),
		SnapshotTag: w.run.SnapshotTag,
	}, float64(totalSize-workloadSize)/float64(totalSize)*100)
}

func aggregateLocationWorkload(rawWorkload []RemoteDirWorkload) LocationWorkload {
	remoteDirWorkloads := make(map[TableName][]RemoteDirWorkload)
	for _, rw := range rawWorkload {
		remoteDirWorkloads[rw.TableName] = append(remoteDirWorkloads[rw.TableName], rw)
	}

	var tableWorkloads []TableWorkload
	for _, tw := range remoteDirWorkloads {
		var size int64
		for _, rdw := range tw {
			size += rdw.Size
		}
		tableWorkloads = append(tableWorkloads, TableWorkload{
			Location:   tw[0].Location,
			TableName:  tw[0].TableName,
			Size:       size,
			RemoteDirs: tw,
		})
	}

	var size int64
	for _, tw := range tableWorkloads {
		size += tw.Size
	}
	return LocationWorkload{
		Location: tableWorkloads[0].Location,
		Size:     size,
		Tables:   tableWorkloads,
	}
}

func (w *tablesWorker) adjustSSTablesWithRemote(ctx context.Context, host, remoteDir string, sstables map[string]SSTable) ([]RemoteSSTable, error) {
	versioned, err := ListVersionedFiles(ctx, w.client, w.run.SnapshotTag, host, remoteDir)
	if err != nil {
		return nil, errors.Wrap(err, "list versioned files")
	}

	remoteSSTables := make([]RemoteSSTable, 0, len(sstables))
	for id, sst := range sstables {
		rsst := RemoteSSTable{SSTable: SSTable{ID: id}}
		for _, f := range sst.Files {
			v, ok := versioned[f]
			if !ok {
				return nil, errors.Errorf("file %s is not present in listed versioned files", f)
			}

			rsst.Files = append(rsst.Files, v.FullName())
			rsst.Size += v.Size
			rsst.Versioned = rsst.Versioned || v.Version != ""
		}
		remoteSSTables = append(remoteSSTables, rsst)
	}

	return remoteSSTables, nil
}

func filesMetaToSSTables(fm FilesMeta) (map[string]SSTable, error) {
	const expectedSSTableFileCnt = 9
	sstables := make(map[string]SSTable, len(fm.Files)/expectedSSTableFileCnt)

	for _, f := range fm.Files {
		id, err := sstable.ExtractID(f)
		if err != nil {
			return nil, errors.Wrapf(err, "extract sstable component %s generation ID", f)
		}

		if sst, ok := sstables[id]; ok {
			sst.Files = append(sst.Files, f)
			sstables[id] = sst
		} else {
			sstables[id] = SSTable{
				ID:    id,
				Files: []string{f},
			}
		}
	}
	return sstables, nil
}
