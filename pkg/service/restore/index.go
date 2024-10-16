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

// Workload represents total restore workload.
type Workload struct {
	TotalSize    int64
	LocationSize map[Location]int64
	TableSize    map[TableName]int64
	RemoteDir    []RemoteDirWorkload
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
func (w *tablesWorker) IndexWorkload(ctx context.Context, locations []Location) (Workload, error) {
	var rawWorkload []RemoteDirWorkload
	for _, l := range locations {
		lw, err := w.indexLocationWorkload(ctx, l)
		if err != nil {
			return Workload{}, errors.Wrapf(err, "index workload in %s", l)
		}
		rawWorkload = append(rawWorkload, lw...)
	}
	workload := aggregateWorkload(rawWorkload)
	w.logWorkloadInfo(ctx, workload)
	return workload, nil
}

func (w *tablesWorker) indexLocationWorkload(ctx context.Context, location Location) ([]RemoteDirWorkload, error) {
	rawWorkload, err := w.createRemoteDirWorkloads(ctx, location)
	if err != nil {
		return nil, errors.Wrap(err, "create remote dir workloads")
	}
	if w.target.Continue {
		rawWorkload, err = w.filterPreviouslyRestoredSStables(ctx, rawWorkload)
		if err != nil {
			return nil, errors.Wrap(err, "filter already restored sstables")
		}
	}
	return rawWorkload, nil
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
			if size > 0 {
				rawWorkload = append(rawWorkload, workload)
			}
			return nil
		})
	})
	if err != nil {
		return nil, errors.Wrap(err, "iterate over manifests")
	}
	return rawWorkload, nil
}

func (w *tablesWorker) filterPreviouslyRestoredSStables(ctx context.Context, rawWorkload []RemoteDirWorkload) ([]RemoteDirWorkload, error) {
	w.logger.Info(ctx, "Filter out previously restored sstables")

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

	var (
		filtered     []RemoteDirWorkload
		skippedCount int
		skippedSize  int64
	)
	for _, rw := range rawWorkload {
		var filteredSSTables []RemoteSSTable
		var size int64
		for _, sst := range rw.SSTables {
			if !slices.Contains(remoteSSTableDirToRestoredIDs[rw.RemoteSSTableDir], sst.ID) {
				filteredSSTables = append(filteredSSTables, sst)
				size += sst.Size
			} else {
				skippedCount++
				skippedSize += sst.Size
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
		} else {
			w.logger.Info(ctx, "Completely filtered out remote sstable dir", "remote dir", rw.RemoteSSTableDir)
		}
	}

	w.logger.Info(ctx, "Filtered out sstables info", "count", skippedCount, "size", skippedSize)
	return filtered, nil
}

func (w *tablesWorker) initMetrics(workload Workload) {
	// For now, the only persistent across task runs metrics are progress and remaining_bytes.
	// The rest: state, view_build_status, batch_size are calculated from scratch.
	w.metrics.ResetClusterMetrics(w.run.ClusterID)

	// Init remaining bytes
	for _, rdw := range workload.RemoteDir {
		w.metrics.SetRemainingBytes(metrics.RestoreBytesLabels{
			ClusterID:   rdw.ClusterID.String(),
			SnapshotTag: rdw.SnapshotTag,
			Location:    rdw.Location.String(),
			DC:          rdw.DC,
			Node:        rdw.NodeID,
			Keyspace:    rdw.Keyspace,
			Table:       rdw.Table,
		}, rdw.Size)
	}

	// Init progress
	var totalSize int64
	for _, u := range w.run.Units {
		totalSize += u.Size
	}
	w.metrics.SetProgress(metrics.RestoreProgressLabels{
		ClusterID:   w.run.ClusterID.String(),
		SnapshotTag: w.run.SnapshotTag,
	}, float64(totalSize-workload.TotalSize)/float64(totalSize)*100)
}

func (w *tablesWorker) logWorkloadInfo(ctx context.Context, workload Workload) {
	for loc, size := range workload.LocationSize {
		w.logger.Info(ctx, "Location workload",
			"location", loc,
			"size", size)
	}
	for tab, size := range workload.TableSize {
		w.logger.Info(ctx, "Table workload",
			"table", tab,
			"size", size)
	}
	for _, rdw := range workload.RemoteDir {
		cnt := int64(len(rdw.SSTables))
		if cnt == 0 {
			w.logger.Info(ctx, "Empty remote dir workload", "path", rdw.RemoteSSTableDir)
			continue
		}

		var maxSST int64
		for _, sst := range rdw.SSTables {
			maxSST = max(maxSST, sst.Size)
		}
		w.logger.Info(ctx, "Remote sstable dir workload info",
			"path", rdw.RemoteSSTableDir,
			"total size", rdw.Size,
			"max size", maxSST,
			"average size", rdw.Size/cnt,
			"count", cnt)
	}
}

func aggregateWorkload(rawWorkload []RemoteDirWorkload) Workload {
	var (
		totalSize    int64
		locationSize = make(map[Location]int64)
		tableSize    = make(map[TableName]int64)
	)
	for _, rdw := range rawWorkload {
		totalSize += rdw.Size
		locationSize[rdw.Location] += rdw.Size
		tableSize[rdw.TableName] += rdw.Size
	}
	return Workload{
		TotalSize:    totalSize,
		LocationSize: locationSize,
		TableSize:    tableSize,
		RemoteDir:    rawWorkload,
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
