// Copyright (C) 2026 ScyllaDB

package restore

import (
	"context"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/restore/tablet"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	cqlTable "github.com/scylladb/scylla-manager/v3/pkg/table"
	"golang.org/x/sync/errgroup"
)

// Workload represents total restore workload.
type Workload struct {
	TotalSize    int64
	LocationSize map[Location]int64
	TableSize    map[TableName]int64
	RemoteDir    []RemoteDirWorkload

	TabletAwareWorkload tablet.Workload
}

// NativeRestoreSupport validates that native restore can be used for all sstables in the workload.
func (w Workload) NativeRestoreSupport() error {
	for _, rdw := range w.RemoteDir {
		for _, sst := range rdw.SSTables {
			if err := sst.NativeRestoreSupport(); err != nil {
				return errors.Wrapf(err, "%s: %s.%s", rdw.NodeID, rdw.Keyspace, rdw.Table)
			}
		}
	}
	return nil
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
	SSTable // SSTable.Files and SSTable.TOC might contain versioned snapshot tag extension

	Size      int64
	Versioned bool
}

// NativeRestoreSupport validates that native restore can be used for given sstable.
func (sst RemoteSSTable) NativeRestoreSupport() error {
	if sst.Versioned {
		return errors.New("native restore api does not support restoration of versioned sstables files")
	}
	if sst.ID.Type == sstable.IntegerID {
		return errors.New("native restore api does not support restoration of sstables with integer based IDs")
	}
	return nil
}

// SSTable represents files creating a single sstable.
type SSTable struct {
	ID    sstable.ID
	TOC   string
	Files []string
}

// IndexWorkload returns sstables to be restored aggregated by location, table and remote sstable dir.
func (w *tablesWorker) IndexWorkload(ctx context.Context) (Workload, error) {
	// Read remote manifests and store their indexes on disk just once
	// for both regular and tablet aware restore workload indexing.
	// Indexes will be read from disk multiple times.
	var manifests []ManifestInfoWithContent
	var mu sync.Mutex

	eg := errgroup.Group{}
	eg.SetLimit(runtime.NumCPU())
	for _, l := range w.target.locationInfo {
		eg.Go(func() error {
			return w.forEachManifest(ctx, l, func(m ManifestInfoWithContent) error {
				mu.Lock()
				manifests = append(manifests, m)
				mu.Unlock()
				return nil
			})
		})
	}
	if err := eg.Wait(); err != nil {
		return Workload{}, errors.Wrap(err, "read remote manifests")
	}

	tabletWorkload, err := w.indexTabletAwareWorkload(ctx, manifests)
	if err != nil {
		return Workload{}, errors.Wrap(err, "index tablet aware workload")
	}

	tables := make(map[cqlTable.CQLTable]struct{})
	for _, u := range w.run.Units {
		for _, t := range u.Tables {
			tables[cqlTable.CQLTable{Keyspace: u.Keyspace, Name: t.Table}] = struct{}{}
		}
	}
	// Don't index tables already indexed and compatible with tablet aware restore
	for t := range tabletWorkload {
		delete(tables, t)
	}

	rawWorkload, err := w.indexLocationWorkload(ctx, tables, manifests)
	if err != nil {
		return Workload{}, errors.Wrapf(err, "index workload")
	}
	workload := aggregateWorkload(rawWorkload)
	workload.TabletAwareWorkload = tabletWorkload
	w.logWorkloadInfo(ctx, workload)
	return workload, nil
}

func (w *tablesWorker) indexTabletAwareWorkload(ctx context.Context, manifests []ManifestInfoWithContent) (tablet.Workload, error) {
	tables := make(map[cqlTable.CQLTable]struct{})
	for _, u := range w.run.Units {
		for _, t := range u.Tables {
			tables[cqlTable.CQLTable{Keyspace: u.Keyspace, Name: t.Table}] = struct{}{}
		}
	}

	tw := tablet.NewIndexWorker(w.logger, w.client, w.clusterSession, w.nodeConfig, w.target.DCMappings)
	return tw.Index(ctx, tables, manifests)
}

func (w *tablesWorker) indexLocationWorkload(ctx context.Context, tables map[cqlTable.CQLTable]struct{}, manifests []ManifestInfoWithContent) ([]RemoteDirWorkload, error) {
	rawWorkload, err := w.createRemoteDirWorkloads(ctx, tables, manifests)
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

func (w *tablesWorker) createRemoteDirWorkloads(ctx context.Context, tables map[cqlTable.CQLTable]struct{}, manifests []ManifestInfoWithContent) ([]RemoteDirWorkload, error) {
	var rawWorkload []RemoteDirWorkload
	for _, m := range manifests {
		err := m.ForEachIndexIterWithError(nil, func(fm FilesMeta) error {
			t := cqlTable.CQLTable{Keyspace: fm.Keyspace, Name: fm.Table}
			if _, ok := tables[t]; !ok {
				return nil
			}

			sstables, err := filesMetaToSSTables(fm)
			if err != nil {
				return errors.Wrapf(err, "convert files meta to sstables")
			}
			sstDir := m.LocationSSTableVersionDir(fm.Keyspace, fm.Table, fm.Version)
			remoteSSTables, err := w.adjustSSTablesWithRemote(ctx, w.anyHost(m.Location), sstDir, sstables)
			if err != nil {
				return errors.Wrap(err, "fetch sstables sizes")
			}

			var size int64
			for _, sst := range remoteSSTables {
				size += sst.Size
			}
			workload := RemoteDirWorkload{
				TableName: TableName{
					Keyspace: fm.Keyspace,
					Table:    fm.Table,
				},
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
		if err != nil {
			return nil, errors.Wrap(err, "iterate over manifest index")
		}
	}
	return rawWorkload, nil
}

func (w *tablesWorker) filterPreviouslyRestoredSStables(ctx context.Context, rawWorkload []RemoteDirWorkload) ([]RemoteDirWorkload, error) {
	w.logger.Info(ctx, "Filter out previously restored sstables")

	remoteSSTableDirToRestoredIDs := make(map[string][]string)
	seq := newRunProgressSeq()
	for pr := range seq.All(w.run.ClusterID, w.run.TaskID, w.run.ID, w.session) {
		if validateTimeIsSet(pr.RestoreCompletedAt) {
			remoteSSTableDirToRestoredIDs[pr.RemoteSSTableDir] = append(remoteSSTableDirToRestoredIDs[pr.RemoteSSTableDir], pr.SSTableID...)
		}
	}
	if seq.err != nil {
		return nil, errors.Wrap(seq.err, "iterate over prev run progress")
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
			if !slices.Contains(remoteSSTableDirToRestoredIDs[rw.RemoteSSTableDir], sst.ID.ID) {
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
	for _, tm := range workload.TabletAwareWorkload {
		w.logger.Info(ctx, "Tablet aware table workload",
			"keyspace", tm.Table.Keyspace,
			"table", tm.Table.Name,
			"size", tm.Size,
			"files count", tm.FileCnt,
		)
	}

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

func (w *tablesWorker) adjustSSTablesWithRemote(ctx context.Context, host, remoteDir string, sstables map[sstable.ID]SSTable) ([]RemoteSSTable, error) {
	versioned, err := backup.ListVersionedFiles(ctx, w.client, w.run.SnapshotTag, host, remoteDir)
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

			fullName := v.FullName()
			rsst.Files = append(rsst.Files, fullName)
			rsst.Size += v.Size
			rsst.Versioned = rsst.Versioned || v.Version != ""
			if f == sst.TOC {
				rsst.TOC = fullName
			}
		}
		remoteSSTables = append(remoteSSTables, rsst)
	}

	return remoteSSTables, nil
}

func (w *tablesWorker) anyHost(loc Location) string {
	for _, l := range w.target.locationInfo {
		if l.Location == loc {
			return l.AnyHost()
		}
	}
	return ""
}

func filesMetaToSSTables(fm FilesMeta) (map[sstable.ID]SSTable, error) {
	const expectedSSTableFileCnt = 9
	sstables := make(map[sstable.ID]SSTable, len(fm.Files)/expectedSSTableFileCnt)

	for _, f := range fm.Files {
		id, err := sstable.ParseID(f)
		if err != nil {
			return nil, errors.Wrapf(err, "parse SSTable component %q ID", f)
		}

		sst := sstables[id]
		sst.ID = id
		sst.Files = append(sst.Files, f)
		if strings.HasSuffix(f, string(sstable.ComponentTOC)) {
			sst.TOC = f
		}
		sstables[id] = sst
	}
	return sstables, nil
}
