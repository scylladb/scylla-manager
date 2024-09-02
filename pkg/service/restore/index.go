// Copyright (C) 2024 ScyllaDB

package restore

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// TableName represents full table name.
type TableName struct {
	Keyspace string
	Table    string
}

// TableWorkload represents aggregated restore workload
// from many manifests for given table.
type TableWorkload struct {
	TableName
	TableVersion string

	Size       int64
	RemoteDirs []RemoteDirWorkload
}

// RemoteDirWorkload represents restore workload
// for given table and manifest.
type RemoteDirWorkload struct {
	TableName
	Location     Location
	ClusterID    uuid.UUID
	DC           string
	NodeID       string
	TableVersion string

	RemoteSSTableDir string
	Size             int64
	SSTables         []RemoteSSTable
}

// SSTable represents files creating a single sstable.
type SSTable struct {
	ID    string
	Files []string
}

// RemoteSSTable represents SSTable updated with size from remote.
type RemoteSSTable struct {
	SSTable
	Size      int64
	Versioned bool
}

func (w *tablesWorker) IndexWorkload(ctx context.Context) ([]TableWorkload, error) {
	w.SetProgressMetric(0)
	var (
		rawIndex     = make(map[TableName][]RemoteDirWorkload)
		tableVersion = make(map[TableName]string)
		done         = make(map[string][]string)
		err          error
	)
	if w.target.Continue {
		done, err = w.doneIDs(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "query already restored sstables")
		}
	}

	for _, loc := range w.target.Location {
		err := w.forEachManifest(ctx, loc, func(m ManifestInfoWithContent) error {
			return m.ForEachIndexIterWithError(w.target.Keyspace, func(fm FilesMeta) error {
				sstables := filesMetaToSSTables(fm)
				sstDir := m.LocationSSTableVersionDir(fm.Keyspace, fm.Table, fm.Version)
				remoteSSTables, err := w.adjustSSTablesWithRemote(ctx, w.randomHostFromLocation(loc), sstDir, sstables)
				if err != nil {
					return errors.Wrap(err, "fetch sstables sizes")
				}
				remoteSSTables = filterDoneSSTables(remoteSSTables, done[sstDir])

				var size int64
				for _, sst := range remoteSSTables {
					size += sst.Size
				}

				t := TableName{
					Keyspace: fm.Keyspace,
					Table:    fm.Table,
				}

				v, ok := tableVersion[t]
				if !ok {
					v, err = query.GetTableVersion(w.clusterSession, t.Keyspace, t.Table)
					if err != nil {
						return errors.Wrap(err, "get table version")
					}
				}

				workload := RemoteDirWorkload{
					TableName:        t,
					Location:         m.Location,
					ClusterID:        m.ClusterID,
					DC:               m.DC,
					NodeID:           m.NodeID,
					TableVersion:     v,
					RemoteSSTableDir: sstDir,
					Size:             size,
					SSTables:         remoteSSTables,
				}
				w.SetRemainingBytesMetric(workload)
				rawIndex[t] = append(rawIndex[t], workload)
				return nil
			})
		})
		if err != nil {
			return nil, errors.Wrapf(err, "iterate over manifests in %s", loc)
		}
	}

	workload := make([]TableWorkload, 0, len(rawIndex))
	for t, dirWorkload := range rawIndex {
		var size int64
		for idx := range dirWorkload {
			size += dirWorkload[idx].Size
		}
		workload = append(workload, TableWorkload{
			TableName:    t,
			TableVersion: tableVersion[t],
			Size:         size,
			RemoteDirs:   dirWorkload,
		})
	}

	return workload, nil
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
				panic("no entry for: " + f)
			}

			rsst.Files = append(rsst.Files, v.FullName())
			rsst.Size += v.Size
			rsst.Versioned = rsst.Versioned || v.Version != ""
		}
		remoteSSTables = append(remoteSSTables, rsst)
	}

	return remoteSSTables, nil
}

func (w *tablesWorker) LogFileStats(ctx context.Context, workload []TableWorkload) {
	var totalSize int64
	for _, t := range workload {
		totalSize += t.Size
	}
	w.logger.Info(ctx, "Total workload statistics", "size", totalSize)
	for _, t := range workload {
		w.logger.Info(ctx, "Table workload statistics",
			"keyspace", t.Keyspace,
			"table", t.Table,
			"size", t.Size,
		)
		for idx := range t.RemoteDirs {
			d := &t.RemoteDirs[idx]
			if len(d.SSTables) == 0 {
				continue
			}
			big := d.SSTables[0].Size
			small := d.SSTables[len(d.SSTables)-1].Size
			cnt := int64(len(d.SSTables))
			avg := d.Size / cnt
			w.logger.Info(ctx, "Remote sst dir workload statistics",
				"keyspace", d.Keyspace,
				"table", d.Table,
				"remote sst dir", d.RemoteSSTableDir,
				"sst count", cnt,
				"total sst size", d.Size,
				"biggest sst size", big,
				"smallest sst size", small,
				"average sst size", avg,
			)
		}
	}
}

func (w *tablesWorker) doneIDs(ctx context.Context) (map[string][]string, error) {
	done := make(map[string][]string)
	err := forEachProgress(w.session, w.run.ClusterID, w.run.TaskID, w.run.ID, func(pr *RunProgress) {
		if validateTimeIsSet(pr.RestoreCompletedAt) {
			done[pr.RemoteSSTableDir] = append(done[pr.RemoteSSTableDir], pr.SSTableID...)
		} else {
			w.deleteRunProgress(ctx, pr)
		}
	})
	if err != nil {
		return nil, errors.Wrap(err, "iterate over prev run progress")
	}
	return done, nil
}

func filterDoneSSTables(sstables []RemoteSSTable, doneIDs []string) []RemoteSSTable {
	var out []RemoteSSTable
	done := strset.New(doneIDs...)
	for _, sst := range sstables {
		if !done.Has(sst.ID) {
			out = append(out, sst)
		}
	}
	return out
}

func filesMetaToSSTables(fm FilesMeta) map[string]SSTable {
	const expectedSSTableFileCnt = 9
	sstables := make(map[string]SSTable, len(fm.Files)/expectedSSTableFileCnt)

	for _, f := range fm.Files {
		id, err := sstable.ExtractID(f)
		if err != nil {
			panic(err)
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
	return sstables
}
