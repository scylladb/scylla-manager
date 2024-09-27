// Copyright (C) 2024 ScyllaDB

package restore

import (
	"context"

	"github.com/pkg/errors"
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
