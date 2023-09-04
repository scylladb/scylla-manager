// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"path"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"go.uber.org/atomic"

	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

type schemaWorker struct {
	worker

	hosts         []string
	generationCnt atomic.Int64
	miwc          ManifestInfoWithContent // Currently restored manifest
	// Maps original SSTable name to its existing older version (with respect to currently restored snapshot tag)
	// that should be used during the restore procedure. It should be initialized per each restored table.
	versionedFiles VersionedMap
}

// restore downloads all backed-up schema files to each node in the cluster. This approach is necessary because
// it's not possible to alter gc_grace_seconds or tombstone_gc on schema tables (safety requirement for nodetool refresh).
// It introduces great data duplication, but is necessary in order to simulate schema repair on each node.
// Luckily, schema files are small, so this shouldn't be noticeable in terms of performance.
// When all files are downloaded, they are restored using nodetool refresh.
// Note that due to small schema size:
// - resuming schema restoration will always start from scratch
// - schema restoration does not use long polling for updating download progress
// Adding the ability to resume schema restoration might be added in the future.
func (w *schemaWorker) restore(ctx context.Context, target Target) error {
	return errors.Wrap(w.stageRestoreData(ctx, target), "restore data")
}

func (w *schemaWorker) stageRestoreData(ctx context.Context, target Target) error {
	w.run.Stage = StageData
	w.insertRun(ctx)

	w.AwaitSchemaAgreement(ctx, w.clusterSession)
	w.logger.Info(ctx, "Started restoring schema")
	defer w.logger.Info(ctx, "Restoring schema finished")

	status, err := w.client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get status")
	}
	// Clean upload dirs.
	// This is required as we rename SSTables during download in order to avoid name overlaps.
	for _, u := range w.run.Units {
		for _, t := range u.Tables {
			version, err := query.GetTableVersion(w.clusterSession, u.Keyspace, t.Table)
			if err != nil {
				return err
			}
			uploadDir := UploadTableDir(u.Keyspace, t.Table, version)

			for _, h := range status {
				if err := w.cleanUploadDir(ctx, h.Addr, uploadDir, nil); err != nil {
					return err
				}
			}
		}
	}
	// Download files
	for _, l := range target.Location {
		if err := w.locationDownloadHandler(ctx, target, l); err != nil {
			return err
		}
	}
	// Set restore start in all run progresses
	err = forEachProgress(w.clusterSession, w.run.ClusterID, w.run.TaskID, w.run.ID, func(pr *RunProgress) {
		pr.setRestoreStartedAt()
		w.insertRunProgress(ctx, pr)
	})
	if err != nil {
		w.logger.Error(ctx, "Couldn't set restore start", "error", err)
	}

	// Load schema SSTables on all nodes
	f := func(i int) error {
		host := status[i]
		for _, ks := range w.run.Units {
			for _, t := range ks.Tables {
				if _, err := w.client.LoadSSTables(ctx, host.Addr, ks.Keyspace, t.Table, false, false); err != nil {
					return errors.Wrap(err, "restore schema")
				}
			}
		}
		return nil
	}

	notify := func(i int, err error) {
		w.logger.Error(ctx, "Failed to load schema on host",
			"host", status[i],
			"error", err,
		)
	}

	if err := parallel.Run(len(status), parallel.NoLimit, f, notify); err != nil {
		return err
	}
	// Set restore completed in all run progresses
	err = forEachProgress(w.clusterSession, w.run.ClusterID, w.run.TaskID, w.run.ID, func(pr *RunProgress) {
		pr.setRestoreCompletedAt()
		w.insertRunProgress(ctx, pr)
	})
	if err != nil {
		w.logger.Error(ctx, "Couldn't set restore end", "error", err)
	}

	return nil
}

func (w *schemaWorker) locationDownloadHandler(ctx context.Context, target Target, location Location) error {
	w.logger.Info(ctx, "Downloading schema from location", "location", location)
	defer w.logger.Info(ctx, "Downloading schema from location finished", "location", location)

	w.run.Location = location.String()

	if err := w.initHosts(ctx, location); err != nil {
		return errors.Wrap(err, "initialize hosts")
	}

	tableDownloadHandler := func(fm FilesMeta) error {
		w.logger.Info(ctx, "Downloading schema table", "keyspace", fm.Keyspace, "table", fm.Table)
		defer w.logger.Info(ctx, "Downloading schema table finished", "keyspace", fm.Keyspace, "table", fm.Table)

		w.run.Table = fm.Table
		w.run.Keyspace = fm.Keyspace

		return w.workFunc(ctx, target, fm)
	}

	manifestDownloadHandler := func(miwc ManifestInfoWithContent) error {
		w.logger.Info(ctx, "Downloading schema from manifest", "manifest", miwc.ManifestInfo)
		defer w.logger.Info(ctx, "Downloading schema from manifest finished", "manifest", miwc.ManifestInfo)

		w.miwc = miwc
		w.run.ManifestPath = miwc.Path()
		w.insertRun(ctx)

		return miwc.ForEachIndexIterWithError(target.Keyspace, tableDownloadHandler)
	}

	return w.forEachRestoredManifest(ctx, location, manifestDownloadHandler)
}

func (w *schemaWorker) workFunc(ctx context.Context, target Target, fm FilesMeta) error {
	version, err := query.GetTableVersion(w.clusterSession, fm.Keyspace, fm.Table)
	if err != nil {
		return err
	}

	var (
		srcDir = w.miwc.LocationSSTableVersionDir(fm.Keyspace, fm.Table, fm.Version)
		dstDir = UploadTableDir(fm.Keyspace, fm.Table, version)
	)

	w.logger.Info(ctx, "Start downloading schema files",
		"keyspace", fm.Keyspace,
		"table", fm.Table,
		"src_dir", srcDir,
		"dst_dir", dstDir,
		"files", fm.Files,
	)

	w.versionedFiles, err = ListVersionedFiles(ctx, w.client, w.run.SnapshotTag, w.hosts[0], srcDir)
	if err != nil {
		return errors.Wrap(err, "initialize versioned SSTables")
	}
	if len(w.versionedFiles) > 0 {
		w.logger.Info(ctx, "Chosen versioned SSTables",
			"dir", srcDir,
			"versioned_files", w.versionedFiles,
		)
	}

	idMapping := w.getFileNamesMapping(fm.Files, false)
	uuidMapping := w.getFileNamesMapping(fm.Files, true)

	f := func(i int) error {
		host := w.hosts[i]
		nodeInfo, err := w.client.NodeInfo(ctx, host)
		if err != nil {
			return errors.Wrapf(err, "get node info on host %s", host)
		}

		renamedSSTables := idMapping
		if nodeInfo.SstableUUIDFormat {
			renamedSSTables = uuidMapping
		}

		if err := w.checkAvailableDiskSpace(ctx, host); err != nil {
			return errors.Wrapf(err, "validate free disk space on host %s", host)
		}

		start := timeutc.Now()

		fHost := func(j int) error {
			file := fm.Files[j]
			// Rename SSTable in the destination in order to avoid name conflicts
			dstFile := renamedSSTables[file]
			// Take the correct version of restored file
			srcFile := file
			if v, ok := w.versionedFiles[file]; ok {
				srcFile = v.FullName()
			}

			srcPath := path.Join(srcDir, srcFile)
			dstPath := path.Join(dstDir, dstFile)

			return w.client.RcloneCopyFile(ctx, host, dstPath, srcPath)
		}

		notifyHost := func(j int, err error) {
			w.logger.Error(ctx, "Failed to download schema SSTable",
				"host", host,
				"file", fm.Files[j],
				"error", err,
			)
		}

		// Rely on rclone ability to limit number of concurrent transfers
		err = parallel.Run(len(fm.Files), parallel.NoLimit, fHost, notifyHost)
		if err != nil {
			return errors.Wrapf(err, "download renamed SSTables on host: %s", host)
		}
		end := timeutc.Now()
		// In order to ensure that the size calculated in newUnits matches the sum of restored bytes from
		// run progresses, insert only fraction of the whole downloaded size. This is caused by the data duplication.
		proportionalSize := int64((int(fm.Size) + i) / len(w.hosts))

		w.insertRunProgress(ctx, &RunProgress{
			ClusterID:           w.run.ClusterID,
			TaskID:              w.run.TaskID,
			RunID:               w.run.ID,
			ManifestPath:        w.run.ManifestPath,
			Keyspace:            w.run.Keyspace,
			Table:               w.run.Table,
			Host:                host,
			DownloadStartedAt:   &start,
			DownloadCompletedAt: &end,
			Downloaded:          proportionalSize,
		})

		return nil
	}

	notify := func(i int, err error) {
		w.logger.Error(ctx, "Failed to restore schema on host",
			"host", w.hosts[i],
			"error", err,
		)
	}

	return parallel.Run(len(w.hosts), target.Parallel, f, notify)
}

func (w *schemaWorker) initHosts(ctx context.Context, location Location) error {
	status, err := w.client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get client status")
	}

	remotePath := location.RemotePath("")
	nodes, err := w.client.GetNodesWithLocationAccess(ctx, status, remotePath)
	if err != nil {
		return errors.Wrap(err, "no live nodes with location access")
	}

	w.hosts = make([]string, 0)
	for _, n := range nodes {
		w.hosts = append(w.hosts, n.Addr)
	}

	w.logger.Info(ctx, "Initialized restore hosts", "hosts", w.hosts)
	return nil
}

// getFileNamesMapping creates renaming mapping for the sstables solving problems with sstables file names.
func (w *schemaWorker) getFileNamesMapping(sstables []string, sstableUUIDFormat bool) map[string]string {
	if sstableUUIDFormat {
		// Target naming scheme is UUID-like
		return sstable.RenameToUUIDs(sstables)
	}
	return sstable.RenameToIDs(sstables, &w.generationCnt)
}
