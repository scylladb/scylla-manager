// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/table"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"

	"go.uber.org/atomic"

	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

type schemaWorker struct {
	worker

	generationCnt atomic.Int64
	locationInfo  LocationInfo                       // Currently restored Location
	miwc          backupspec.ManifestInfoWithContent // Currently restored manifest
	// Maps original SSTable name to its existing older version (with respect to currently restored snapshot tag)
	// that should be used during the restore procedure. It should be initialized per each restored table.
	versionedFiles backup.VersionedMap
}

// Due to Scylla issue #16349 it is expected that restore schema tests will fail
// when running with the following setup:
// - scylla-version: scylla:5.2.X,               raft-enabled: true
// - scylla-version: scylla-enterprise:2024.1.X, raft-enabled: true

// restore downloads all backed-up schema files to each node in the cluster. This approach is necessary because
// it's not possible to alter gc_grace_seconds or tombstone_gc on schema tables (safety requirement for nodetool refresh).
// It introduces great data duplication, but is necessary in order to simulate schema repair on each node.
// Luckily, schema files are small, so this shouldn't be noticeable in terms of performance.
// When all files are downloaded, they are restored using nodetool refresh.
// Note that due to small schema size:
// - resuming schema restoration will always start from scratch
// - schema restoration does not use long polling for updating download progress
// Adding the ability to resume schema restoration might be added in the future.
func (w *schemaWorker) restore(ctx context.Context) error {
	return errors.Wrap(w.stageRestoreData(ctx), "restore data")
}

func (w *schemaWorker) stageRestoreData(ctx context.Context) error {
	w.run.Stage = StageData
	w.insertRun(ctx)

	w.AwaitSchemaAgreement(ctx, w.clusterSession)
	w.logger.Info(ctx, "Started restoring schema")
	defer w.logger.Info(ctx, "Restoring schema finished")

	if w.cqlSchema != nil {
		return w.restoreFromSchemaFile(ctx)
	}

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
			uploadDir := backupspec.UploadTableDir(u.Keyspace, t.Table, version)

			for _, h := range status {
				if err := w.cleanUploadDir(ctx, h.Addr, uploadDir, nil); err != nil {
					return err
				}
			}
		}
	}
	// Download files
	for _, l := range w.target.locationInfo {
		if err := w.locationDownloadHandler(ctx, l); err != nil {
			return err
		}
	}
	// Set restore start in all run progresses
	seq := newRunProgressSeq()
	for pr := range seq.All(w.run.ClusterID, w.run.TaskID, w.run.ID, w.session) {
		pr.setRestoreStartedAt()
		w.insertRunProgress(ctx, pr)
	}
	if seq.err != nil {
		w.logger.Error(ctx, "Couldn't set restore start", "error", seq.err)
	}

	// Load schema SSTables on all nodes
	f := func(i int) error {
		host := status[i]
		for _, ks := range w.run.Units {
			for _, t := range ks.Tables {
				if err := w.client.AwaitLoadSSTables(ctx, host.Addr, ks.Keyspace, t.Table, false, false, false, false); err != nil {
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
	for pr := range seq.All(w.run.ClusterID, w.run.TaskID, w.run.ID, w.session) {
		pr.setRestoreCompletedAt()
		pr.Restored = pr.Downloaded + pr.VersionedDownloaded
		w.insertRunProgress(ctx, pr)
	}
	if seq.err != nil {
		w.logger.Error(ctx, "Couldn't set restore end", "error", seq.err)
	}

	return nil
}

func (w *schemaWorker) restoreFromSchemaFile(ctx context.Context) error {
	w.logger.Info(ctx, "Apply schema CQL statements")
	start := timeutc.Now()

	// Alternator schema is independent of CQL schema, as each alternator table
	// lives in its own personal keyspace. On the other hand, CQL schema contains
	// alternator schema translated to CQL statements - those should be skipped.
	// Moreover, CQL schema contains auth and service levels, which might rely on
	// already existing alternator schema, so we need to restore alternator schema first.
	aw, err := newAlternatorSchemaWorker(w.alternatorClient, w.alternatorSchema)
	if err != nil {
		return errors.Wrap(err, "create alternator schema worker")
	}
	if err := aw.restore(ctx); err != nil {
		return errors.Wrap(err, "restore alternator schema")
	}

	var createdKs []string
	for _, row := range *w.cqlSchema {
		if row.Keyspace == "" {
			// Scylla 6.3 added roles and service levels to the output of
			// DESC SCHEMA WITH INTERNALS (https://github.com/scylladb/scylladb/pull/20168).
			// Those entities do not live in any particular keyspace, so that's how we identify them.
			// We are skipping them until we properly support their restoration.
			continue
		}
		if row.Keyspace == "system_replicated_keys" {
			// See https://github.com/scylladb/scylla-enterprise/issues/4168
			continue
		}
		sanitizedName := strings.TrimPrefix(strings.TrimSuffix(row.Name, "\""), "\"")
		if row.Type == "table" && strings.HasSuffix(sanitizedName, table.LWTStateTableSuffix) {
			// No support for LWT state tables restoration (#4732)
			continue
		}
		if aw.isAlternatorSchemaRow(row) {
			continue
		}
		// Sometimes a single object might require multiple CQL statements (e.g. table with dropped and added column)
		for _, stmt := range parseCQLStatement(row.CQLStmt) {
			if err := w.clusterSession.ExecStmt(stmt); err != nil {
				if dropErr := dropKeyspaces(createdKs, w.clusterSession); dropErr != nil {
					w.logger.Error(ctx, "Couldn't rollback already applied schema changes", "error", dropErr)
				}
				return errors.Wrapf(err, "create %s (%s) with %s", row.Name, row.Keyspace, stmt)
			}
			if row.Type == "keyspace" {
				createdKs = append(createdKs, row.Name)
			}
		}
	}
	end := timeutc.Now()

	// Insert dummy run for some progress display
	for _, u := range w.run.Units {
		for _, t := range u.Tables {
			pr := RunProgress{
				ClusterID:           w.run.ClusterID,
				TaskID:              w.run.TaskID,
				RunID:               w.run.ID,
				Keyspace:            u.Keyspace,
				Table:               t.Table,
				DownloadStartedAt:   &start,
				DownloadCompletedAt: &start,
				RestoreStartedAt:    &start,
				RestoreCompletedAt:  &end,
				Downloaded:          t.Size,
				Restored:            t.Size,
			}
			w.insertRunProgress(ctx, &pr)
		}
	}

	w.logger.Info(ctx, "Restored schema from schema file")
	return nil
}

func (w *schemaWorker) locationDownloadHandler(ctx context.Context, location LocationInfo) error {
	w.logger.Info(ctx, "Downloading schema from location", "location", location.Location)
	defer w.logger.Info(ctx, "Downloading schema from location finished", "location", location.Location)

	w.locationInfo = location

	tableDownloadHandler := func(fm backupspec.FilesMeta) error {
		if !unitsContainTable(w.run.Units, fm.Keyspace, fm.Table) {
			return nil
		}

		w.logger.Info(ctx, "Downloading schema table", "keyspace", fm.Keyspace, "table", fm.Table)
		defer w.logger.Info(ctx, "Downloading schema table finished", "keyspace", fm.Keyspace, "table", fm.Table)

		return w.workFunc(ctx, fm)
	}

	manifestDownloadHandler := func(miwc backupspec.ManifestInfoWithContent) error {
		w.logger.Info(ctx, "Downloading schema from manifest", "manifest", miwc.ManifestInfo)
		defer w.logger.Info(ctx, "Downloading schema from manifest finished", "manifest", miwc.ManifestInfo)

		w.miwc = miwc
		w.insertRun(ctx)

		return miwc.ForEachIndexIterWithError(nil, tableDownloadHandler)
	}

	return w.forEachManifest(ctx, location, manifestDownloadHandler)
}

func (w *schemaWorker) workFunc(ctx context.Context, fm backupspec.FilesMeta) error {
	version, err := query.GetTableVersion(w.clusterSession, fm.Keyspace, fm.Table)
	if err != nil {
		return err
	}

	var (
		srcDir = w.miwc.LocationSSTableVersionDir(fm.Keyspace, fm.Table, fm.Version)
		dstDir = backupspec.UploadTableDir(fm.Keyspace, fm.Table, version)
	)

	w.logger.Info(ctx, "Start downloading schema files",
		"keyspace", fm.Keyspace,
		"table", fm.Table,
		"src_dir", srcDir,
		"dst_dir", dstDir,
		"files", fm.Files,
	)

	hosts, ok := w.locationInfo.DCHosts[w.miwc.DC]
	if !ok {
		return errors.Errorf("hosts for the DC %s are not found", w.miwc.DC)
	}
	w.versionedFiles, err = backup.ListVersionedFiles(ctx, w.client, w.run.SnapshotTag, hosts[0], srcDir)
	if err != nil {
		return errors.Wrap(err, "initialize versioned SSTables")
	}

	idMapping := w.getFileNamesMapping(fm.Files, false)
	uuidMapping := w.getFileNamesMapping(fm.Files, true)

	f := func(i int) error {
		host := hosts[i]
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
			srcFile := w.versionedFiles[file].FullName()

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
		proportionalSize := int64((int(fm.Size) + i) / len(hosts))

		w.insertRunProgress(ctx, &RunProgress{
			ClusterID:           w.run.ClusterID,
			TaskID:              w.run.TaskID,
			RunID:               w.run.ID,
			RemoteSSTableDir:    srcDir,
			Keyspace:            fm.Keyspace,
			Table:               fm.Table,
			Host:                host,
			DownloadStartedAt:   &start,
			DownloadCompletedAt: &end,
			Downloaded:          proportionalSize,
		})

		return nil
	}

	notify := func(i int, err error) {
		w.logger.Error(ctx, "Failed to restore schema on host",
			"host", hosts[i],
			"error", err,
		)
	}

	return parallel.Run(len(hosts), w.target.Parallel, f, notify)
}

// getFileNamesMapping creates renaming mapping for the sstables solving problems with sstables file names.
func (w *schemaWorker) getFileNamesMapping(sstables []string, sstableUUIDFormat bool) map[string]string {
	if sstableUUIDFormat {
		// Target naming scheme is UUID-like
		return sstable.RenameToUUIDs(sstables)
	}
	return sstable.RenameToIDs(sstables, &w.generationCnt)
}

// parseCQLStatement splits composite CQL statement into a slice of single CQL statements.
func parseCQLStatement(cql string) []string {
	var out []string
	for stmt := range strings.SplitSeq(cql, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			out = append(out, stmt)
		}
	}
	return out
}

func dropKeyspaces(keyspaces []string, session gocqlx.Session) error {
	const dropKsStmt = "DROP KEYSPACE IF EXISTS %q"
	for _, ks := range keyspaces {
		if err := session.ExecStmt(fmt.Sprintf(dropKsStmt, ks)); err != nil {
			return errors.Wrapf(err, "drop keyspace %q", ks)
		}
	}
	return nil
}
