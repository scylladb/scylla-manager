// Copyright (C) 2023 ScyllaDB

package restore

import (
	"compress/gzip"
	"context"
	"encoding/json"
	stdErr "errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"go.uber.org/atomic"

	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

type schemaWorker struct {
	worker

	generationCnt atomic.Int64
	miwc          ManifestInfoWithContent // Currently restored manifest
	// Maps original SSTable name to its existing older version (with respect to currently restored snapshot tag)
	// that should be used during the restore procedure. It should be initialized per each restored table.
	versionedFiles VersionedMap
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

	if w.describedSchema != nil {
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
			uploadDir := UploadTableDir(u.Keyspace, t.Table, version)

			for _, h := range status {
				if err := w.cleanUploadDir(ctx, h.Addr, uploadDir, nil); err != nil {
					return err
				}
			}
		}
	}
	// Download files
	for _, l := range w.target.Location {
		if err := w.locationDownloadHandler(ctx, l); err != nil {
			return err
		}
	}
	// Set restore start in all run progresses
	err = forEachProgress(w.session, w.run.ClusterID, w.run.TaskID, w.run.ID, func(pr *RunProgress) {
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
	err = forEachProgress(w.session, w.run.ClusterID, w.run.TaskID, w.run.ID, func(pr *RunProgress) {
		pr.setRestoreCompletedAt()
		w.insertRunProgress(ctx, pr)
	})
	if err != nil {
		w.logger.Error(ctx, "Couldn't set restore end", "error", err)
	}

	return nil
}

func (w *schemaWorker) restoreFromSchemaFile(ctx context.Context) error {
	w.logger.Info(ctx, "Apply schema CQL statements")
	start := timeutc.Now()

	var createdKs []string
	for _, row := range *w.describedSchema {
		if row.Keyspace == "system_replicated_keys" {
			// See https://github.com/scylladb/scylla-enterprise/issues/4168
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
			}
			w.insertRunProgress(ctx, &pr)
		}
	}

	w.logger.Info(ctx, "Restored schema from schema file")
	return nil
}

func (w *schemaWorker) locationDownloadHandler(ctx context.Context, location Location) error {
	w.logger.Info(ctx, "Downloading schema from location", "location", location)
	defer w.logger.Info(ctx, "Downloading schema from location finished", "location", location)

	tableDownloadHandler := func(fm FilesMeta) error {
		if !unitsContainTable(w.run.Units, fm.Keyspace, fm.Table) {
			return nil
		}

		w.logger.Info(ctx, "Downloading schema table", "keyspace", fm.Keyspace, "table", fm.Table)
		defer w.logger.Info(ctx, "Downloading schema table finished", "keyspace", fm.Keyspace, "table", fm.Table)

		return w.workFunc(ctx, fm)
	}

	manifestDownloadHandler := func(miwc ManifestInfoWithContent) error {
		w.logger.Info(ctx, "Downloading schema from manifest", "manifest", miwc.ManifestInfo)
		defer w.logger.Info(ctx, "Downloading schema from manifest finished", "manifest", miwc.ManifestInfo)

		w.miwc = miwc
		w.insertRun(ctx)

		return miwc.ForEachIndexIterWithError(nil, tableDownloadHandler)
	}

	return w.forEachManifest(ctx, location, manifestDownloadHandler)
}

func (w *schemaWorker) workFunc(ctx context.Context, fm FilesMeta) error {
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

	hosts := w.target.locationHosts[w.miwc.Location]
	w.versionedFiles, err = ListVersionedFiles(ctx, w.client, w.run.SnapshotTag, hosts[0], srcDir)
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

func getDescribedSchema(ctx context.Context, client *scyllaclient.Client, snapshotTag string, locHost map[Location][]string) (schema *query.DescribedSchema, err error) {
	baseDir := path.Join("backup", string(SchemaDirKind))
	// It's enough to get a single schema file, but it's important to validate
	// that each location contains exactly one or none of them.
	var (
		host       string
		schemaPath *string
		foundCnt   int
	)
	for l, hosts := range locHost {
		host = hosts[0]
		schemaPath, err = getRemoteSchemaFilePath(ctx, client, snapshotTag, host, l.RemotePath(baseDir))
		if err != nil {
			return nil, errors.Wrapf(err, "get schema file from %s", l.RemotePath(baseDir))
		}
		if schemaPath != nil {
			foundCnt++
		}
	}

	if foundCnt == 0 {
		return nil, nil // nolint: nilnil
	} else if foundCnt < len(locHost) {
		return nil, errors.New("only a subset of provided locations has schema files")
	}

	r, err := client.RcloneOpen(ctx, host, *schemaPath)
	if err != nil {
		return nil, errors.Wrap(err, "open schema file")
	}
	defer func() {
		err = stdErr.Join(err, errors.Wrap(r.Close(), "close schema file reader"))
	}()

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return nil, errors.Wrap(err, "create gzip reader")
	}
	defer func() {
		err = stdErr.Join(err, errors.Wrap(gzr.Close(), "close gzip reader"))
	}()

	rawSchema, err := io.ReadAll(gzr)
	if err != nil {
		return nil, errors.Wrap(err, "decompress schema")
	}

	schema = new(query.DescribedSchema)
	if err := json.Unmarshal(rawSchema, schema); err != nil {
		return nil, errors.Wrap(err, "unmarshal schema")
	}

	return schema, nil
}

// getRemoteSchemaFilePath returns path to the schema file with given snapshotTag.
// Both search and returned path are relative to the remotePath.
// In case schema file wasn't found, nil is returned.
func getRemoteSchemaFilePath(ctx context.Context, client *scyllaclient.Client, snapshotTag, host, remotePath string) (*string, error) {
	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		Recurse:   true,
	}

	var schemaPaths []string
	err := client.RcloneListDirIter(ctx, host, remotePath, &opts, func(f *scyllaclient.RcloneListDirItem) {
		if strings.HasSuffix(f.Name, RemoteSchemaFileSuffix(snapshotTag)) {
			schemaPaths = append(schemaPaths, f.Path)
		}
	})
	if err != nil {
		return nil, errors.Wrapf(err, "iterate over schema dir")
	}

	if len(schemaPaths) == 0 {
		return nil, nil // nolint: nilnil
	}
	if len(schemaPaths) > 1 {
		return nil, errors.Errorf("many schema files with %s snapshot tag: %v", snapshotTag, schemaPaths)
	}
	schemaPath := path.Join(remotePath, schemaPaths[0])
	return &schemaPath, nil
}

// parseCQLStatement splits composite CQL statement into a slice of single CQL statements.
func parseCQLStatement(cql string) []string {
	var out []string
	for _, stmt := range strings.Split(cql, ";") {
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
