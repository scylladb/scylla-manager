// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// restoreWorker represents common functionalities of both schemaWorker and tablesWorker
// that are needed to perform restore procedure.
type restoreWorker interface {
	restore(ctx context.Context, run *RestoreRun, target RestoreTarget) error
	newUnits(ctx context.Context, target RestoreTarget) ([]RestoreUnit, error)
	continuePrevRun()
	insertRun(ctx context.Context, run *RestoreRun)
	decorateWithPrevRun(ctx context.Context, run *RestoreRun) error
	clonePrevProgress(ctx context.Context, run *RestoreRun)
}

// restoreWorkerTools consists of utils common for both schemaWorker and tablesWorker.
type restoreWorkerTools struct {
	workerTools

	metrics        metrics.RestoreM
	managerSession gocqlx.Session
	clusterSession gocqlx.Session
	// Iterates over all manifests in given location with cluster ID and snapshot tag specified in restore target.
	forEachRestoredManifest func(ctx context.Context, location Location, f func(ManifestInfoWithContent) error) error

	location Location // Currently restored location
}

func (w *restoreWorkerTools) newUnits(ctx context.Context, target RestoreTarget) ([]RestoreUnit, error) {
	var (
		units   []RestoreUnit
		unitMap = make(map[string]RestoreUnit)
	)

	var foundManifest bool
	for _, l := range target.Location {
		manifestHandler := func(miwc ManifestInfoWithContent) error {
			foundManifest = true

			filesHandler := func(fm FilesMeta) {
				ru := unitMap[fm.Keyspace]
				ru.Keyspace = fm.Keyspace
				ru.Size += fm.Size

				for i, t := range ru.Tables {
					if t.Table == fm.Table {
						ru.Tables[i].Size += fm.Size
						unitMap[fm.Keyspace] = ru

						return
					}
				}

				ru.Tables = append(ru.Tables, RestoreTable{
					Table: fm.Table,
					Size:  fm.Size,
				})
				unitMap[fm.Keyspace] = ru
			}

			return miwc.ForEachIndexIter(target.Keyspace, filesHandler)
		}

		if err := w.forEachRestoredManifest(ctx, l, manifestHandler); err != nil {
			return nil, err
		}
	}

	if !foundManifest {
		return nil, errors.Errorf("no snapshot with given tag: %s", target.SnapshotTag)
	}

	for _, u := range unitMap {
		units = append(units, u)
	}

	if units == nil {
		return nil, errors.New("no data in backup locations match given keyspace pattern")
	}

	for _, u := range units {
		for _, t := range u.Tables {
			if err := w.ValidateTableExists(ctx, u.Keyspace, t.Table); err != nil {
				return nil, err
			}
		}
	}

	w.Logger.Info(ctx, "Created restore units", "units", units)

	return units, nil
}

// cleanUploadDir deletes all SSTables from host's upload directory except for those present in excludedFiles.
func (w *restoreWorkerTools) cleanUploadDir(ctx context.Context, host, uploadDir string, excludedFiles []string) error {
	s := strset.New(excludedFiles...)
	var filesToBeDeleted []string

	getFilesToBeDeleted := func(item *scyllaclient.RcloneListDirItem) {
		if !s.Has(item.Name) {
			filesToBeDeleted = append(filesToBeDeleted, item.Name)
		}
	}

	opts := &scyllaclient.RcloneListDirOpts{FilesOnly: true}
	if err := w.Client.RcloneListDirIter(ctx, host, uploadDir, opts, getFilesToBeDeleted); err != nil {
		return errors.Wrapf(err, "list dir: %s on host: %s", uploadDir, host)
	}

	if len(filesToBeDeleted) > 0 {
		w.Logger.Info(ctx, "Delete files from host's upload directory",
			"host", host,
			"upload_dir", uploadDir,
			"files", filesToBeDeleted,
		)
	}

	for _, f := range filesToBeDeleted {
		remotePath := path.Join(uploadDir, f)
		if err := w.Client.RcloneDeleteFile(ctx, host, remotePath); err != nil {
			return errors.Wrapf(err, "delete file: %s on host: %s", remotePath, host)
		}
	}

	return nil
}

func (w *restoreWorkerTools) ValidateTableExists(ctx context.Context, keyspace, table string) error {
	q := qb.Select("system_schema.tables").
		Columns("table_name").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(w.clusterSession).
		Bind(keyspace, table)
	defer q.Release()

	var name string
	if err := q.Scan(&name); err != nil {
		return errors.Wrap(err, "validate table exists")
	}

	return nil
}

func (w *restoreWorkerTools) GetTableVersion(ctx context.Context, keyspace, table string) (string, error) {
	q := qb.Select("system_schema.tables").
		Columns("id").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(w.clusterSession).
		Bind(keyspace, table)

	defer q.Release()

	var version string
	if err := q.Scan(&version); err != nil {
		return "", errors.Wrap(err, "record table's version")
	}
	// Table's version is stripped of '-' characters
	version = strings.ReplaceAll(version, "-", "")

	w.Logger.Info(ctx, "Received table's version",
		"keyspace", keyspace,
		"table", table,
		"version", version,
	)

	return version, nil
}

// DisableTableGGS disables 'tombstone_gc' option for the time of restoring tables' contents.
// It should be enabled by the user after repairing restored cluster.
func (w *restoreWorkerTools) DisableTableGGS(ctx context.Context, keyspace, table string) error {
	w.Logger.Info(ctx, "Disabling table's gc_grace_seconds",
		"keyspace", keyspace,
		"table", table,
	)

	if err := w.clusterSession.ExecStmt(disableTableGGSStatement(keyspace, table)); err != nil {
		return errors.Wrap(err, "disable gc_grace_seconds")
	}
	return nil
}

func disableTableGGSStatement(keyspace, table string) string {
	return fmt.Sprintf(`ALTER TABLE "%s"."%s" WITH tombstone_gc = {'mode':'disabled'}`, keyspace, table)
}

func (w *restoreWorkerTools) insertRun(ctx context.Context, run *RestoreRun) {
	if err := table.RestoreRun.InsertQuery(w.managerSession).BindStruct(run).ExecRelease(); err != nil {
		w.Logger.Error(ctx, "Insert run",
			"run", *run,
			"error", err,
		)
	}
}

func (w *restoreWorkerTools) insertRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if err := table.RestoreRunProgress.InsertQuery(w.managerSession).BindStruct(pr).ExecRelease(); err != nil {
		w.Logger.Error(ctx, "Insert run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

func (w *restoreWorkerTools) deleteRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if err := table.RestoreRunProgress.DeleteQuery(w.managerSession).BindStruct(pr).ExecRelease(); err != nil {
		w.Logger.Error(ctx, "Delete run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

// decorateWithPrevRun gets restore task previous run and if it is not done
// sets prev ID on the given run.
func (w *restoreWorkerTools) decorateWithPrevRun(ctx context.Context, run *RestoreRun) error {
	prev, err := w.GetRun(ctx, run.ClusterID, run.TaskID, uuid.Nil)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "get run")
	}
	if prev.Stage == StageRestoreDone {
		return nil
	}

	w.Logger.Info(ctx, "Resuming previous run", "prev_run_id", prev.ID)

	run.PrevID = prev.ID
	run.Location = prev.Location
	run.ManifestPath = prev.ManifestPath
	run.Keyspace = prev.Keyspace
	run.Table = prev.Table
	run.Stage = prev.Stage
	run.Units = prev.Units

	return nil
}

// clonePrevProgress copies all the previous run progress into
// current run progress.
func (w *restoreWorkerTools) clonePrevProgress(ctx context.Context, run *RestoreRun) {
	q := table.RestoreRunProgress.InsertQuery(w.managerSession)
	defer q.Release()

	prevRun := &RestoreRun{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        run.PrevID,
	}

	w.ForEachProgress(ctx, prevRun, func(pr *RestoreRunProgress) {
		pr.RunID = run.ID

		if err := q.BindStruct(pr).Exec(); err != nil {
			w.Logger.Error(ctx, "Couldn't clone run progress",
				"run_progress", *pr,
				"error", err,
			)
		}
	})

	w.Logger.Info(ctx, "Run after decoration", "run", *run)
}

// GetRun returns run with specified cluster, task and run ID.
// If run ID is not specified, it returns the latest run with specified cluster and task ID.
func (w *restoreWorkerTools) GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*RestoreRun, error) {
	w.Logger.Debug(ctx, "Get run",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	var q *gocqlx.Queryx
	if runID != uuid.Nil {
		q = table.RestoreRun.GetQuery(w.managerSession).BindMap(qb.M{
			"cluster_id": clusterID,
			"task_id":    taskID,
			"id":         runID,
		})
	} else {
		q = table.RestoreRun.SelectQuery(w.managerSession).BindMap(qb.M{
			"cluster_id": clusterID,
			"task_id":    taskID,
		})
	}

	var r RestoreRun
	return &r, q.GetRelease(&r)
}

// getProgress fetches restore worker's run and returns its aggregated progress information.
func (w *restoreWorkerTools) getProgress(ctx context.Context) (RestoreProgress, error) {
	w.Logger.Debug(ctx, "Getting progress",
		"cluster_id", w.ClusterID,
		"task_id", w.TaskID,
		"run_id", w.RunID,
	)

	run, err := w.GetRun(ctx, w.ClusterID, w.TaskID, w.RunID)
	if err != nil {
		return RestoreProgress{}, err
	}

	return w.aggregateProgress(ctx, run), nil
}

// sstableID returns ID from SSTable name.
// Supported SSTable format versions are: "mc", "md", "me", "la", "ka".
// Scylla code validating SSTable format can be found here:
// https://github.com/scylladb/scylladb/blob/2c1ef0d2b768a793c284fc68944526179bfd0171/sstables/sstables.cc#L2333
func sstableID(sstable string) string {
	parts := strings.Split(sstable, "-")

	if regexLaMx.MatchString(sstable) {
		return parts[1]
	}
	if regexKa.MatchString(sstable) {
		return parts[3]
	}

	panic(unknownSSTableError(sstable))
}

func renameSSTableID(sstable, newID string) string {
	parts := strings.Split(sstable, "-")

	switch {
	case regexLaMx.MatchString(sstable):
		parts[1] = newID
	case regexKa.MatchString(sstable):
		parts[3] = newID
	default:
		panic(unknownSSTableError(sstable))
	}

	return strings.Join(parts, "-")
}

func unknownSSTableError(sstable string) error {
	return errors.Errorf("unknown SSTable format version: %s. Supported versions are: 'mc', 'md', 'me', 'la', 'ka'", sstable)
}

var (
	regexLaMx = regexp.MustCompile(`(la|m[cde])-(\d+)-(\w+)-(.*)`)
	regexKa   = regexp.MustCompile(`(\w+)-(\w+)-ka-(\d+)-(.*)`)
)

func buildFilesSizesCache(ctx context.Context, client *scyllaclient.Client, host, dir string, versioned VersionedMap) (map[string]int64, error) {
	filesSizesCache := make(map[string]int64)
	opts := &scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
	}
	f := func(item *scyllaclient.RcloneListDirItem) {
		filesSizesCache[item.Name] = item.Size
	}
	if err := client.RcloneListDirIter(ctx, host, dir, opts, f); err != nil {
		return nil, errors.Wrapf(err, "host %s: listing all files from %s", host, dir)
	}
	for k, v := range versioned {
		filesSizesCache[k] = v.Size
	}
	return filesSizesCache, nil
}
