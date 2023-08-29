// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type restorer interface {
	restore(ctx context.Context, run *RestoreRun, target RestoreTarget) error
}

// restoreWorkerTools consists of utils common for both schemaWorker and tablesWorker.
type worker struct {
	clusterID   uuid.UUID
	taskID      uuid.UUID
	runID       uuid.UUID
	snapshotTag string

	config  Config
	logger  log.Logger
	metrics metrics.RestoreM

	client         *scyllaclient.Client
	session        gocqlx.Session
	clusterSession gocqlx.Session

	forEachRestoredManifest func(ctx context.Context, location Location, f func(ManifestInfoWithContent) error) error
	repairSvc               *repair.Service
	location                Location
}

func (w *worker) newUnits(ctx context.Context, target RestoreTarget) ([]RestoreUnit, error) {
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
		return nil, errors.Errorf("no snapshot with tag %s", target.SnapshotTag)
	}

	for _, u := range unitMap {
		units = append(units, u)
	}

	if units == nil {
		return nil, errors.New("no data in backup locations match given keyspace pattern")
	}

	for _, u := range units {
		for i, t := range u.Tables {
			mode, err := w.GetTableTombstoneGC(u.Keyspace, t.Table)
			if err != nil {
				return nil, errors.Wrapf(err, "get tombstone_gc of %s.%s", u.Keyspace, t.Table)
			}
			u.Tables[i].TombstoneGC = mode
		}
	}

	w.logger.Info(ctx, "Created restore units", "units", units)

	return units, nil
}

var (
	regexMV = regexp.MustCompile(`CREATE\s*MATERIALIZED\s*VIEW`)
	regexSI = regexp.MustCompile(`CREATE\s*INDEX`)
)

func (w *worker) newViews(ctx context.Context, units []RestoreUnit) ([]RestoreView, error) {
	restoredTables := strset.New()
	for _, u := range units {
		for _, t := range u.Tables {
			restoredTables.Add(u.Keyspace + "." + t.Table)
		}
	}

	keyspaces, err := w.client.Keyspaces(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "get keyspaces")
	}

	// Create stmt has to contain "IF NOT EXISTS" clause as we have to be able to resume restore from any point
	addIfNotExists := func(stmt string, t ViewType) (string, error) {
		var loc []int
		switch t {
		case MaterializedView:
			loc = regexMV.FindStringIndex(stmt)
		case SecondaryIndex:
			loc = regexSI.FindStringIndex(stmt)
		}
		if loc == nil {
			return "", fmt.Errorf("unknown create view statement %s", stmt)
		}

		return stmt[loc[0]:loc[1]] + " IF NOT EXISTS" + stmt[loc[1]:], nil
	}

	var views []RestoreView
	for _, ks := range keyspaces {
		meta, err := w.clusterSession.KeyspaceMetadata(ks)
		if err != nil {
			return nil, errors.Wrapf(err, "get keyspace %s metadata", ks)
		}

		for _, index := range meta.Indexes {
			if !restoredTables.Has(index.KeyspaceName + "." + index.TableName) {
				continue
			}
			dummyMeta := gocql.KeyspaceMetadata{
				Indexes: map[string]*gocql.IndexMetadata{index.Name: index},
			}

			schema, err := dummyMeta.ToCQL()
			if err != nil {
				return nil, errors.Wrapf(err, "get index %s.%s create statement", ks, index.Name)
			}

			// DummyMeta schema consists of create keyspace and create view statements
			stmt := strings.Split(schema, ";")[1]
			stmt, err = addIfNotExists(stmt, SecondaryIndex)
			if err != nil {
				return nil, err
			}

			views = append(views, RestoreView{
				Keyspace:   index.KeyspaceName,
				View:       index.Name,
				Type:       SecondaryIndex,
				BaseTable:  index.TableName,
				CreateStmt: stmt,
			})
		}

		for _, view := range meta.Views {
			if !restoredTables.Has(view.KeyspaceName + "." + view.BaseTableName) {
				continue
			}
			dummyMeta := gocql.KeyspaceMetadata{
				Views: map[string]*gocql.ViewMetadata{view.ViewName: view},
			}

			schema, err := dummyMeta.ToCQL()
			if err != nil {
				return nil, errors.Wrapf(err, "get view %s.%s create statement", ks, view.ViewName)
			}

			// DummyMeta schema consists of create keyspace and create view statements
			stmt := strings.Split(schema, ";")[1]
			stmt, err = addIfNotExists(stmt, MaterializedView)
			if err != nil {
				return nil, err
			}

			views = append(views, RestoreView{
				Keyspace:   view.KeyspaceName,
				View:       view.ViewName,
				Type:       MaterializedView,
				BaseTable:  view.BaseTableName,
				CreateStmt: stmt,
			})
		}
	}

	return views, nil
}

// cleanUploadDir deletes all SSTables from host's upload directory except for those present in excludedFiles.
func (w *worker) cleanUploadDir(ctx context.Context, host, uploadDir string, excludedFiles []string) error {
	s := strset.New(excludedFiles...)
	var filesToBeDeleted []string

	getFilesToBeDeleted := func(item *scyllaclient.RcloneListDirItem) {
		if !s.Has(item.Name) {
			filesToBeDeleted = append(filesToBeDeleted, item.Name)
		}
	}

	opts := &scyllaclient.RcloneListDirOpts{FilesOnly: true}
	if err := w.client.RcloneListDirIter(ctx, host, uploadDir, opts, getFilesToBeDeleted); err != nil {
		return errors.Wrapf(err, "list dir: %s on host: %s", uploadDir, host)
	}

	if len(filesToBeDeleted) > 0 {
		w.logger.Info(ctx, "Delete files from host's upload directory",
			"host", host,
			"upload_dir", uploadDir,
			"files", filesToBeDeleted,
		)
	}

	for _, f := range filesToBeDeleted {
		remotePath := path.Join(uploadDir, f)
		if err := w.client.RcloneDeleteFile(ctx, host, remotePath); err != nil {
			return errors.Wrapf(err, "delete file: %s on host: %s", remotePath, host)
		}
	}

	return nil
}

func (w *worker) ValidateTableExists(keyspace, table string) error {
	q := qb.Select("system_schema.tables").
		Columns("table_name").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(w.clusterSession).
		Bind(keyspace, table)
	defer q.Release()

	var name string
	return q.Scan(&name)
}

func (w *worker) GetTableVersion(ctx context.Context, keyspace, table string) (string, error) {
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

	w.logger.Info(ctx, "Received table's version",
		"keyspace", keyspace,
		"table", table,
		"version", version,
	)

	return version, nil
}

// Docs: https://docs.scylladb.com/stable/cql/ddl.html#tombstones-gc-options.
type tombstoneGCMode string

const (
	modeDisabled  tombstoneGCMode = "disabled"
	modeTimeout   tombstoneGCMode = "timeout"
	modeRepair    tombstoneGCMode = "repair"
	modeImmediate tombstoneGCMode = "immediate"
)

// AlterTableTombstoneGC alters 'tombstone_gc' mode.
func (w *worker) AlterTableTombstoneGC(ctx context.Context, keyspace, table string, mode tombstoneGCMode) error {
	w.logger.Info(ctx, "Alter table's tombstone_gc mode",
		"keyspace", keyspace,
		"table", table,
		"mode", mode,
	)

	op := func() error {
		return w.clusterSession.ExecStmt(alterTableTombstoneGCStmt(keyspace, table, mode))
	}

	notify := func(err error, wait time.Duration) {
		w.logger.Info(ctx, "Altering table's tombstone_gc mode failed",
			"keyspace", keyspace,
			"table", table,
			"mode", mode,
			"error", err,
			"wait", wait,
		)
	}

	return alterSchemaRetryWrapper(ctx, op, notify)
}

// DropView drops specified Materialized View or Secondary Index.
func (w *worker) DropView(ctx context.Context, view RestoreView) error {
	w.logger.Info(ctx, "Dropping view",
		"keyspace", view.Keyspace,
		"view", view.View,
		"type", view.Type,
	)

	op := func() error {
		dropStmt := ""
		switch view.Type {
		case SecondaryIndex:
			dropStmt = "DROP INDEX IF EXISTS %s.%s"
		case MaterializedView:
			dropStmt = "DROP MATERIALIZED VIEW IF EXISTS %s.%s"
		}

		return w.clusterSession.ExecStmt(fmt.Sprintf(dropStmt, view.Keyspace, view.View))
	}

	notify := func(err error, wait time.Duration) {
		w.logger.Info(ctx, "Dropping view failed",
			"keyspace", view.Keyspace,
			"view", view.View,
			"type", view.Type,
			"error", err,
			"wait", wait,
		)
	}

	return alterSchemaRetryWrapper(ctx, op, notify)
}

// CreateView creates specified Materialized View or Secondary Index.
func (w *worker) CreateView(ctx context.Context, view RestoreView) error {
	w.logger.Info(ctx, "Creating view",
		"keyspace", view.Keyspace,
		"view", view.View,
		"type", view.Type,
		"statement", view.CreateStmt,
	)

	op := func() error {
		return w.clusterSession.ExecStmt(view.CreateStmt)
	}

	notify := func(err error, wait time.Duration) {
		w.logger.Info(ctx, "Creating view failed",
			"keyspace", view.Keyspace,
			"view", view.View,
			"type", view.Type,
			"error", err,
			"wait", wait,
		)
	}

	return alterSchemaRetryWrapper(ctx, op, notify)
}

func (w *worker) WaitForViewBuilding(ctx context.Context, view RestoreView) error {
	labels := metrics.RestoreViewBuildStatusLabels{
		ClusterID: w.clusterID.String(),
		Keyspace:  view.Keyspace,
		View:      view.View,
	}

	op := func() error {
		viewTableName := view.View
		if view.Type == SecondaryIndex {
			viewTableName += "_index"
		}

		status, err := w.client.ViewBuildStatus(ctx, view.Keyspace, viewTableName)
		if err != nil {
			w.metrics.SetViewBuildStatus(labels, metrics.BuildStatusError)
			return retry.Permanent(err)
		}

		switch status {
		case scyllaclient.StatusUnknown:
			w.metrics.SetViewBuildStatus(labels, metrics.BuildStatusUnknown)
			return fmt.Errorf("current status: %s", status)
		case scyllaclient.StatusStarted:
			w.metrics.SetViewBuildStatus(labels, metrics.BuildStatusStarted)
			return fmt.Errorf("current status: %s", status)
		case scyllaclient.StatusSuccess:
			w.metrics.SetViewBuildStatus(labels, metrics.BuildStatusSuccess)
		}

		return nil
	}

	notify := func(err error) {
		w.logger.Info(ctx, "Waiting for view",
			"keyspace", view.Keyspace,
			"view", view.View,
			"type", view.Type,
			"error", err,
		)
	}

	return indefiniteHangingRetryWrapper(ctx, op, notify)
}

// alterSchemaRetryWrapper is useful when executing many statements altering schema,
// as it might take more time for Scylla to process them one after another.
// This wrapper exits on: success, context cancel, op returned non-timeout error or after maxTotalTime has passed.
func alterSchemaRetryWrapper(ctx context.Context, op func() error, notify func(err error, wait time.Duration)) error {
	const (
		minWait      = 5 * time.Second
		maxWait      = 1 * time.Minute
		maxTotalTime = 15 * time.Minute
		multiplier   = 2
		jitter       = 0.2
	)
	backoff := retry.NewExponentialBackoff(minWait, maxTotalTime, maxWait, multiplier, jitter)

	wrappedOp := func() error {
		err := op()
		if err == nil || strings.Contains(err.Error(), "timeout") {
			return err
		}
		// All non-timeout errors shouldn't be retried
		return retry.Permanent(err)
	}

	return retry.WithNotify(ctx, wrappedOp, backoff, notify)
}

// GetTableTombstoneGC returns table's tombstone_gc mode.
func (w *worker) GetTableTombstoneGC(keyspace, table string) (tombstoneGCMode, error) {
	var ext map[string]string
	q := qb.Select("system_schema.tables").
		Columns("extensions").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(w.clusterSession).
		Bind(keyspace, table)

	defer q.Release()
	err := q.Scan(&ext)
	if err != nil {
		return "", err
	}

	// Timeout (just using gc_grace_seconds) is the default mode
	mode, ok := ext["tombstone_gc"]
	if !ok {
		return modeTimeout, nil
	}

	allModes := []tombstoneGCMode{modeDisabled, modeTimeout, modeRepair, modeImmediate}
	for _, m := range allModes {
		if strings.Contains(mode, string(m)) {
			return m, nil
		}
	}
	return "", errors.Errorf("unrecognised tombstone_gc mode: %s", mode)
}

func alterTableTombstoneGCStmt(keyspace, table string, mode tombstoneGCMode) string {
	return fmt.Sprintf(`ALTER TABLE %q.%q WITH tombstone_gc = {'mode': '%s'}`, keyspace, table, mode)
}

func (w *worker) restoreSSTables(ctx context.Context, host, keyspace, table string, loadAndStream, primaryReplicaOnly bool) error {
	w.logger.Info(ctx, "Load SSTables for the first time",
		"host", host,
		"load_and_stream", loadAndStream,
		"primary_replica_only", primaryReplicaOnly,
	)

	op := func() error {
		running, err := w.client.LoadSSTables(ctx, host, keyspace, table, loadAndStream, primaryReplicaOnly)
		if err == nil || running || strings.Contains(err.Error(), "timeout") {
			return err
		}
		// Don't retry if error isn't connected to timeout or already running l&s
		return retry.Permanent(err)
	}

	notify := func(err error) {
		w.logger.Info(ctx, "Waiting for SSTables loading to finish",
			"host", host,
			"error", err,
		)
	}

	return indefiniteHangingRetryWrapper(ctx, op, notify)
}

// indefiniteHangingRetryWrapper is useful when waiting on
// Scylla operation that might take a really long (and difficult to estimate) time.
// This wrapper exits ONLY on: success, context cancel, op returned retry.IsPermanent error.
func indefiniteHangingRetryWrapper(ctx context.Context, op func() error, notify func(err error)) error {
	const repeatInterval = 10 * time.Second

	ticker := time.NewTicker(repeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		err := op()
		switch {
		case err == nil:
			return nil
		case retry.IsPermanent(err):
			return err
		default:
			notify(err)
		}
	}
}

func (w *worker) insertRun(ctx context.Context, run *RestoreRun) {
	if err := table.RestoreRun.InsertQuery(w.session).BindStruct(run).ExecRelease(); err != nil {
		w.logger.Error(ctx, "Insert run",
			"run", *run,
			"error", err,
		)
	}
}

func (w *worker) insertRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if err := table.RestoreRunProgress.InsertQuery(w.session).BindStruct(pr).ExecRelease(); err != nil {
		w.logger.Error(ctx, "Insert run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

func (w *worker) deleteRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if err := table.RestoreRunProgress.DeleteQuery(w.session).BindStruct(pr).ExecRelease(); err != nil {
		w.logger.Error(ctx, "Delete run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

// decorateWithPrevRun gets restore task previous run and if it is not done
// sets prev ID on the given run.
func (w *worker) decorateWithPrevRun(ctx context.Context, run *RestoreRun, cont bool) error {
	prev, err := w.GetRun(ctx, run.ClusterID, run.TaskID, uuid.Nil)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil
		}
		return errors.Wrap(err, "get run")
	}

	if prev.Stage == StageRestoreDone {
		return nil
	}

	// Always copy Units as they contain information about original tombstone_gc mode
	run.Units = prev.Units
	if cont {
		run.PrevID = prev.ID
		run.Location = prev.Location
		run.ManifestPath = prev.ManifestPath
		run.Keyspace = prev.Keyspace
		run.Table = prev.Table
		run.Stage = prev.Stage
		run.RepairTaskID = prev.RepairTaskID
	}

	w.logger.Info(ctx, "Decorated run", "run", *run)
	return nil
}

// clonePrevProgress copies all the previous run progress into
// current run progress.
func (w *worker) clonePrevProgress(ctx context.Context, run *RestoreRun) {
	q := table.RestoreRunProgress.InsertQuery(w.session)
	defer q.Release()

	prevRun := &RestoreRun{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        run.PrevID,
	}

	w.ForEachProgress(ctx, prevRun, func(pr *RestoreRunProgress) {
		pr.RunID = run.ID

		if err := q.BindStruct(pr).Exec(); err != nil {
			w.logger.Error(ctx, "Couldn't clone run progress",
				"run_progress", *pr,
				"error", err,
			)
		}
	})

	w.logger.Info(ctx, "Run after decoration", "run", *run)
}

// GetRun returns run with specified cluster, task and run ID.
// If run ID is not specified, it returns the latest run with specified cluster and task ID.
func (w *worker) GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*RestoreRun, error) {
	w.logger.Debug(ctx, "Get run",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	var q *gocqlx.Queryx
	if runID != uuid.Nil {
		q = table.RestoreRun.GetQuery(w.session).BindMap(qb.M{
			"cluster_id": clusterID,
			"task_id":    taskID,
			"id":         runID,
		})
	} else {
		q = table.RestoreRun.SelectQuery(w.session).BindMap(qb.M{
			"cluster_id": clusterID,
			"task_id":    taskID,
		})
	}

	var r RestoreRun
	return &r, q.GetRelease(&r)
}

// getProgress fetches restore worker's run and returns its aggregated progress information.
func (w *worker) getProgress(ctx context.Context) (RestoreProgress, error) {
	w.logger.Debug(ctx, "Getting progress",
		"cluster_id", w.clusterID,
		"task_id", w.taskID,
		"run_id", w.runID,
	)

	run, err := w.GetRun(ctx, w.clusterID, w.taskID, w.runID)
	if err != nil {
		return RestoreProgress{}, errors.Wrap(err, "get restore run")
	}

	pr := w.aggregateProgress(ctx, run)

	// Check if repair progress needs to be filled
	if run.RepairTaskID == uuid.Nil {
		return pr, nil
	}

	q := table.RepairRun.SelectQuery(w.session).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.RepairTaskID,
	})

	var repairRun repair.Run
	if err = q.GetRelease(&repairRun); err != nil {
		return pr, errors.Wrap(err, "get repair run")
	}

	repairPr, err := w.repairSvc.GetProgress(ctx, repairRun.ClusterID, repairRun.TaskID, repairRun.ID)
	if err != nil {
		return pr, errors.Wrap(err, "get repair progress")
	}

	pr.RepairProgress = &repairPr
	return pr, nil
}

func (w *worker) AwaitSchemaAgreement(ctx context.Context, clusterSession gocqlx.Session) {
	w.logger.Info(ctx, "Awaiting schema agreement...")

	var stepError error
	defer func(start time.Time) {
		if stepError != nil {
			w.logger.Error(ctx, "Awaiting schema agreement failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.logger.Info(ctx, "Done awaiting schema agreement", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	const (
		waitMin        = 15 * time.Second // nolint: revive
		waitMax        = 1 * time.Minute
		maxElapsedTime = 15 * time.Minute
		multiplier     = 2
		jitter         = 0.2
	)

	backoff := retry.NewExponentialBackoff(
		waitMin,
		maxElapsedTime,
		waitMax,
		multiplier,
		jitter,
	)

	notify := func(err error, wait time.Duration) {
		w.logger.Info(ctx, "Schema agreement not reached, retrying...", "error", err, "wait", wait)
	}

	const (
		peerSchemasStmt = "SELECT schema_version FROM system.peers"
		localSchemaStmt = "SELECT schema_version FROM system.local WHERE key='local'"
	)

	stepError = retry.WithNotify(ctx, func() error {
		var v []string
		if err := clusterSession.Query(peerSchemasStmt, nil).SelectRelease(&v); err != nil {
			return retry.Permanent(err)
		}
		var lv string
		if err := clusterSession.Query(localSchemaStmt, nil).GetRelease(&lv); err != nil {
			return retry.Permanent(err)
		}

		// Join all versions
		m := strset.New(v...)
		m.Add(lv)
		if m.Size() > 1 {
			return errors.Errorf("cluster schema versions not consistent: %s", m.List())
		}

		return nil
	}, backoff, notify)
}

func (w *worker) checkAvailableDiskSpace(ctx context.Context, host string) error {
	freePercent, err := w.diskFreePercent(ctx, host)
	if err != nil {
		return err
	}
	w.logger.Info(ctx, "Available disk space", "host", host, "percent", freePercent)
	if freePercent < w.config.DiskSpaceFreeMinPercent {
		return errors.New("not enough disk space")
	}
	return nil
}

func (w *worker) diskFreePercent(ctx context.Context, host string) (int, error) {
	du, err := w.client.RcloneDiskUsage(ctx, host, DataDir)
	if err != nil {
		return 0, err
	}
	return int(100 * (float64(du.Free) / float64(du.Total))), nil
}

func (w *worker) clearJobStats(ctx context.Context, jobID int64, host string) error {
	w.logger.Debug(ctx, "Clearing job stats", "host", host, "job_id", jobID)
	return errors.Wrap(w.client.RcloneDeleteJobStats(ctx, host, jobID), "clear job stats")
}

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
