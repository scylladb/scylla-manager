// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"sort"
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
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/slice"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/multierr"
)

// restoreWorkerTools consists of utils common for both schemaWorker and tablesWorker.
type worker struct {
	run    *Run
	target Target

	config  Config
	logger  log.Logger
	metrics metrics.RestoreM

	client         *scyllaclient.Client
	session        gocqlx.Session
	clusterSession gocqlx.Session
}

func (w *worker) init(ctx context.Context, properties json.RawMessage) error {
	if err := w.initTarget(ctx, properties); err != nil {
		return errors.Wrap(err, "init target")
	}
	if err := w.initUnits(ctx); err != nil {
		return errors.Wrap(err, "init units")
	}
	return errors.Wrap(w.initViews(ctx), "init views")
}

func (w *worker) initTarget(ctx context.Context, properties json.RawMessage) error {
	t := defaultTarget()
	if err := json.Unmarshal(properties, &t); err != nil {
		return err
	}
	if err := t.validateProperties(); err != nil {
		return err
	}

	if t.Keyspace == nil {
		t.Keyspace = []string{"*"}
	}
	if t.RestoreSchema {
		t.Keyspace = []string{"system_schema"}
	}
	if t.RestoreTables {
		// Skip restoration of those tables regardless of the '--keyspace' param
		doNotRestore := []string{
			"system",        // system.* tables are recreated on every cluster and shouldn't even be backed-up
			"system_schema", // Schema restoration is only possible with '--restore-schema' flag
			// Don't restore tables related to CDC.
			// Currently, it is forbidden to alter those tables, so SM wouldn't be able to ensure their data consistency.
			// Moreover, those tables usually contain data with small TTL value,
			// so their contents would probably expire right after restore has ended.
			"system_distributed_everywhere.cdc_generation_descriptions_v2",
			"system_distributed.cdc_streams_descriptions_v2",
			"system_distributed.cdc_generation_timestamps",
			"*.*_scylla_cdc_log", // All regular CDC tables have "_scylla_cdc_log" suffix
		}

		for _, ks := range doNotRestore {
			t.Keyspace = append(t.Keyspace, "!"+ks)
		}

		// Filter out all materialized views and secondary indexes. They are not a part of restore procedure at the moment.
		// See https://docs.scylladb.com/stable/operating-scylla/procedures/backup-restore/restore.html#repeat-the-following-steps-for-each-node-in-the-cluster.
		views, err := query.GetAllViews(w.clusterSession)
		if err != nil {
			return errors.Wrap(err, "get cluster views")
		}

		for _, viewName := range views.List() {
			t.Keyspace = append(t.Keyspace, "!"+viewName)
		}
	}

	status, err := w.client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get status")
	}

	// All nodes should be up during restore
	if err := w.client.VerifyNodesAvailability(ctx); err != nil {
		return errors.Wrap(err, "verify all nodes availability")
	}

	allLocations := strset.New()
	locationHosts := make(map[Location][]string)
	for _, l := range t.Location {
		p := l.RemotePath("")
		if allLocations.Has(p) {
			return errors.Errorf("location %s is specified multiple times", l)
		}
		allLocations.Add(p)

		var (
			remotePath     = l.RemotePath("")
			locationStatus = status
		)
		// In case location does not have specified dc, use nodes from all dcs
		if l.DC == "" {
			w.logger.Info(ctx, "No datacenter specified for location - using all nodes for this location", "location", l)
		} else {
			locationStatus = status.Datacenter([]string{l.DC})
		}

		nodes, err := w.client.GetNodesWithLocationAccess(ctx, locationStatus, remotePath)
		if err != nil {
			if strings.Contains(err.Error(), "NoSuchBucket") {
				return errors.Errorf("specified bucket does not exist: %s", l)
			}
			return errors.Wrapf(err, "location %s is not accessible", l)
		}
		if len(nodes) == 0 {
			return fmt.Errorf("no nodes with location %s access", l)
		}

		var hosts []string
		for _, n := range nodes {
			hosts = append(hosts, n.Addr)
		}

		w.logger.Info(ctx, "Found hosts with location access", "location", l, "hosts", hosts)
		locationHosts[l] = hosts
	}
	t.locationHosts = locationHosts
	t.sortLocations()

	w.target = t
	w.run.SnapshotTag = t.SnapshotTag
	w.logger.Info(ctx, "Initialized target", "target", t)

	return nil
}

// initUnits should be called with already initialized target.
func (w *worker) initUnits(ctx context.Context) error {
	var (
		units   []Unit
		unitMap = make(map[string]Unit)
	)

	var foundManifest bool
	for _, l := range w.target.Location {
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

				ru.Tables = append(ru.Tables, Table{
					Table: fm.Table,
					Size:  fm.Size,
				})
				unitMap[fm.Keyspace] = ru
			}

			return miwc.ForEachIndexIter(w.target.Keyspace, filesHandler)
		}

		if err := w.forEachManifest(ctx, l, manifestHandler); err != nil {
			return err
		}
	}

	if !foundManifest {
		return errors.Errorf("no snapshot with tag %s", w.run.SnapshotTag)
	}

	for _, u := range unitMap {
		units = append(units, u)
	}

	if units == nil {
		return errors.New("no data in backup locations match given keyspace pattern")
	}

	for _, u := range units {
		for i, t := range u.Tables {
			mode, err := w.GetTableTombstoneGCMode(u.Keyspace, t.Table)
			if err != nil {
				return errors.Wrapf(err, "get tombstone_gc of %s.%s", u.Keyspace, t.Table)
			}
			u.Tables[i].TombstoneGC = mode
		}
	}

	w.run.Units = units
	w.logger.Info(ctx, "Initialized units", "units", units)

	return nil
}

var (
	regexMV = regexp.MustCompile(`CREATE\s*MATERIALIZED\s*VIEW`)
	regexSI = regexp.MustCompile(`CREATE\s*INDEX`)
)

// initViews should be called with already initialized target and units.
func (w *worker) initViews(ctx context.Context) error {
	restoredTables := strset.New()
	for _, u := range w.run.Units {
		for _, t := range u.Tables {
			restoredTables.Add(u.Keyspace + "." + t.Table)
		}
	}

	keyspaces, err := w.client.Keyspaces(ctx)
	if err != nil {
		return errors.Wrapf(err, "get keyspaces")
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

	var views []View
	for _, ks := range keyspaces {
		meta, err := w.clusterSession.KeyspaceMetadata(ks)
		if err != nil {
			return errors.Wrapf(err, "get keyspace %s metadata", ks)
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
				return errors.Wrapf(err, "get index %s.%s create statement", ks, index.Name)
			}

			// DummyMeta schema consists of create keyspace and create view statements
			stmt := strings.Split(schema, ";")[1]
			stmt, err = addIfNotExists(stmt, SecondaryIndex)
			if err != nil {
				return err
			}

			views = append(views, View{
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
				return errors.Wrapf(err, "get view %s.%s create statement", ks, view.ViewName)
			}

			// DummyMeta schema consists of create keyspace and create view statements
			stmt := strings.Split(schema, ";")[1]
			stmt, err = addIfNotExists(stmt, MaterializedView)
			if err != nil {
				return err
			}

			views = append(views, View{
				Keyspace:   view.KeyspaceName,
				View:       view.ViewName,
				Type:       MaterializedView,
				BaseTable:  view.BaseTableName,
				CreateStmt: stmt,
			})
		}
	}

	w.logger.Info(ctx, "Initialized views", "views", views)
	w.run.Views = views

	return nil
}

// cleanUploadDir deletes all SSTables from host's upload directory except for those present in excluded.
func (w *worker) cleanUploadDir(ctx context.Context, host, dir string, excluded []string) error {
	var toBeDeleted []string
	s := strset.New(excluded...)
	opts := &scyllaclient.RcloneListDirOpts{FilesOnly: true}

	err := w.client.RcloneListDirIter(ctx, host, dir, opts, func(item *scyllaclient.RcloneListDirItem) {
		if !s.Has(item.Name) {
			toBeDeleted = append(toBeDeleted, item.Name)
		}
	})
	if err != nil {
		return errors.Wrapf(err, "list dir: %s on host: %s", dir, host)
	}

	if len(toBeDeleted) > 0 {
		w.logger.Info(ctx, "Delete files from host's upload directory",
			"host", host,
			"upload_dir", dir,
			"files", toBeDeleted,
		)
	}

	for _, f := range toBeDeleted {
		remotePath := path.Join(dir, f)
		if err := w.client.RcloneDeleteFile(ctx, host, remotePath); err != nil {
			return errors.Wrapf(err, "delete file: %s on host: %s", remotePath, host)
		}
	}
	return nil
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
func (w *worker) DropView(ctx context.Context, view View) error {
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
func (w *worker) CreateView(ctx context.Context, view View) error {
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

func (w *worker) WaitForViewBuilding(ctx context.Context, view View) error {
	labels := metrics.RestoreViewBuildStatusLabels{
		ClusterID: w.run.ClusterID.String(),
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

// GetTableTombstoneGCMode returns table's tombstone_gc mode.
func (w *worker) GetTableTombstoneGCMode(keyspace, table string) (tombstoneGCMode, error) {
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

func (w *worker) insertRun(ctx context.Context) {
	if err := table.RestoreRun.InsertQuery(w.session).BindStruct(w.run).ExecRelease(); err != nil {
		w.logger.Error(ctx, "Insert run",
			"run", *w.run,
			"error", err,
		)
	}
}

func (w *worker) insertRunProgress(ctx context.Context, pr *RunProgress) {
	if err := table.RestoreRunProgress.InsertQuery(w.session).BindStruct(pr).ExecRelease(); err != nil {
		w.logger.Error(ctx, "Insert run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

func (w *worker) deleteRunProgress(ctx context.Context, pr *RunProgress) {
	if err := table.RestoreRunProgress.DeleteQuery(w.session).BindStruct(pr).ExecRelease(); err != nil {
		w.logger.Error(ctx, "Delete run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

func (w *worker) decorateWithPrevRun(ctx context.Context) error {
	prev, err := GetRun(w.session, w.run.ClusterID, w.run.TaskID, uuid.Nil)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil
		}
		return errors.Wrap(err, "get run")
	}

	if prev.Stage == StageDone {
		return nil
	}
	// Always copy units and views from previous run. Otherwise, restore will believe
	// that the initial cluster state has dropped views and disabled tombstone_gc.
	w.run.Units = prev.Units
	w.run.Views = prev.Views

	if w.target.Continue {
		w.run.PrevID = prev.ID
		w.run.Location = prev.Location
		w.run.ManifestPath = prev.ManifestPath
		w.run.Keyspace = prev.Keyspace
		w.run.Table = prev.Table
		w.run.Stage = prev.Stage
		w.run.RepairTaskID = prev.RepairTaskID
	}

	w.logger.Info(ctx, "Decorated run", "run", *w.run)
	return nil
}

// Clone insert all previous RunProgress for current run.
func (w *worker) clonePrevProgress(ctx context.Context) {
	q := table.RestoreRunProgress.InsertQuery(w.session)
	defer q.Release()

	err := forEachProgress(w.session, w.run.ClusterID, w.run.TaskID, w.run.PrevID, func(pr *RunProgress) {
		pr.RunID = w.run.ID
		if err := q.BindStruct(pr).Exec(); err != nil {
			w.logger.Error(ctx, "Couldn't clone run progress",
				"run_progress", *pr,
				"error", err,
			)
		}
	})
	if err != nil {
		w.logger.Error(ctx, "Couldn't clone run progress", "error", err)
	}
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

func (w *worker) clearJobStats(ctx context.Context, jobID int64, host string) {
	if err := w.client.RcloneDeleteJobStats(ctx, host, jobID); err != nil {
		w.logger.Error(ctx, "Failed to clear job stats",
			"host", host,
			"id", jobID,
			"error", err,
		)
	}
}

func (w *worker) stopJob(ctx context.Context, jobID int64, host string) {
	if err := w.client.RcloneJobStop(ctx, host, jobID); err != nil {
		w.logger.Error(ctx, "Failed to stop job",
			"host", host,
			"id", jobID,
			"error", err,
		)
	}
}

func (w *worker) forEachManifest(ctx context.Context, location Location, f func(ManifestInfoWithContent) error) error {
	closest := w.client.Config().Hosts
	hosts, ok := w.target.locationHosts[location]
	if !ok {
		return fmt.Errorf("no hosts for location %s", location)
	}

	var host string
	for _, h := range closest {
		if slice.ContainsString(hosts, h) {
			host = h
			break
		}
	}
	if host == "" {
		host = hosts[0]
	}

	manifests, err := w.getManifestInfo(ctx, host, location)
	if err != nil {
		return errors.Wrap(err, "list manifests")
	}

	// Load manifest content
	load := func(c *ManifestContentWithIndex, m *ManifestInfo) error {
		r, err := w.client.RcloneOpen(ctx, host, m.Location.RemotePath(m.Path()))
		if err != nil {
			return err
		}
		return multierr.Append(c.Read(r), r.Close())
	}

	for _, m := range manifests {
		c := new(ManifestContentWithIndex)
		if err := load(c, m); err != nil {
			return err
		}

		err := f(ManifestInfoWithContent{
			ManifestInfo:             m,
			ManifestContentWithIndex: c,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// getManifestInfo returns manifests with receiver's snapshot tag for all nodes in the location.
func (w *worker) getManifestInfo(ctx context.Context, host string, location Location) ([]*ManifestInfo, error) {
	baseDir := path.Join("backup", string(MetaDirKind))
	opts := scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
		Recurse:   true,
	}

	var manifests []*ManifestInfo
	err := w.client.RcloneListDirIter(ctx, host, location.RemotePath(baseDir), &opts, func(f *scyllaclient.RcloneListDirItem) {
		m := new(ManifestInfo)
		if err := m.ParsePath(path.Join(baseDir, f.Path)); err != nil {
			return
		}
		m.Location = location
		if m.SnapshotTag == w.run.SnapshotTag {
			manifests = append(manifests, m)
		}
	})
	if err != nil {
		return nil, err
	}

	// Ensure deterministic order
	sort.Slice(manifests, func(i, j int) bool {
		return manifests[i].NodeID < manifests[j].NodeID
	})
	return manifests, nil
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