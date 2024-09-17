// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/pkg/util/version"
)

// restoreWorkerTools consists of utils common for both schemaWorker and tablesWorker.
type worker struct {
	run             *Run
	target          Target
	describedSchema *query.DescribedSchema

	config  Config
	logger  log.Logger
	metrics metrics.RestoreMetrics

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
		notRestored, err := skipRestorePatterns(ctx, w.client, w.clusterSession)
		if err != nil {
			return errors.Wrap(err, "find not restored tables")
		}
		w.logger.Info(ctx, "Extended excluded tables pattern", "pattern", notRestored)
		t.Keyspace = append(t.Keyspace, notRestored...)
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

	if t.RestoreSchema {
		w.logger.Info(ctx, "Look for schema file")
		w.describedSchema, err = getDescribedSchema(ctx, w.client, t.SnapshotTag, locationHosts)
		if err != nil {
			return errors.Wrap(err, "look for schema file")
		}

		if w.describedSchema == nil {
			w.logger.Info(ctx, "Couldn't find schema file. Proceeding with schema restoration using sstables")
			if err := IsRestoreSchemaFromSSTablesSupported(ctx, w.client); err != nil {
				return errors.Wrap(err, "check safety of restoring schema from sstables")
			}
		} else {
			w.logger.Info(ctx, "Found schema file")
		}
	}

	w.logger.Info(ctx, "Initialized target", "target", t)
	return nil
}

func skipRestorePatterns(ctx context.Context, client *scyllaclient.Client, session gocqlx.Session) ([]string, error) {
	keyspaces, err := client.KeyspacesByType(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get keyspaces by type")
	}
	tables, err := client.AllTables(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get all tables")
	}

	var skip []string
	// Skip local data.
	// Note that this also covers the raft based tables (e.g. system and system_schema).
	for _, ks := range keyspaces[scyllaclient.KeyspaceTypeAll] {
		if !slices.Contains(keyspaces[scyllaclient.KeyspaceTypeNonLocal], ks) {
			skip = append(skip, ks)
		}
	}

	// Skip outdated tables.
	// Note that even though system_auth is not used in Scylla 6.0,
	// it might still be present there (leftover after upgrade).
	// That's why SM should always skip known outdated tables so that backups
	// from older Scylla versions don't cause unexpected problems.
	if err := IsRestoreAuthAndServiceLevelsFromSStablesSupported(ctx, client); err != nil {
		if errors.Is(err, ErrRestoreAuthAndServiceLevelsUnsupportedScyllaVersion) {
			skip = append(skip, "system_auth", "system_distributed.service_levels")
		} else {
			return nil, errors.Wrap(err, "check auth and service levels restore support")
		}
	}

	// Skip system cdc tables
	systemCDCTableRegex := regexp.MustCompile(`(^|_)cdc(_|$)`)
	for ks, tabs := range tables {
		// Local keyspaces were already excluded
		if !slices.Contains(keyspaces[scyllaclient.KeyspaceTypeNonLocal], ks) {
			continue
		}
		// Here we only skip system cdc tables
		if slices.Contains(keyspaces[scyllaclient.KeyspaceTypeUser], ks) {
			continue
		}
		for _, t := range tabs {
			if systemCDCTableRegex.MatchString(t) {
				skip = append(skip, ks+"."+t)
			}
		}
	}

	// Skip user cdc tables
	skip = append(skip, "*.*_scylla_cdc_log")

	// Skip views
	views, err := query.GetAllViews(session)
	if err != nil {
		return nil, errors.Wrap(err, "get cluster views")
	}
	skip = append(skip, views.List()...)

	// Exclude collected patterns
	out := make([]string, 0, len(skip))
	for _, p := range skip {
		out = append(out, "!"+p)
	}
	return out, nil
}

// ErrRestoreSchemaUnsupportedScyllaVersion means that restore schema procedure is not safe for used Scylla configuration.
var ErrRestoreSchemaUnsupportedScyllaVersion = errors.Errorf("restore into cluster with given ScyllaDB version and consistent_cluster_management is not supported. " +
	"See https://manager.docs.scylladb.com/stable/restore/restore-schema.html for a workaround.")

// IsRestoreSchemaFromSSTablesSupported checks if restore schema procedure is supported for used Scylla configuration.
// Because of #3662, there is no way fo SM to safely restore schema into cluster with consistent_cluster_management
// and version higher or equal to OSS 5.4 or ENT 2024. There is a documented workaround in SM docs.
func IsRestoreSchemaFromSSTablesSupported(ctx context.Context, client *scyllaclient.Client) error {
	const (
		DangerousConstraintOSS = ">= 6.0, < 2000"
		DangerousConstraintENT = ">= 2024.2, > 1000"
		SafeConstraintOSS      = "< 5.4, < 2000"
		SafeConstraintENT      = "< 2024, > 1000"
	)

	raftSchema := false
	raftIsSafe := true

	status, err := client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get status")
	}
	for _, n := range status {
		ni, err := client.NodeInfo(ctx, n.Addr)
		if err != nil {
			return errors.Wrapf(err, "get node %s info", n.Addr)
		}

		dangerousOSS, err := version.CheckConstraint(ni.ScyllaVersion, DangerousConstraintOSS)
		if err != nil {
			return errors.Wrapf(err, "check version constraint for %s", n.Addr)
		}
		dangerousENT, err := version.CheckConstraint(ni.ScyllaVersion, DangerousConstraintENT)
		if err != nil {
			return errors.Wrapf(err, "check version constraint for %s", n.Addr)
		}
		safeOSS, err := version.CheckConstraint(ni.ScyllaVersion, SafeConstraintOSS)
		if err != nil {
			return errors.Wrapf(err, "check version constraint for %s", n.Addr)
		}
		safeENT, err := version.CheckConstraint(ni.ScyllaVersion, SafeConstraintENT)
		if err != nil {
			return errors.Wrapf(err, "check version constraint for %s", n.Addr)
		}

		if dangerousOSS || dangerousENT {
			raftSchema = true
			raftIsSafe = false
		} else if !safeOSS && !safeENT {
			raftSchema = raftSchema || ni.ConsistentClusterManagement
			raftIsSafe = false
		}
	}

	if raftSchema && !raftIsSafe {
		return ErrRestoreSchemaUnsupportedScyllaVersion
	}
	return nil
}

// ErrRestoreAuthAndServiceLevelsUnsupportedScyllaVersion means that restore auth and service levels procedure is not safe for used Scylla configuration.
var ErrRestoreAuthAndServiceLevelsUnsupportedScyllaVersion = errors.Errorf("restoring authentication and service levels is not supported for given ScyllaDB version")

// IsRestoreAuthAndServiceLevelsFromSStablesSupported checks if restore auth and service levels procedure is supported for used Scylla configuration.
// Because of #3869 and #3875, there is no way fo SM to safely restore auth and service levels into cluster with
// version higher or equal to OSS 6.0 or ENT 2024.2.
func IsRestoreAuthAndServiceLevelsFromSStablesSupported(ctx context.Context, client *scyllaclient.Client) error {
	const (
		ossConstraint = ">= 6.0, < 2000"
		entConstraint = ">= 2024.2, > 1000"
	)

	status, err := client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get status")
	}
	for _, n := range status {
		ni, err := client.NodeInfo(ctx, n.Addr)
		if err != nil {
			return errors.Wrapf(err, "get node %s info", n.Addr)
		}

		ossNotSupported, err := version.CheckConstraint(ni.ScyllaVersion, ossConstraint)
		if err != nil {
			return errors.Wrapf(err, "check version constraint for %s", n.Addr)
		}
		entNotSupported, err := version.CheckConstraint(ni.ScyllaVersion, entConstraint)
		if err != nil {
			return errors.Wrapf(err, "check version constraint for %s", n.Addr)
		}

		if ossNotSupported || entNotSupported {
			return ErrRestoreAuthAndServiceLevelsUnsupportedScyllaVersion
		}
	}

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
