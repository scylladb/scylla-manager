// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/scylladb/gocqlx/v2/qb"
)

// Docs: https://docs.scylladb.com/stable/cql/ddl.html#tombstones-gc-options.
type tombstoneGCMode string

const (
	modeDisabled  tombstoneGCMode = "disabled"
	modeTimeout   tombstoneGCMode = "timeout"
	modeRepair    tombstoneGCMode = "repair"
	modeImmediate tombstoneGCMode = "immediate"
)

// setTombstoneGCModeRepair sets tombstone gc mode to repair to avoid data resurrection issues during restore.
func (w *worker) setTombstoneGCModeRepair(ctx context.Context, workload []hostWorkload) error {
	type alterTGCTarget struct {
		keyspace string
		name     string
		isView   bool
	}
	var targets []alterTGCTarget
	for table := range getTablesToRestore(workload) {
		targets = append(targets, alterTGCTarget{
			keyspace: table.keyspace,
			name:     table.table,
			isView:   false,
		})
	}
	views, err := w.getViews(ctx, workload)
	if err != nil {
		return errors.Wrap(err, "get views")
	}
	for _, view := range views {
		viewName := view.View
		if view.Type == SecondaryIndex {
			viewName += "_index"
		}
		targets = append(targets, alterTGCTarget{
			keyspace: view.Keyspace,
			name:     viewName,
			isView:   true,
		})
	}

	for _, target := range targets {
		mode, err := w.getTombstoneGCMode(target.keyspace, target.name, target.isView)
		if err != nil {
			return errors.Wrapf(err, "get tombstone_gc mode: %s.%s", target.keyspace, target.name)
		}
		// No need to change tombstone gc mode.
		if mode == modeDisabled || mode == modeImmediate || mode == modeRepair {
			w.logger.Info(ctx, "Skipping set tombstone_gc mode", "name", target.keyspace+"."+target.name, "mode", mode)
			continue
		}
		if err := w.setTombstoneGCMode(ctx, target.keyspace, target.name, target.isView, modeRepair); err != nil {
			return errors.Wrapf(err, "set tombstone_gc mode repair: %s.%s", target.keyspace, target.name)
		}
	}

	return nil
}

// getTombstoneGCMode returns table's tombstone_gc mode.
func (w *worker) getTombstoneGCMode(keyspace, name string, isView bool) (tombstoneGCMode, error) {
	systemSchemaTable := "system_schema.tables"
	columnName := "table_name"
	if isView {
		systemSchemaTable = "system_schema.views"
		columnName = "view_name"
	}
	var ext map[string]string
	q := qb.Select(systemSchemaTable).
		Columns("extensions").
		Where(qb.Eq("keyspace_name"), qb.Eq(columnName)).
		Query(w.clusterSession).
		Bind(keyspace, name)

	defer q.Release()
	err := q.Scan(&ext)
	if err != nil {
		return "", errors.Wrap(err, "scan")
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
	return "", errors.Errorf("unrecognized tombstone_gc mode: %s", mode)
}

// setTombstoneGCMode alters 'tombstone_gc' mode.
func (w *worker) setTombstoneGCMode(ctx context.Context, keyspace, name string, isView bool, mode tombstoneGCMode) error {
	logger := w.logger.With("keyspace", keyspace, "name", name, "is_view", isView)

	logger.Info(ctx, "Alter tombstone_gc mode")

	op := func() error {
		stmt := alterTableTombstoneGCStmt(keyspace, name, mode)
		if isView {
			stmt = alterViewTombstoneGCStmt(keyspace, name, mode)
		}
		return w.clusterSession.ExecStmt(stmt)
	}

	notify := func(err error, wait time.Duration) {
		logger.Info(ctx, "Altering tombstone_gc mode failed",
			"error", err,
			"wait", wait,
		)
	}

	return alterSchemaRetryWrapper(ctx, op, notify)
}

func alterTableTombstoneGCStmt(keyspace, table string, mode tombstoneGCMode) string {
	return fmt.Sprintf(`ALTER TABLE %q.%q WITH tombstone_gc = {'mode': '%s'}`, keyspace, table, mode)
}

func alterViewTombstoneGCStmt(keyspace, view string, mode tombstoneGCMode) string {
	return fmt.Sprintf(`ALTER MATERIALIZED VIEW %q.%q WITH tombstone_gc = {'mode': '%s'}`, keyspace, view, mode)
}
