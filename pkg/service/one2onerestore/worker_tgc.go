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
	for table := range getTablesToRestore(workload) {
		mode, err := w.getTableTombstoneGCMode(table.keyspace, table.table)
		if err != nil {
			return errors.Wrap(err, "get tombstone_gc mode")
		}
		// No need to change tombstone gc mode.
		if mode == modeDisabled || mode == modeImmediate || mode == modeRepair {
			w.logger.Info(ctx, "Skipping set tombstone_gc mode", "table", table, "mode", mode)
			continue
		}
		if err := w.setTableTombstoneGCMode(ctx, table.keyspace, table.table, modeRepair); err != nil {
			return errors.Wrap(err, "set tombstone_gc mode repair")
		}
	}

	return nil
}

// getTableTombstoneGCMode returns table's tombstone_gc mode.
func (w *worker) getTableTombstoneGCMode(keyspace, table string) (tombstoneGCMode, error) {
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
	return "", errors.Errorf("unrecognized tombstone_gc mode: %s", mode)
}

// setTableTombstoneGCMode alters 'tombstone_gc' mode.
func (w *worker) setTableTombstoneGCMode(ctx context.Context, keyspace, table string, mode tombstoneGCMode) error {
	w.logger.Info(ctx, "Alter table's tombstone_gc mode",
		"keyspace", keyspace,
		"table", table,
	)

	op := func() error {
		return w.clusterSession.ExecStmt(alterTableTombstoneGCStmt(keyspace, table, mode))
	}

	notify := func(err error, wait time.Duration) {
		w.logger.Info(ctx, "Altering table's tombstone_gc mode failed",
			"keyspace", keyspace,
			"table", table,
			"error", err,
			"wait", wait,
		)
	}

	return alterSchemaRetryWrapper(ctx, op, notify)
}

func alterTableTombstoneGCStmt(keyspace, table string, mode tombstoneGCMode) string {
	return fmt.Sprintf(`ALTER TABLE %q.%q WITH tombstone_gc = {'mode': '%s'}`, keyspace, table, mode)
}
