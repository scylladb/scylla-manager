// Copyright (C) 2023 ScyllaDB

package restore

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
