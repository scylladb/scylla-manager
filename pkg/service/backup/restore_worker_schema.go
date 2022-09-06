// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2/qb"
	"go.uber.org/multierr"
)

func (w *restoreWorker) ValidateTableExists(ctx context.Context, keyspace, table string) error {
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

func (w *restoreWorker) GetGraceSeconds(ctx context.Context, keyspace, table string) (int, error) {
	q := qb.Select("system_schema.tables").
		Columns("gc_grace_seconds").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(w.clusterSession).
		Bind(keyspace, table)
	defer q.Release()

	var ggs int
	if err := q.Scan(&ggs); err != nil {
		return 0, errors.Wrap(err, "get gc_grace_seconds")
	}

	w.Logger.Info(ctx, "Received table's gc_grace_seconds",
		"keyspace", keyspace,
		"table", table,
		"gc_grace_seconds", ggs,
	)

	return ggs, nil
}

func (w *restoreWorker) SetGraceSeconds(ctx context.Context, keyspace, table string, ggs int) error {
	w.Logger.Info(ctx, "Setting table's gc_grace_seconds",
		"keyspace", keyspace,
		"table", table,
		"gc_grace_seconds", ggs,
	)

	if err := w.clusterSession.ExecStmt(alterGGSStatement(keyspace, table, ggs)); err != nil {
		return errors.Wrap(err, "set gc_grace_seconds")
	}

	return nil
}

// ExecOnDisabledTable executes given function with
// table's compaction and gc_grace_seconds temporarily disabled.
func (w *restoreWorker) ExecOnDisabledTable(ctx context.Context, keyspace, table string, f func() error) (err error) {
	w.Logger.Info(ctx, "Temporarily disabling compaction and gc_grace_seconds",
		"keyspace", keyspace,
		"table", table,
	)

	// Temporarily disable gc grace seconds
	ggs, err := w.GetGraceSeconds(ctx, keyspace, table)
	if err != nil {
		return err
	}

	const maxGGS = math.MaxInt32
	if err = w.SetGraceSeconds(ctx, keyspace, table, maxGGS); err != nil {
		return err
	}
	// Reset gc grace seconds
	defer func() {
		if ggsErr := w.SetGraceSeconds(context.Background(), keyspace, table, ggs); ggsErr != nil {
			ggsErr = fmt.Errorf(
				"%w: please reset gc_grace_seconds manually by running \"%s\" in CQLSH",
				ggsErr,
				alterGGSStatement(keyspace, table, ggs),
			)
			err = multierr.Append(err, ggsErr)
		}
	}()

	// Temporarily disable compaction
	comp, err := w.Client.IsAutoCompactionEnabled(ctx, keyspace, table)
	if comp {
		if err = w.Client.DisableAutoCompaction(ctx, keyspace, table); err != nil {
			return err
		}
		// Reset compaction
		defer func() {
			if compErr := w.Client.EnableAutoCompaction(context.Background(), keyspace, table); compErr != nil {
				compErr = fmt.Errorf(
					"%w: please enable autocompaction manually by running \"nodetool enableautocompaction %s.%s\"",
					compErr,
					keyspace,
					table,
				)
				err = multierr.Append(err, compErr)
			}
		}()
	}

	return f()
}

func alterGGSStatement(keyspace, table string, ggs int) string {
	return fmt.Sprintf("ALTER TABLE %s.%s WITH gc_grace_seconds=%d", keyspace, table, ggs)
}

func (w *restoreWorker) GetTableVersion(ctx context.Context, keyspace, table string) (string, error) {
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
