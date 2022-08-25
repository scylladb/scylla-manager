package backup

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
)

// compaction strategy is represented as map of options.
// Note that altering table's compaction strategy completely overrides previous one
// (old options that are not specified in new strategy are discarded).
// 'class' option is mandatory when any option is specified.
type compaction map[string]string

func (c compaction) String() string {
	var opts []string
	for k, v := range c {
		opts = append(opts, fmt.Sprintf("'%s': '%s'", k, v))
	}

	cqlOpts := strings.Join(opts, ", ")
	return fmt.Sprintf("{%s}", cqlOpts)
}

func (w *restoreWorker) RecordGraceSecondsAndCompaction(ctx context.Context, clusterSession gocqlx.Session, keyspace, table string) (int, compaction, error) {
	w.Logger.Info(ctx, "Retrieving gc_grace_seconds and compaction")

	q := qb.Select("system_schema.tables").
		Columns("gc_grace_seconds", "compaction").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(clusterSession).
		Bind(keyspace, table)
	defer q.Release()

	var (
		ggs  int
		comp compaction
	)

	if err := q.Scan(&ggs, &comp); err != nil {
		return 0, nil, errors.Wrap(err, "record gc_grace_seconds and compaction")
	}

	return ggs, comp, nil
}

// TODO: what consistency level should be assigned to ALTER TABLE queries?

func (w *restoreWorker) SetGraceSecondsAndCompaction(ctx context.Context, clusterSession gocqlx.Session, keyspace, table string, ggs int, comp compaction) error {
	w.Logger.Info(ctx, "Setting gc_grace_seconds and compaction",
		"gc_grace_seconds", ggs,
		"compaction", comp.String(),
	)

	alterStmt := fmt.Sprintf("ALTER TABLE %s.%s WITH gc_grace_seconds=%s AND compaction=%s", keyspace, table, strconv.Itoa(ggs), comp)

	if err := clusterSession.ExecStmt(alterStmt); err != nil {
		return errors.Wrap(err, "set gc_grace_seconds and compaction")
	}

	return nil
}

// ExecOnDisabledTable executes given function with
// table's compaction and gc_grace_seconds temporarily disabled.
func (w *restoreWorker) ExecOnDisabledTable(ctx context.Context, clusterSession gocqlx.Session, keyspace, table string, f func() error) error {
	w.Logger.Info(ctx, "Temporarily disabling compaction and gc_grace_seconds",
		"keyspace", keyspace,
		"table", table,
	)

	// Temporarily disable gc grace seconds and compaction
	ggs, comp, err := w.RecordGraceSecondsAndCompaction(ctx, clusterSession, keyspace, table)
	if err != nil {
		return err
	}

	var (
		maxGGS  = math.MaxInt32
		tmpComp = make(compaction)
	)

	for k, v := range comp {
		tmpComp[k] = v
	}
	// Disable compaction option
	tmpComp["enabled"] = "false"

	if err := w.SetGraceSecondsAndCompaction(ctx, clusterSession, keyspace, table, maxGGS, tmpComp); err != nil {
		return err
	}
	// Reset gc grace seconds and compaction
	defer w.SetGraceSecondsAndCompaction(ctx, clusterSession, keyspace, table, ggs, comp)

	return f()
}

func (w *restoreWorker) RecordTableVersion(ctx context.Context, clusterSession gocqlx.Session, keyspace, table string) (string, error) {
	w.Logger.Info(ctx, "Retrieving table's version")

	q := qb.Select("system_schema.tables").
		Columns("id").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(clusterSession).
		Bind(keyspace, table)

	defer q.Release()

	var version string
	if err := q.Scan(&version); err != nil {
		return "", errors.Wrap(err, "record table's version")
	}
	// Table's version is stripped of '-' characters
	return strings.ReplaceAll(version, "-", ""), nil
}
