// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func (w *worker) AwaitSchemaAgreement(ctx context.Context, clusterSession gocqlx.Session) {
	w.Logger.Info(ctx, "Awaiting schema agreement...")

	var stepError error
	defer func(start time.Time) {
		if stepError != nil {
			w.Logger.Error(ctx, "Awaiting schema agreement failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done awaiting schema agreement", "duration", timeutc.Since(start))
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
		w.Logger.Info(ctx, "Schema agreement not reached, retrying...", "error", err, "wait", wait)
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

func (w *worker) DumpSchema(ctx context.Context, clusterSession gocqlx.Session) error {
	b, err := createSchemaArchive(ctx, w.Units, clusterSession)
	if err != nil {
		return errors.Wrap(err, "get schema")
	}
	w.Schema = b
	return nil
}

func (w *worker) UploadSchema(ctx context.Context, hosts []hostInfo) (stepError error) {
	if w.Schema == nil {
		return nil
	}

	w.Logger.Info(ctx, "Uploading schema...")

	defer func(start time.Time) {
		if stepError != nil {
			w.Logger.Error(ctx, "Uploading schema failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done uploading schema", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	// Select single host per location
	locations := map[string]hostInfo{}
	for _, hi := range hosts {
		locations[hi.Location.String()] = hi
	}
	hostPerLocation := make([]hostInfo, 0, len(locations))
	for _, hi := range locations {
		hostPerLocation = append(hostPerLocation, hi)
	}

	return hostsInParallel(hostPerLocation, parallel.NoLimit, func(h hostInfo) error {
		dst := h.Location.RemotePath(RemoteSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag))
		return w.Client.RclonePut(ctx, h.IP, dst, w.Schema)
	})
}

func (w *worker) RecordGraceSeconds(ctx context.Context, clusterSession gocqlx.Session, keyspace, table string) (int, error) {
	w.Logger.Info(ctx, "Retrieving gc_grace_seconds")

	q := qb.Select("system_schema.tables").
		Columns("gc_grace_seconds").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(clusterSession).
		Bind(keyspace, table)

	defer q.Release()

	var ggs int
	if err := q.Scan(&ggs); err != nil {
		w.Logger.Error(ctx, "Couldn't record gc_grace_seconds",
			"error", err,
		)

		return 0, errors.Wrap(err, "record gc_grace_seconds")
	}
	return ggs, nil
}

// TODO: what consistency level should be assigned to ALTER TABLE queries?

func (w *worker) SetGraceSeconds(ctx context.Context, clusterSession gocqlx.Session, keyspace, table string, ggs int) error {
	w.Logger.Info(ctx, "Setting gc_grace_seconds",
		"value", ggs,
	)

	alterStmt := fmt.Sprintf("ALTER TABLE %s.%s WITH gc_grace_seconds=%s", keyspace, table, strconv.Itoa(ggs))

	if err := clusterSession.ExecStmt(alterStmt); err != nil {
		w.Logger.Error(ctx, "Couldn't set gc_grace_seconds",
			"error", err,
		)

		return errors.Wrap(err, "set gc_grace_seconds")
	}
	return nil
}

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

func (w *worker) RecordCompaction(ctx context.Context, clusterSession gocqlx.Session, keyspace, table string) (compaction, error) {
	w.Logger.Info(ctx, "Retrieving compaction")

	q := qb.Select("system_schema.tables").
		Columns("compaction").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(clusterSession).
		Bind(keyspace, table)

	defer q.Release()

	var comp compaction
	if err := q.Scan(&comp); err != nil {
		w.Logger.Error(ctx, "Couldn't record compaction",
			"error", err,
		)

		return nil, errors.Wrap(err, "record compaction")
	}
	return comp, nil
}

func (w *worker) SetCompaction(ctx context.Context, clusterSession gocqlx.Session, keyspace, table string, comp compaction) error {
	w.Logger.Info(ctx, "Setting compaction",
		"value", comp,
	)

	alterStmt := fmt.Sprintf("ALTER TABLE %s.%s WITH compaction=%s", keyspace, table, comp)

	if err := clusterSession.ExecStmt(alterStmt); err != nil {
		w.Logger.Error(ctx, "Couldn't set compaction",
			"error", err,
		)

		return errors.Wrap(err, "set compaction")
	}
	return nil
}

// ExecOnDisabledTable executes given function with
// table's compaction and gc_grace_seconds temporarily disabled.
func (w *worker) ExecOnDisabledTable(ctx context.Context, clusterSession gocqlx.Session, keyspace, table string, f func() error) error {
	w.Logger.Info(ctx, "Disabling compaction and gc_grace_seconds",
		"keyspace", keyspace,
		"table", table,
	)

	// Temporarily disable compaction
	comp, err := w.RecordCompaction(ctx, clusterSession, keyspace, table)
	if err != nil {
		return err
	}

	tmpComp := make(compaction)
	for k, v := range comp {
		tmpComp[k] = v
	}
	// Disable compaction option
	tmpComp["enabled"] = "false"

	if err := w.SetCompaction(ctx, clusterSession, keyspace, table, tmpComp); err != nil {
		return err
	}
	// Reset compaction
	defer w.SetCompaction(ctx, clusterSession, keyspace, table, comp)

	// Temporarily set gc_grace_seconds to max supported value
	ggs, err := w.RecordGraceSeconds(ctx, clusterSession, keyspace, table)

	const maxGGS = math.MaxInt32
	if err := w.SetGraceSeconds(ctx, clusterSession, keyspace, table, maxGGS); err != nil {
		return err
	}
	// Reset gc_grace_seconds
	defer w.SetGraceSeconds(ctx, clusterSession, keyspace, table, ggs)

	err = f()

	w.Logger.Info(ctx, "Restoring compaction and gc_grace_seconds",
		"keyspace", keyspace,
		"table", table,
	)

	return err
}

// TODO: all RecordXXX could be merged into one query.

func (w *worker) RecordTableVersion(ctx context.Context, clusterSession gocqlx.Session, keyspace, table string) (string, error) {
	w.Logger.Info(ctx, "Retrieving table's version")

	q := qb.Select("system_schema.tables").
		Columns("id").
		Where(qb.Eq("keyspace_name"), qb.Eq("table_name")).
		Query(clusterSession).
		Bind(keyspace, table)

	defer q.Release()

	var version string
	if err := q.Scan(&version); err != nil {
		w.Logger.Error(ctx, "Couldn't record table's version",
			"error", err,
		)

		return "", errors.Wrap(err, "record table's version")
	}
	return version, nil
}
