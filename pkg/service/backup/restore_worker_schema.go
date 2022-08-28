package backup

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type fakeSchemaError struct{}

func (err fakeSchemaError) Error() string {
	return "fake error to stop manifest iteration"
}

func (w *restoreWorker) RestoreSchema(ctx context.Context, target RestoreTarget) error {
	w.Logger.Info(ctx, "Restoring schema")

	var (
		prevClusterID uuid.UUID // ID of the cluster on which snapshot was taken
		prevTaskID    uuid.UUID // ID of the backup task that created restored snapshot

		l = target.Location[0] // Any location is good for restoring schema
	)

	// Snapshot's ClusterID and TaskID can be obtained from any snapshot's manifest
	err := w.forEachRestoredManifest(ctx, l, func(miwc ManifestInfoWithContent) error {
		prevClusterID = miwc.ClusterID
		prevTaskID = miwc.TaskID
		// One manifest is enough to get ClusterID and TaskID
		return fakeSchemaError{}
	})

	if !errors.Is(err, fakeSchemaError{}) {
		return errors.Wrap(err, "iterate over manifests")
	}

	status, err := w.Client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get client status")
	}

	nodes, err := w.Client.GetLiveNodes(ctx, status.Datacenter([]string{l.DC}))
	if err != nil {
		return errors.Wrap(err, "get live nodes")
	}

	var (
		schemaPath       = l.RemotePath(RemoteSchemaFile(prevClusterID, prevTaskID, target.SnapshotTag))
		compressedSchema []byte
	)

	for _, n := range nodes {
		compressedSchema, err = w.Client.RcloneCat(ctx, n.Addr, schemaPath)
		if err == nil {
			break
		}

		w.Logger.Error(ctx, "Couldn't get schema from backup location",
			"host", n.Addr,
			"error", err,
		)
	}

	if compressedSchema == nil {
		return errors.New("none of the hosts could fetch compressed schema")
	}

	srcr := bytes.NewBuffer(compressedSchema)
	gr, err := gzip.NewReader(srcr)
	if err != nil {
		return errors.Wrap(err, "create gzip reader")
	}

	tr := tar.NewReader(gr)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "decompress schema file")
		}

		w.Logger.Info(ctx, "Applying schema file",
			"file", hdr.Name,
		)

		dstr := bufio.NewReader(tr)
		for {
			// Schema files consist of multiple statements.
			stmt, err := dstr.ReadString(';')
			if err != nil {
				if err == io.EOF && strings.TrimSpace(stmt) == "" {
					break
				}

				return errors.Wrap(err, "read next schema change")
			}

			err = w.clusterSession.ExecStmt(stmt)
			if err != nil {
				return errors.Wrap(err, "apply schema change")
			}
		}
	}

	w.AwaitSchemaAgreement(ctx, w.clusterSession)

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
