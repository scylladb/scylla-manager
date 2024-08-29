// Copyright (C) 2017 ScyllaDB

package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
)

func (w *worker) DumpSchema(ctx context.Context, hi []hostInfo, sessionFunc cluster.SessionFunc) error {
	var hosts []string
	for _, h := range hi {
		hosts = append(hosts, h.IP)
	}

	descSchemaHosts, err := backupAndRestoreFromDescSchemaHosts(ctx, w.Client, hosts)
	if err != nil {
		return errors.Wrap(err, "get hosts supporting backup/restore from desc schema with internals")
	}

	var session gocqlx.Session
	if len(descSchemaHosts) > 0 {
		session, err = w.createSingleHostSessionToAnyHost(ctx, descSchemaHosts, sessionFunc)
	} else {
		session, err = sessionFunc(ctx, w.ClusterID)
	}
	if err != nil {
		if errors.Is(err, cluster.ErrNoCQLCredentials) {
			w.Logger.Error(ctx, "No CQL cluster credentials, backup of schema as CQL files will be skipped", "error", err)
			return nil
		}
		return errors.Wrapf(err, "create cql session")
	}
	defer session.Close()

	if len(descSchemaHosts) > 0 {
		return w.safeDumpSchema(ctx, session)
	}
	w.unsafeDumpSchema(ctx, session)
	return nil
}

func (w *worker) safeDumpSchema(ctx context.Context, session gocqlx.Session) error {
	w.Logger.Info(ctx, "Schema backup for used ScyllaDB version is done via DESCRIBE SCHEMA WITH INTERNALS CQL query. "+
		"ScyllaDB Manager will snapshot system_schema sstables, but they won't be used during schema restoration")
	if err := query.RaftReadBarrier(session); err != nil {
		w.Logger.Error(ctx, "Couldn't perform raft read barrier, backup of schema as CQL files will be skipped "+
			"(restoring schema from this backup won't be possible)", "error", err)
		return nil
	}
	schema, err := query.DescribeSchemaWithInternals(session)
	if err != nil {
		return err
	}
	b, err := marshalAndCompressSchema(schema)
	if err != nil {
		return errors.Wrap(err, "create safe schema file")
	}
	w.SchemaFilePath = backupspec.RemoteSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag)
	w.Schema = b
	return nil
}

func (w *worker) unsafeDumpSchema(ctx context.Context, session gocqlx.Session) {
	w.Logger.Info(ctx, "Schema backup for used ScyllaDB version is done via snapshot of system_schema sstables. "+
		"ScyllaDB Manager will try to create unsafe schema CQL archive, but it won't be used during schema restoration")
	if err := session.AwaitSchemaAgreement(ctx); err != nil {
		w.Logger.Error(ctx, "Couldn't await schema agreement, backup of unsafe schema as CQL files will be skipped", "error", err)
		return
	}
	b, err := createUnsafeSchemaArchive(ctx, w.Units, session)
	if err != nil {
		w.Logger.Error(ctx, "Couldn't create unsafe schema archive, backup of unsafe schema as CQL files will be skipped", "error", err)
		return
	}
	w.SchemaFilePath = backupspec.RemoteUnsafeSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag)
	w.Schema = b
}

func (w *worker) UploadSchema(ctx context.Context, hosts []hostInfo) (stepError error) {
	if w.SchemaFilePath == "" {
		w.Logger.Info(ctx, "No schema CQL file to upload")
		return nil
	}

	// Select single host per location
	locations := map[string]hostInfo{}
	for _, hi := range hosts {
		locations[hi.Location.String()] = hi
	}
	hostPerLocation := make([]hostInfo, 0, len(locations))
	for _, hi := range locations {
		hostPerLocation = append(hostPerLocation, hi)
	}

	f := func(h hostInfo) error {
		dst := h.Location.RemotePath(w.SchemaFilePath)
		return w.Client.RclonePut(ctx, h.IP, dst, &w.Schema)
	}

	notify := func(h hostInfo, err error) {
		w.Logger.Error(ctx, "Failed to upload schema from host",
			"host", h.IP,
			"error", err,
		)
	}

	return hostsInParallel(hostPerLocation, parallel.NoLimit, f, notify)
}

func (w *worker) createSingleHostSessionToAnyHost(ctx context.Context, hosts []string, sessionFunc cluster.SessionFunc) (gocqlx.Session, error) {
	var retErr error
	for _, h := range hosts {
		session, err := sessionFunc(ctx, w.ClusterID, cluster.SingleHostSessionConfigOption(h))
		if err != nil {
			w.Logger.Error(ctx, "Couldn't connect to host via CQL", "host", h, "error", err)
			retErr = err
		} else {
			return session, nil
		}
	}
	return gocqlx.Session{}, errors.Wrap(retErr, "no host that can be accessed via CQL (more info in the logs)")
}

func marshalAndCompressSchema(schema query.DescribedSchema) (bytes.Buffer, error) {
	rawSchema, err := json.Marshal(schema)
	if err != nil {
		return bytes.Buffer{}, errors.Wrap(err, "marshal schema")
	}

	var b bytes.Buffer
	gw := gzip.NewWriter(&b)
	if _, err := gw.Write(rawSchema); err != nil {
		return bytes.Buffer{}, errors.Wrap(err, "write compressed schema")
	}
	if err := gw.Close(); err != nil {
		return bytes.Buffer{}, errors.Wrap(err, "close gzip writer")
	}

	return b, nil
}

// backupAndRestoreFromDescSchemaHosts returns hosts that restore schema from desc schema with internals output.
func backupAndRestoreFromDescSchemaHosts(ctx context.Context, client *scyllaclient.Client, hosts []string) ([]string, error) {
	var (
		mu  = sync.Mutex{}
		eg  = errgroup.Group{}
		out []string
	)
	for _, host := range hosts {
		h := host
		eg.Go(func() error {
			ni, err := client.NodeInfo(ctx, h)
			if err != nil {
				return err
			}
			res, err := ni.SupportsSafeDescribeSchemaWithInternals()
			if err != nil {
				return err
			}
			if res {
				mu.Lock()
				out = append(out, h)
				mu.Unlock()
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

func createUnsafeSchemaArchive(ctx context.Context, units []Unit, clusterSession gocqlx.Session) (b bytes.Buffer, err error) {
	gw := gzip.NewWriter(&b)
	tw := tar.NewWriter(gw)

	now := timeutc.Now()

	for _, u := range units {
		if err := ctx.Err(); err != nil {
			return bytes.Buffer{}, err
		}

		km, err := clusterSession.KeyspaceMetadata(u.Keyspace)
		if err != nil {
			return bytes.Buffer{}, errors.Wrapf(err, "describe keyspace %s schema", u.Keyspace)
		}

		cqlSchema, err := km.ToCQL()
		if err != nil {
			return bytes.Buffer{}, errors.Wrapf(err, "cql keyspace %s metadata", u.Keyspace)
		}

		if err := tw.WriteHeader(&tar.Header{
			Name:    u.Keyspace + ".cql",
			Size:    int64(len(cqlSchema)),
			Mode:    0o600,
			ModTime: now,
		}); err != nil {
			return bytes.Buffer{}, errors.Wrapf(err, "tar keyspace %s schema", u.Keyspace)
		}

		if _, err := io.Copy(tw, strings.NewReader(cqlSchema)); err != nil {
			return bytes.Buffer{}, errors.Wrapf(err, "copy %s schema", u.Keyspace)
		}
	}

	if err := multierr.Combine(tw.Close(), gw.Close()); err != nil {
		return bytes.Buffer{}, errors.Wrap(err, "writer close")
	}

	return b, nil
}
