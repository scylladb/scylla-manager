// Copyright (C) 2026 ScyllaDB

package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
)

func (w *worker) DumpSchema(ctx context.Context, hi []hostInfo, sessionFunc cluster.SessionFunc) error {
	w.Logger.Info(ctx, "Back up schema from DESCRIBE SCHEMA WITH INTERNALS")

	var hosts []string
	for i := range hi {
		hosts = append(hosts, hi[i].IP)
	}
	session, host, err := w.createSingleHostSessionToAnyHost(ctx, hosts, sessionFunc)
	if err != nil {
		if errors.Is(err, cluster.ErrNoCQLCredentials) {
			err = errors.Wrapf(err, "CQL credentials are required to back up schema for this ScyllaDB version")
		}
		return errors.Wrap(err, "create single host CQL session")
	}
	defer session.Close()

	if err := w.Client.RaftReadBarrier(ctx, host, ""); err != nil {
		return errors.Wrap(err, "perform raft read barrier on desc schema host")
	}

	schema, err := query.DescribeSchemaWithInternals(session)
	if err != nil {
		return err
	}
	b, err := marshalAndCompressArchive(schema)
	if err != nil {
		return errors.Wrap(err, "create safe schema file")
	}

	w.SchemaFilePath = backupspec.RemoteSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag)
	w.Schema = b
	return nil
}

func (w *worker) UploadSchema(ctx context.Context, hosts []hostInfo) (stepError error) {
	if w.SchemaFilePath == "" {
		w.Logger.Info(ctx, "No schema CQL file to upload")
		return nil
	}

	// Select single host per location
	locations := map[string]hostInfo{}
	for i := range hosts {
		locations[hosts[i].Location.String()] = hosts[i]
	}
	hostPerLocation := make([]hostInfo, 0, len(locations))
	for l := range locations {
		hostPerLocation = append(hostPerLocation, locations[l])
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

func (w *worker) createSingleHostSessionToAnyHost(ctx context.Context, hosts []string, sessionFunc cluster.SessionFunc) (session gocqlx.Session, host string, err error) {
	var retErr error
	for _, h := range hosts {
		session, err := sessionFunc(ctx, w.ClusterID, cluster.SingleHostSessionConfigOption(h))
		if err != nil {
			w.Logger.Error(ctx, "Couldn't connect to host via CQL", "host", h, "error", err)
			retErr = err
		} else {
			return session, h, nil
		}
	}
	return gocqlx.Session{}, "", errors.Wrap(retErr, "no host that can be accessed via CQL (more info in the logs)")
}

func marshalAndCompressArchive(v any) (bytes.Buffer, error) {
	rawSchema, err := json.Marshal(v)
	if err != nil {
		return bytes.Buffer{}, errors.Wrap(err, "marshal to json")
	}

	var b bytes.Buffer
	gw := gzip.NewWriter(&b)
	if _, err := gw.Write(rawSchema); err != nil {
		return bytes.Buffer{}, errors.Wrap(err, "compress with gzip")
	}
	if err := gw.Close(); err != nil {
		return bytes.Buffer{}, errors.Wrap(err, "close gzip writer")
	}

	return b, nil
}
