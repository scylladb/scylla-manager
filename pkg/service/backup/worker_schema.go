// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"golang.org/x/sync/errgroup"
)

func (w *worker) DumpSchema(ctx context.Context, clusterSession gocqlx.Session, hosts []string) error {
	safe, err := isDescribeSchemaSafe(ctx, w.Client, hosts)
	if err != nil {
		return errors.Wrap(err, "check describe schema support")
	}

	if safe {
		if err := query.RaftReadBarrier(clusterSession); err != nil {
			w.Logger.Error(ctx, "Couldn't perform raft read barrier, backup of schema as CQL files will be skipped", "error", err)
			return nil
		}
	} else {
		if err := clusterSession.AwaitSchemaAgreement(ctx); err != nil {
			w.Logger.Error(ctx, "Couldn't await schema agreement, backup of unsafe schema as CQL files will be skipped", "error", err)
			return nil
		}
	}

	schema, err := query.DescribeSchemaWithInternals(clusterSession)
	if err != nil {
		return err
	}
	b, err := marshalAndCompressSchema(schema)
	if err != nil {
		return err
	}

	if safe {
		w.SchemaFilePath = backupspec.RemoteSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag)
	} else {
		w.SchemaFilePath = backupspec.RemoteUnsafeSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag)
		w.Logger.Info(ctx, "Backing-up and restoring schema from output of "+
			"DESCRIBE SCHEMA WITH INTERNALS is supported starting from ScyllaDB 6.0 or 2024.2. "+
			"Previous versions restore schema via snapshot of system_schema sstables")
	}

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

// isDescribeSchemaSafe checks if restoring schema from DESCRIBE SCHEMA WITH INTERNALS is safe.
func isDescribeSchemaSafe(ctx context.Context, client *scyllaclient.Client, hosts []string) (bool, error) {
	out := atomic.Bool{}
	out.Store(true)
	eg := errgroup.Group{}

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
			if !res {
				out.Store(false)
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return false, err
	}
	return out.Load(), nil
}
