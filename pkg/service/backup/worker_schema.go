// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/version"
)

func (w *workerTools) AwaitSchemaAgreement(ctx context.Context, clusterSession gocqlx.Session) error {
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

	return retry.WithNotify(ctx, func() error {
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

func (w *worker) DumpSchema(ctx context.Context, clusterSession gocqlx.Session, hosts []string) error {
	schema, err := query.DescribeSchemaWithInternals(clusterSession)
	if err != nil {
		return err
	}

	b, err := marshalAndCompressSchema(schema)
	if err != nil {
		return err
	}

	safe, err := isDescribeSchemaSafe(ctx, w.Client, hosts)
	if err != nil {
		return errors.Wrap(err, "check describe schema support")
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
	const (
		ConstraintOSS = ">= 6.0, < 2000"
		ConstraintENT = ">= 2024.2, > 1000"
	)

	for _, h := range hosts {
		ni, err := client.NodeInfo(ctx, h)
		if err != nil {
			return false, errors.Wrapf(err, "get node %s info", h)
		}

		oss, err := version.CheckConstraint(ni.ScyllaVersion, ConstraintOSS)
		if err != nil {
			return false, errors.Wrapf(err, "check version constraint for %s", h)
		}
		ent, err := version.CheckConstraint(ni.ScyllaVersion, ConstraintENT)
		if err != nil {
			return false, errors.Wrapf(err, "check version constraint for %s", h)
		}

		if !oss && !ent {
			return false, nil
		}
	}

	return true, nil
}
