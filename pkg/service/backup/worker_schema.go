// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func (w *workerTools) AwaitSchemaAgreement(ctx context.Context, clusterSession gocqlx.Session) {
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
