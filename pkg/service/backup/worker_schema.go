// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/pkg/util/retry"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
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
		waitMin        = 15 * time.Second
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

func (w *worker) UploadSchema(ctx context.Context, hosts []hostInfo, b *bytes.Buffer) (stepError error) {
	w.Logger.Info(ctx, "Uploading schema...")

	defer func(start time.Time) {
		if stepError != nil {
			w.Logger.Error(ctx, "Uploading schema failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done uploading schema", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	// Single schema file per location.
	locations := map[string]hostInfo{}
	for _, hi := range hosts {
		locations[hi.Location.String()] = hi
	}
	hostsPerLocation := make([]hostInfo, 0, len(locations))
	for _, hi := range locations {
		hostsPerLocation = append(hostsPerLocation, hi)
	}

	var (
		rollbacks []func(context.Context) error
		mu        sync.Mutex
	)

	err := parallel.Run(len(hostsPerLocation), parallel.NoLimit, func(i int) error {
		h := hostsPerLocation[i]
		r, err := w.uploadHostSchema(ctx, h, b)

		mu.Lock()
		rollbacks = append(rollbacks, r)
		mu.Unlock()

		return errors.Wrapf(err, "host %s", h.IP)
	})
	if err != nil {
		// Parent context might be already canceled, use background context
		// Request timeout is configured on transport layer
		ctx = context.Background()

		for i := range rollbacks {
			if rollbacks[i] != nil {
				if err := rollbacks[i](ctx); err != nil {
					w.Logger.Error(ctx, "Cannot rollback schema upload", "error", err)
				}
			}
		}

		return err
	}

	w.SchemaUploaded = true
	return nil
}

func (w *worker) uploadHostSchema(ctx context.Context, h hostInfo, b *bytes.Buffer) (func(context.Context) error, error) {
	w.Logger.Info(ctx, "Uploading schema on host", "host", h.IP)

	schemaDst := h.Location.RemotePath(remoteSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag))

	rollback := func(ctx context.Context) error {
		return errors.Wrapf(w.deleteHostFile(ctx, h.IP, schemaDst), "delete schema file at %s", schemaDst)
	}
	// Create new reader for each request, to not consume source buffer
	r := bytes.NewReader(b.Bytes())

	return rollback, errors.Wrapf(w.Client.RclonePut(ctx, h.IP, schemaDst, r, int64(b.Len())), "upload schema file to %s", schemaDst)
}
