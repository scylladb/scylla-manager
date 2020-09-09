// Copyright (C) 2017 ScyllaDB

package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/util/parallel"
	"github.com/scylladb/mermaid/pkg/util/retry"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"go.uber.org/multierr"
)

func (w *worker) AwaitSchema(ctx context.Context) (stepError error) {
	w.Logger.Info(ctx, "Awaiting schema agreement...")
	defer func(start time.Time) {
		if stepError != nil {
			w.Logger.Error(ctx, "Awaiting schema agreement failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done awaiting schema agreement", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	if w.clusterSession.Session == nil {
		w.Logger.Info(ctx, "Skipping awaiting schema agreement due to missing cluster session")
		return nil
	}

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

	return retry.WithNotify(ctx, func() error {
		if err := w.clusterSession.AwaitSchemaAgreement(ctx); err != nil {
			if strings.Contains(err.Error(), "cluster schema versions not consistent") {
				return err
			}
			return retry.Permanent(err)
		}
		return nil
	}, backoff, notify)
}

func (w *worker) UploadSchema(ctx context.Context, hosts []hostInfo) (stepError error) {
	if w.clusterSession.Session == nil {
		w.Logger.Error(ctx, "Skipping schema backup due to missing cluster session")
		return nil
	}

	w.Logger.Info(ctx, "Uploading schema files...")
	defer func(start time.Time) {
		if stepError != nil {
			w.Logger.Error(ctx, "Uploading schema files failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done uploading schema files", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	rollbacks, err := w.uploadSchema(ctx, hosts)
	if err != nil {
		for i := range rollbacks {
			// Parent context might be already canceled, use background context.
			// Request timeout is configured on transport layer.
			if rollbacks[i] != nil {
				if err := rollbacks[i](context.Background()); err != nil {
					w.Logger.Error(ctx, "Cannot rollback schema upload", "error", err)
				}
			}
		}
		return err
	}

	return nil
}

func (w *worker) uploadSchema(ctx context.Context, hosts []hostInfo) (rollbacks []func(context.Context) error, err error) {
	w.Logger.Info(ctx, "Uploading schema files")

	buf, err := w.createSchemaArchive(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "create schema archive")
	}

	// Single schema file per location.
	locations := map[string]hostInfo{}
	for _, hi := range hosts {
		locations[hi.Location.String()] = hi
	}
	hostsPerLocation := make([]hostInfo, 0, len(locations))
	for _, hi := range locations {
		hostsPerLocation = append(hostsPerLocation, hi)
	}

	var rollbacksMu sync.Mutex

	err = parallel.Run(len(locations), parallel.NoLimit, func(i int) error {
		hi := hostsPerLocation[i]
		schemaDst := hi.Location.RemotePath(remoteSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag))

		// Create new reader for each request, to not consume source buffer.
		if err := w.Client.RclonePut(ctx, hi.IP, schemaDst, bytes.NewReader(buf.Bytes()), int64(buf.Len())); err != nil {
			return errors.Wrap(err, "cqlsh describe schema")
		}

		rollbacksMu.Lock()
		rollbacks = append(rollbacks, func(ctx context.Context) error {
			if err := w.deleteHostFile(ctx, hi.IP, schemaDst); err != nil {
				return errors.Wrapf(err, "delete schema file at %s", schemaDst)
			}
			return nil
		})
		rollbacksMu.Unlock()

		return nil
	})
	if err != nil {
		return rollbacks, nil
	}

	w.SchemaUploaded = true

	return rollbacks, nil
}

func (w *worker) createSchemaArchive(ctx context.Context) (b *bytes.Buffer, err error) {
	b = &bytes.Buffer{}
	gw := gzip.NewWriter(b)
	tw := tar.NewWriter(gw)

	now := timeutc.Now()

	for _, u := range w.Units {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		w.Logger.Info(ctx, "Getting keyspace schema", "keyspace", u.Keyspace)
		km, err := w.clusterSession.KeyspaceMetadata(u.Keyspace)
		if err != nil {
			return nil, errors.Wrapf(err, "describe keyspace %s schema", u.Keyspace)
		}

		cqlSchema, err := km.ToCQL()
		if err != nil {
			return nil, errors.Wrapf(err, "cql keyspace %s metadata", u.Keyspace)
		}

		if err := tw.WriteHeader(&tar.Header{
			Name:    u.Keyspace + ".cql",
			Size:    int64(len(cqlSchema)),
			Mode:    0600,
			ModTime: now,
		}); err != nil {
			return nil, errors.Wrapf(err, "tar keyspace %s schema", u.Keyspace)
		}

		if _, err := io.Copy(tw, strings.NewReader(cqlSchema)); err != nil {
			return nil, errors.Wrapf(err, "copy %s schema", u.Keyspace)
		}
	}

	if err := multierr.Combine(tw.Close(), gw.Close()); err != nil {
		return nil, errors.Wrap(err, "writer close")
	}

	return b, nil
}
