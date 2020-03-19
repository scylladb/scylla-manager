// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/scylladb/mermaid/pkg/util/parallel"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
)

func (w *worker) AggregateManifests(ctx context.Context, hosts []hostInfo, limits []DCLimit) (stepError error) {
	w.Logger.Info(ctx, "Aggregating manifest files...")
	defer func(start time.Time) {
		if stepError != nil {
			w.Logger.Error(ctx, "Aggregating manifest files failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done aggregating manifest files", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	rollbacks := make([]func(context.Context) error, 0, len(hosts))
	rollbacksMu := sync.Mutex{}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := inParallelWithLimits(hosts, limits, func(h hostInfo) error {
		m := w.aggregateHostManifest(workerCtx, h)

		rollback, err := w.uploadHostManifest(workerCtx, h, m)
		if err != nil {
			// Fail fast in case of any errors
			cancel()
			w.Logger.Error(ctx, "Uploading aggregated manifest file failed", "host", h.IP, "error", err)
			return parallel.Abort(err)
		}
		rollbacksMu.Lock()
		rollbacks = append(rollbacks, rollback)
		rollbacksMu.Unlock()

		return nil
	})
	if err != nil {
		for i := range rollbacks {
			// Parent context might be already canceled, use background context.
			// Request timeout is configured on transport layer.
			if rollbacks[i] != nil {
				if err := rollbacks[i](context.Background()); err != nil {
					w.Logger.Error(ctx, "Cannot rollback manifest upload", "error", err)
				}
			}
		}
		return err
	}

	return nil
}

func (w *worker) aggregateHostManifest(ctx context.Context, h hostInfo) *remoteManifest {
	w.Logger.Info(ctx, "Aggregating manifest files on host", "host", h.IP)

	dirs := w.hostSnapshotDirs(h)

	m := &remoteManifest{
		Location:    h.Location,
		DC:          h.DC,
		ClusterID:   w.ClusterID,
		NodeID:      h.ID,
		TaskID:      w.TaskID,
		SnapshotTag: w.SnapshotTag,
		Content: manifestContent{
			Version: "v2",
			Index:   make([]filesInfo, len(dirs)),
		},
	}

	w.transformSnapshotIndexIntoManifest(dirs, m)

	w.Logger.Info(ctx, "Done aggregating manifest file on host", "host", h.IP)
	return m
}

func (w *worker) transformSnapshotIndexIntoManifest(dirs []snapshotDir, m *remoteManifest) {
	for i, d := range dirs {
		idx := &m.Content.Index[i]
		idx.Keyspace = d.Keyspace
		idx.Table = d.Table
		idx.Version = d.Version
		idx.Files = d.Progress.files
		m.Content.Size += d.Progress.Size
	}
}

func (w *worker) uploadHostManifest(ctx context.Context, h hostInfo, m *remoteManifest) (rollback func(context.Context) error, err error) {
	w.Logger.Info(ctx, "Uploading manifest file on host", "host", h.IP)

	buf := w.memoryPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer func() {
		w.memoryPool.Put(buf)
	}()

	if err := m.DumpContent(buf); err != nil {
		return nil, err
	}

	manifestDst := h.Location.RemotePath(remoteManifestFile(m.ClusterID, m.TaskID, m.SnapshotTag, h.DC, h.ID))
	if err := w.Client.RclonePut(ctx, h.IP, manifestDst, buf, int64(buf.Len())); err != nil {
		return nil, err
	}

	w.Logger.Info(ctx, "Done uploading manifest file on host", "host", h.IP)
	return func(ctx context.Context) error {
		return w.deleteHostManifest(ctx, h.IP, manifestDst)
	}, nil
}

func (w *worker) deleteHostManifest(ctx context.Context, host, manifestPath string) error {
	w.Logger.Error(ctx, "Deleting manifest", "path", manifestPath, "host", host)
	return w.Client.RcloneDeleteFile(ctx, host, manifestPath)
}
