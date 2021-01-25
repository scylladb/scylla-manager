// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
)

func (w *worker) UploadManifest(ctx context.Context, hosts []hostInfo, limits []DCLimit) (stepError error) {
	w.Logger.Info(ctx, "Uploading manifest files...")
	defer func(start time.Time) {
		if stepError != nil {
			w.Logger.Error(ctx, "Uploading manifest files failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done uploading manifest files", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	rollbacks := make([]func(context.Context) error, 0, len(hosts))
	rollbacksMu := sync.Mutex{}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := inParallelWithLimits(hosts, limits, func(h hostInfo) (hostErr error) {
		defer func() {
			// Fail fast in case of any errors
			if hostErr != nil {
				cancel()
				w.Logger.Error(ctx, "Uploading manifest failed", "host", h.IP, "error", hostErr)
				hostErr = parallel.Abort(hostErr)
			}
		}()

		// Get tokens
		tokens, err := w.Client.Tokens(ctx, h.IP)
		if err != nil {
			return err
		}

		// Create manifest
		m := w.createHostManifest(ctx, h, tokens)
		r, err := w.uploadHostManifest(workerCtx, h, m)
		if err != nil {
			return err
		}

		// Register rollback
		rollbacksMu.Lock()
		rollbacks = append(rollbacks, r)
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

func (w *worker) createHostManifest(ctx context.Context, h hostInfo, tokens []int64) *remoteManifest {
	w.Logger.Info(ctx, "Creating manifest files on host", "host", h.IP)

	dirs := w.hostSnapshotDirs(h)

	content := manifestContent{
		Version:     "v2",
		ClusterName: w.ClusterName,
		IP:          h.IP,
		Index:       make([]filesInfo, len(dirs)),
		Tokens:      tokens,
	}
	if w.SchemaUploaded {
		content.Schema = remoteSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag)
	}

	for i, d := range dirs {
		idx := &content.Index[i]
		idx.Keyspace = d.Keyspace
		idx.Table = d.Table
		idx.Version = d.Version
		idx.Files = make([]string, 0, len(d.Progress.files))
		for _, f := range d.Progress.files {
			idx.Files = append(idx.Files, f.Name)
			idx.Size += f.Size
		}
		content.Size += d.Progress.Size
	}

	m := &remoteManifest{
		Location:    h.Location,
		DC:          h.DC,
		ClusterID:   w.ClusterID,
		NodeID:      h.ID,
		TaskID:      w.TaskID,
		SnapshotTag: w.SnapshotTag,
		Content:     content,
	}

	w.Logger.Info(ctx, "Done creating manifest file on host", "host", h.IP)
	return m
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

	manifestDst := h.Location.RemotePath(m.TempRemoteManifestFile())
	if err := w.Client.RclonePut(ctx, h.IP, manifestDst, buf, int64(buf.Len())); err != nil {
		return nil, err
	}

	w.Logger.Info(ctx, "Done uploading manifest file on host", "host", h.IP)
	return func(ctx context.Context) error {
		return w.deleteHostFile(ctx, h.IP, manifestDst)
	}, nil
}

func (w *worker) deleteHostFile(ctx context.Context, host, path string) error {
	w.Logger.Debug(ctx, "Deleting file", "path", path, "host", host)
	return w.Client.RcloneDeleteFile(ctx, host, path)
}

func (w *worker) MoveManifest(ctx context.Context, hosts []hostInfo) (err error) {
	w.Logger.Info(ctx, "Moving manifest files...")
	defer func(start time.Time) {
		if err != nil {
			w.Logger.Error(ctx, "Moving manifest files failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done moving manifest files", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	rollbacks := make([]func(context.Context) error, len(hosts))

	if err = parallel.Run(len(hosts), parallel.NoLimit, func(i int) (hostErr error) {
		h := hosts[i]
		defer func() {
			hostErr = errors.Wrap(hostErr, h.String())
		}()

		w.Logger.Info(ctx, "Moving manifest file on host", "host", h.IP)
		dst := h.Location.RemotePath(remoteManifestFile(w.ClusterID, w.TaskID, w.SnapshotTag, h.DC, h.ID))
		src := tempFile(dst)

		// Register rollback
		rollbacks[i] = func(ctx context.Context) error { return w.Client.RcloneMoveFile(ctx, h.IP, src, dst) }

		// Try to move
		err := w.Client.RcloneMoveFile(ctx, h.IP, dst, src)
		if err != nil {
			w.Logger.Error(ctx, "Moving manifest file on host", "host", h.IP, "error", err)
		} else {
			w.Logger.Info(ctx, "Done moving manifest file on host", "host", h.IP)
		}

		return err
	}); err != nil {
		w.rollbackMoveManifest(ctx, hosts, rollbacks)
		return err
	}

	return nil
}

func (w *worker) rollbackMoveManifest(ctx context.Context, hosts []hostInfo, rollbacks []func(context.Context) error) {
	w.Logger.Info(ctx, "Rolling back manifest files move")
	for i := range rollbacks {
		// Parent context might be already canceled, use background context.
		// Request timeout is configured on transport layer.
		if rollbacks[i] != nil {
			if err := rollbacks[i](context.Background()); err != nil {
				if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
					continue
				}
				w.Logger.Info(ctx, "Cannot rollback manifest move", "host", hosts[i].IP, "error", err)
			}
		}
	}
}
