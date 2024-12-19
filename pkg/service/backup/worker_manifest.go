// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"context"
	"net/http"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/cloudmeta"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
)

func (w *worker) UploadManifest(ctx context.Context, hosts []hostInfo) (stepError error) {
	// Limit parallelism level, on huge clusters creating manifest content in
	// memory for all nodes at the same time can lead to memory issues.
	const maxParallel = 12

	f := func(h hostInfo) error {
		w.Logger.Info(ctx, "Uploading manifest file on host", "host", h.IP)

		err := w.createAndUploadHostManifest(ctx, h)
		if err == nil {
			w.Logger.Info(ctx, "Done uploading manifest file on host", "host", h.IP)
		}
		return err
	}

	notify := func(h hostInfo, err error) {
		w.Logger.Error(ctx, "Uploading manifest file failed on host",
			"host", h.IP,
			"error", err,
		)
	}

	return hostsInParallel(hosts, maxParallel, f, notify)
}

func (w *worker) createAndUploadHostManifest(ctx context.Context, h hostInfo) error {
	// Get tokens for manifest
	tokens, err := w.Client.Tokens(ctx, h.IP)
	if err != nil {
		return err
	}

	m, err := w.createTemporaryManifest(ctx, h, tokens)
	if err != nil {
		return errors.Wrap(err, "create temp manifest")
	}
	return w.uploadHostManifest(ctx, h, m)
}

func (w *worker) createTemporaryManifest(ctx context.Context, h hostInfo, tokens []int64) (ManifestInfoWithContent, error) {
	m := &ManifestInfo{
		Location:    h.Location,
		DC:          h.DC,
		ClusterID:   w.ClusterID,
		NodeID:      h.ID,
		TaskID:      w.TaskID,
		SnapshotTag: w.SnapshotTag,
		Temporary:   true,
	}

	dirs := w.hostSnapshotDirs(h)

	c := &ManifestContentWithIndex{
		ManifestContent: ManifestContent{
			Version:     "v2",
			ClusterName: w.ClusterName,
			ClusterID:   w.ClusterID,
			NodeID:      h.ID,
			DC:          h.DC,
			SnapshotTag: w.SnapshotTag,
			TaskID:      w.TaskID,
			IP:          h.IP,
			Tokens:      tokens,
		},
		Index: make([]FilesMeta, len(dirs)),
	}
	if w.SchemaFilePath != "" {
		c.Schema = w.SchemaFilePath
	}

	for i, d := range dirs {
		idx := &c.Index[i]
		idx.Keyspace = d.Keyspace
		idx.Table = d.Table
		idx.Version = d.Version
		idx.Files = make([]string, 0, len(d.Progress.files))
		for _, f := range d.Progress.files {
			idx.Files = append(idx.Files, f.Name)
			idx.Size += f.Size
		}
		c.Size += d.Progress.Size
	}

	rack, err := w.Client.HostRack(ctx, h.IP)
	if err != nil {
		return ManifestInfoWithContent{}, errors.Wrap(err, "client.HostRack")
	}
	c.Rack = rack

	instanceDetails, err := w.manifestInstanceDetails(ctx, h)
	if err != nil {
		return ManifestInfoWithContent{}, errors.Wrap(err, "manifest instance details")
	}
	c.InstanceDetails = instanceDetails

	return ManifestInfoWithContent{
		ManifestInfo:             m,
		ManifestContentWithIndex: c,
	}, nil
}

// manifestInstanceDetails collects node/instance specific information that's needed for 1-to-1 restore.
func (w *worker) manifestInstanceDetails(ctx context.Context, host hostInfo) (InstanceDetails, error) {
	var result InstanceDetails

	shardCound, err := w.Client.ShardCount(ctx, host.IP)
	if err != nil {
		return InstanceDetails{}, errors.Wrap(err, "client.ShardCount")
	}
	result.ShardCount = int(shardCound)

	nodeInfo, err := w.Client.NodeInfo(ctx, host.IP)
	if err != nil {
		return InstanceDetails{}, errors.Wrap(err, "client.NodeInfo")
	}
	result.StorageSize = nodeInfo.StorageSize

	metaSvc, err := cloudmeta.NewCloudMeta(w.Logger)
	if err != nil {
		return InstanceDetails{}, errors.Wrap(err, "new cloud meta svc")
	}

	instanceMeta, err := metaSvc.GetInstanceMetadata(ctx)
	if err != nil {
		// Metadata may not be available for several reasons:
		// 1. running on-premise 2. disabled 3. smth went wrong with metadata server.
		// As we cannot distinguish between these cases, we can only log err and continue with the backup.
		w.Logger.Error(ctx, "Get instance metadata", "err", err)
	}
	result.CloudProvider = string(instanceMeta.CloudProvider)
	result.InstanceType = instanceMeta.InstanceType

	return result, nil
}

func (w *worker) uploadHostManifest(ctx context.Context, h hostInfo, m ManifestInfoWithContent) error {
	// Get memory buffer for gzip compressed output
	buf := w.memoryPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer func() {
		w.memoryPool.Put(buf)
	}()

	// Write to the buffer
	if err := m.Write(buf); err != nil {
		return err
	}

	// Upload compressed manifest
	dst := h.Location.RemotePath(m.Path())
	return w.Client.RclonePut(ctx, h.IP, dst, buf)
}

func (w *worker) MoveManifest(ctx context.Context, hosts []hostInfo) (err error) {
	rollbacks := make([]func(context.Context) error, len(hosts))

	f := func(i int) (hostErr error) {
		h := hosts[i]
		defer func() {
			hostErr = errors.Wrap(hostErr, h.String())
		}()

		w.Logger.Info(ctx, "Moving manifest file on host", "host", h.IP)
		dst := h.Location.RemotePath(RemoteManifestFile(w.ClusterID, w.TaskID, w.SnapshotTag, h.DC, h.ID))
		src := TempFile(dst)

		// Register rollback
		rollbacks[i] = func(ctx context.Context) error { return w.Client.RcloneMoveFile(ctx, h.IP, src, dst) }

		// Try to move
		err := w.Client.RcloneMoveFile(ctx, h.IP, dst, src)

		// Support resuming backups created with the Scylla Manager versions
		// without manifest move logic. In that case the manifest is uploaded
		// upfront.
		if scyllaclient.StatusCodeOf(err) == http.StatusNotFound {
			if e, _ := w.Client.RcloneFileInfo(ctx, h.IP, dst); e == nil { // nolint: errcheck
				w.Logger.Info(ctx, "Detected manifest was already in place", "host", h.IP)
				err = nil
			}
		}

		if err == nil {
			w.Logger.Info(ctx, "Done moving manifest file on host", "host", h.IP)
		}
		return err
	}

	notify := func(i int, err error) {
		h := hosts[i]
		w.Logger.Error(ctx, "Moving manifest file on host",
			"host", h.IP,
			"error", err,
		)
	}

	if err = parallel.Run(len(hosts), parallel.NoLimit, f, notify); err != nil {
		w.rollbackMoveManifest(ctx, hosts, rollbacks)
	}
	return err
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
