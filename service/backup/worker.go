// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/multierr"
)

type worker struct {
	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	config Config
	units  []Unit
	client *scyllaclient.Client
	logger log.Logger
}

type hostInfo struct {
	IP        string
	ID        string
	Location  Location
	RateLimit RateLimit
}

func (w *worker) Exec(ctx context.Context, hosts []hostInfo) error {
	return w.inParallel(hosts, func(h hostInfo) error {
		return w.execHost(ctx, h)
	})
}

func (w *worker) inParallel(hosts []hostInfo, f func(h hostInfo) error) error {
	out := make(chan error)
	for _, h := range hosts {
		h := h
		go func() {
			out <- errors.Wrapf(f(h), "host %s", h)
		}()
	}

	var errs error
	for range hosts {
		errs = multierr.Append(errs, <-out)
	}
	return errs
}

func (w *worker) execHost(ctx context.Context, h hostInfo) error {
	if err := w.checkAvailableDiskSpace(ctx, h); err != nil {
		return errors.Wrap(err, "disk space check")
	}
	if err := w.takeSnapshot(ctx, h); err != nil {
		return errors.Wrap(err, "failed to take snapshot")
	}
	if err := w.deleteOldSnapshots(ctx, h); err != nil {
		// Not a fatal error we can continue, just log the error
		w.logger.Error(ctx, "Failed to delete old snapshots", "error", err)
	}
	if err := w.register(ctx, h); err != nil {
		return errors.Wrap(err, "failed to register remote")
	}
	if err := w.setRateLimit(ctx, h); err != nil {
		return errors.Wrap(err, "failed to set rate limit")
	}

	dirs, err := w.findSnapshotDirs(ctx, h)
	if err != nil {
		return errors.Wrap(err, "failed to list snapshot dirs")
	}
	for _, d := range dirs {
		if err := w.uploadSnapshotDir(ctx, h, d); err != nil {
			return errors.Wrap(err, "failed to upload snapshot")
		}
	}

	return nil
}

func (w *worker) checkAvailableDiskSpace(ctx context.Context, h hostInfo) error {
	freePercent, err := w.diskFreePercent(ctx, h)
	if err != nil {
		return err
	}
	w.logger.Info(ctx, "Available disk space", "host", h.IP, "percent", freePercent)
	if freePercent < w.config.DiskSpaceFreeMinPercent {
		return errors.New("not enough disk space")
	}
	return nil
}

func (w *worker) diskFreePercent(ctx context.Context, h hostInfo) (int, error) {
	du, err := w.client.RcloneDiskUsage(ctx, h.IP, dataDir)
	if err != nil {
		return 0, err
	}
	return int(100 * (float64(du.Free) / float64(du.Total))), nil
}

func (w *worker) takeSnapshot(ctx context.Context, h hostInfo) error {
	for _, u := range w.units {
		w.logger.Info(ctx, "Taking snapshot", "host", h.IP, "keyspace", u.Keyspace, "tag", w.snapshotTag())
		if err := w.client.TakeSnapshot(ctx, h.IP, w.snapshotTag(), u.Keyspace, u.Tables...); err != nil {
			return errors.Wrapf(err, "keyspace %s: snapshot failed", u.Keyspace)
		}
	}
	return nil
}

func (w *worker) deleteOldSnapshots(ctx context.Context, h hostInfo) error {
	tags, err := w.client.Snapshots(ctx, h.IP)
	if err != nil {
		return err
	}

	for _, t := range tags {
		if claimTag(t) && t != w.snapshotTag() {
			w.logger.Info(ctx, "Deleting snapshot", "host", h.IP, "tag", t)
			if err := w.client.DeleteSnapshot(ctx, h.IP, t); err != nil {
				return err
			}
		}
	}

	return nil
}

// snapshotDir represents a remote directory containing a table snapshot.
type snapshotDir struct {
	Path     string
	Keyspace string
	Table    string
	Version  string
	Size     int64
}

func (w *worker) findSnapshotDirs(ctx context.Context, h hostInfo) ([]snapshotDir, error) {
	var dirs []snapshotDir

	r := regexp.MustCompile("^([A-Za-z0-9_]+)-([a-f0-9]{32})/snapshots/" + w.snapshotTag() + "$")

	for _, u := range w.units {
		w.logger.Debug(ctx, "Inspecting snapshot", "host", h.IP, "keyspace", u.Keyspace, "tag", w.snapshotTag())

		list, err := w.client.RcloneListDir(ctx, h.IP, keyspaceDir(u.Keyspace), true)
		if err != nil {
			return nil, err
		}
		curPath := "/"
		for _, f := range list {
			// Accumulate size of all files in a snapshot directory
			if !f.IsDir && strings.HasPrefix(f.Path, curPath) {
				dirs[len(dirs)-1].Size += f.Size
				continue
			}
			// Match snapshot directories
			m := r.FindStringSubmatch(f.Path)
			if m == nil {
				continue
			}
			dirs = append(dirs, snapshotDir{
				Path:     f.Path,
				Keyspace: u.Keyspace,
				Table:    m[1],
				Version:  m[2],
			})
			curPath = f.Path
		}
	}

	return dirs, nil
}

func (w *worker) register(ctx context.Context, h hostInfo) error {
	w.logger.Info(ctx, "Registering remote", "host", h.IP, "location", h.Location)

	if h.Location.Provider != S3 {
		return errors.Errorf("unsupported provider %s", h.Location.Provider)
	}

	p := scyllaclient.S3Params{
		EnvAuth:         true,
		DisableChecksum: true,

		Endpoint: w.config.TestS3Endpoint,
	}
	return w.client.RcloneRegisterS3Remote(ctx, h.IP, h.Location.RemoteName(), p)
}

func (w *worker) setRateLimit(ctx context.Context, h hostInfo) error {
	w.logger.Info(ctx, "Setting rate limit", "host", h.IP, "limit", h.RateLimit.Limit)
	return w.client.RcloneSetBandwidthLimit(ctx, h.IP, h.RateLimit.Limit)
}

const manifestFile = "manifest.json"

func (w *worker) uploadSnapshotDir(ctx context.Context, h hostInfo, d snapshotDir) error {
	w.logger.Info(ctx, "Uploading",
		"host", h.IP,
		"keyspace", d.Keyspace,
		"table", d.Table,
		"location", h.Location,
	)

	// Upload manifest
	var (
		manifestDst = path.Join(h.Location.RemotePath(w.remoteMetaDir(h, d)), manifestFile)
		manifestSrc = path.Join(dataDir, d.Keyspace, d.Path, manifestFile)
	)
	if err := w.uploadFile(ctx, h.IP, manifestDst, manifestSrc); err != nil {
		return errors.Wrapf(err, "host %s: failed to copy %s to %s", h, manifestSrc, manifestDst)
	}

	// Upload sstables
	var (
		dataDst = h.Location.RemotePath(w.remoteSSTableDir(h, d))
		dataSrc = path.Join(dataDir, d.Keyspace, d.Path)
	)
	if err := w.uploadDir(ctx, h.IP, dataDst, dataSrc); err != nil {
		return errors.Wrapf(err, "host %s: failed to copy %s to %s", h, dataSrc, dataDst)
	}

	return nil
}

func (w *worker) uploadFile(ctx context.Context, ip, dst, src string) error {
	w.logger.Debug(ctx, "Uploading file", "host", ip, "from", src, "to", dst)
	id, err := w.client.RcloneCopyFile(ctx, ip, dst, src)
	if err != nil {
		return err
	}
	return w.waitJob(ctx, ip, id)
}

func (w *worker) uploadDir(ctx context.Context, ip, dst, src string) error {
	w.logger.Debug(ctx, "Uploading dir", "host", ip, "from", src, "to", dst)
	id, err := w.client.RcloneCopyDir(ctx, ip, dst, src, manifestFile)
	if err != nil {
		return err
	}
	return w.waitJob(ctx, ip, id)
}

func (w *worker) waitJob(ctx context.Context, ip string, id uuid.UUID) error {
	t := time.NewTicker(w.config.PollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			err := w.client.RcloneJobStop(context.Background(), ip, id)
			if err != nil {
				w.logger.Error(ctx, "Failed to stop rclone job",
					"error", err,
					"host", ip,
					"jobid", id)
			}
			return ctx.Err()
		case <-t.C:
			s, err := w.client.RcloneJobStatus(ctx, ip, id)
			if err != nil {
				return err
			}
			if s.Finished {
				if s.Success {
					return nil
				}
				return errors.New(s.Error)
			}
		}
	}
}

func (w *worker) remoteMetaDir(h hostInfo, d snapshotDir) string {
	return path.Join(
		"backup",
		"cluster",
		w.clusterID.String(),
		"node",
		h.ID,
		"keyspace",
		d.Keyspace,
		"task",
		w.taskID.String(),
		"run",
		w.runID.String(),
		"table",
		d.Table,
		d.Version,
	)
}

func (w *worker) remoteSSTableDir(h hostInfo, d snapshotDir) string {
	return path.Join(
		"backup",
		"cluster",
		w.clusterID.String(),
		"node",
		h.ID,
		"keyspace",
		d.Keyspace,
		"sst",
		d.Table,
		d.Version,
	)
}

func (w *worker) snapshotTag() string {
	return snapshotTag(w.runID)
}

const dataDir = "/var/lib/scylla/data"

func keyspaceDir(keyspace string) string {
	return dataDir + "/" + keyspace
}

func snapshotTag(id uuid.UUID) string {
	return "sm_" + id.String()
}

func claimTag(tag string) bool {
	return strings.HasPrefix(tag, "sm_")
}
