// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	stdErr "errors"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func (w *worker) Upload(ctx context.Context, hosts []hostInfo, limits []DCLimit) (err error) {
	f := func(h hostInfo) error {
		w.Logger.Info(ctx, "Uploading snapshot files on host", "host", h.IP)
		err := w.uploadHost(ctx, h)
		if err == nil {
			w.Logger.Info(ctx, "Done uploading snapshot files on host", "host", h.IP)
		}
		return err
	}

	notify := func(h hostInfo, err error) {
		w.Logger.Error(ctx, "Uploading snapshot files failed on host", "host", h.IP, "error", err)
	}

	return inParallelWithLimits(hosts, limits, f, notify)
}

func (w *worker) uploadHost(ctx context.Context, h hostInfo) error {
	rclone, native, err := w.hostSnapshotDirsByMethod(ctx, h)
	if err != nil {
		return errors.Wrap(err, "ensure backup method")
	}

	var retErr error
	if len(native) > 0 {
		err := w.uploadSnapshotDirsInParallel(ctx, h, native, w.nativeBackup, parallel.NoLimit)
		retErr = stdErr.Join(retErr, err)
	}
	if len(rclone) > 0 {
		err := w.uploadSnapshotDirsInParallel(ctx, h, rclone, w.rcloneBackup, 1)
		retErr = stdErr.Join(retErr, err)
	}
	return retErr
}

func (w *worker) hostSnapshotDirsByMethod(ctx context.Context, h hostInfo) (rclone, native []snapshotDir, err error) {
	dirs := w.hostSnapshotDirs(h)

	if w.Method == methodRclone {
		return dirs, nil, nil
	}

	// Handle lack of native backup support on the host level
	if err := w.hostNativeBackupSupport(ctx, h.IP, h.NodeConfig.NodeInfo, h.Location); err != nil {
		if w.Method == methodNative {
			return nil, nil, err
		}
		return dirs, nil, nil
	}

	if w.Method == methodNative {
		for _, d := range dirs {
			if err := w.snapshotDirNativeBackupSupport(ctx, h.IP, d); err != nil {
				return nil, nil, errors.Wrapf(err, "%s.%s: ensure native backup support", d.Keyspace, d.Table)
			}
		}
		return nil, dirs, nil
	}

	if w.Method == methodAuto {
		for _, d := range dirs {
			if err := w.snapshotDirNativeBackupSupport(ctx, h.IP, d); err != nil {
				rclone = append(rclone, d)
			} else {
				native = append(native, d)
			}
		}
		return rclone, native, nil
	}

	return nil, nil, errors.New("unknown method: " + string(w.Method))
}

type uploadSnapshotDirFunc func(ctx context.Context, h hostInfo, d snapshotDir) error

func (w *worker) uploadSnapshotDirsInParallel(ctx context.Context, h hostInfo, dirs []snapshotDir, f uploadSnapshotDirFunc, limit int) error {
	return parallel.Run(len(dirs), limit,
		func(i int) error {
			return w.uploadSnapshotDirWrapper(ctx, h, dirs[i], f)
		},
		func(i int, err error) {
			w.Logger.Error(ctx, "Failed to upload host", "host", dirs[i].Host, "error", err)
		})
}

func (w *worker) uploadSnapshotDirWrapper(ctx context.Context, h hostInfo, d snapshotDir, f uploadSnapshotDirFunc) error {
	// Skip snapshots that are empty.
	if d.Progress.Size == 0 {
		w.Logger.Info(ctx, "Table is empty skipping", "host", h.IP, "keyspace", d.Keyspace, "table", d.Table)
		now := timeutc.Now()
		d.Progress.StartedAt = &now
		d.Progress.CompletedAt = &now
		w.onRunProgress(ctx, d.Progress)
		return nil
	}
	// Skip snapshots that are already uploaded.
	if d.Progress.IsUploaded() {
		w.Logger.Info(ctx, "Snapshot already uploaded skipping", "host", h.IP, "keyspace", d.Keyspace, "table", d.Table)
		return nil
	}

	err := f(ctx, h, d)
	// Delete the snapshot only if it was successfully uploaded.
	// Otherwise, we want to continue the upload on retry/resume.
	if err == nil {
		err = errors.Wrap(w.deleteTableSnapshot(ctx, h, d), "delete table snapshot")
	}
	if scheduler.IsTaskInterrupted(ctx) {
		err = parallel.Abort(err)
	}
	return errors.Wrapf(err, "%s.%s", d.Keyspace, d.Table)
}

func (w *worker) rcloneBackup(ctx context.Context, h hostInfo, d snapshotDir) error {
	w.Logger.Info(ctx, "Use rclone movedir api",
		"host", h.IP,
		"keyspace", d.Keyspace,
		"table", d.Table)
	// Check if we should attach to a previous job and wait for it to complete.
	if attached, err := w.attachToJob(ctx, h, d); err != nil {
		return errors.Wrap(err, "attach to the agent job")
	} else if attached {
		return nil
	}
	// Start new upload with new job.
	if err := w.uploadSnapshotDir(ctx, h, d); err != nil {
		return errors.Wrap(err, "upload snapshot")
	}
	return nil
}

// attachToJob returns true if previous job was found and wait procedure was
// initiated.
// Caller needs to check for error if true is returned otherwise it can be
// ignored.
func (w *worker) attachToJob(ctx context.Context, h hostInfo, d snapshotDir) (bool, error) {
	jobID := w.snapshotJobID(ctx, d)
	if jobID == 0 {
		return false, nil
	}
	w.Logger.Info(ctx, "Attaching to the previous agent job",
		"host", h.IP,
		"snapshot_tag", w.SnapshotTag,
		"keyspace", d.Keyspace,
		"job_id", jobID,
	)
	err := w.waitJob(ctx, jobID, d)
	if errors.Is(err, errJobNotFound) {
		return false, nil
	}
	return true, err
}

// snapshotJobID returns the id of the job that was last responsible for
// uploading the snapshot directory.
// If it's not available it will return zero.
func (w *worker) snapshotJobID(ctx context.Context, d snapshotDir) int64 {
	p := d.Progress

	if p.AgentJobID == 0 {
		return 0
	}

	job, err := w.Client.RcloneJobProgress(ctx, d.Host, p.AgentJobID, w.Config.LongPollingTimeoutSeconds)
	if err != nil {
		w.Logger.Error(ctx, "Failed to fetch job info",
			"host", d.Host,
			"job_id", p.AgentJobID,
			"error", err,
		)
		return 0
	}

	if scyllaclient.WorthWaitingForJob(job.Status) {
		return p.AgentJobID
	}

	return 0
}

func (w *worker) uploadSnapshotDir(ctx context.Context, h hostInfo, d snapshotDir) error {
	w.Logger.Info(ctx, "Uploading table snapshot",
		"host", h.IP,
		"keyspace", d.Keyspace,
		"table", d.Table,
		"location", h.Location,
	)

	// Upload sstables
	var (
		sstablesPath = w.remoteSSTableDir(h, d)
		dataDst      = h.Location.RemotePath(sstablesPath)
		dataSrc      = d.Path
		retries      = 10
	)
	for range retries {
		if err := w.uploadDataDir(ctx, h, dataDst, dataSrc, d); err != nil {
			if errors.Is(err, errJobNotFound) {
				continue
			}
			return errors.Wrapf(err, "copy %q to %q", dataSrc, dataDst)
		}
		break
	}

	return nil
}

func (w *worker) uploadDataDir(ctx context.Context, hi hostInfo, dst, src string, d snapshotDir) error {
	// Ensure file versioning during upload
	id, err := w.Client.RcloneMoveDir(ctx, d.Host, hi.Transfers, hi.RateLimit.Limit, dst, src, VersionedFileExt(w.SnapshotTag))
	if err != nil {
		return err
	}

	w.Logger.Info(ctx, "Uploading dir", "host", d.Host, "from", src, "to", dst, "job_id", id)
	d.Progress.AgentJobID = id
	w.onRunProgress(ctx, d.Progress)

	if err := w.waitJob(ctx, id, d); err != nil {
		w.Logger.Error(ctx, "Upload dir failed", "host", d.Host, "from", src, "to", dst, "error", err)
		return err
	}
	return nil
}

var errJobNotFound = errors.New("job not found")

func (w *worker) waitJob(ctx context.Context, id int64, d snapshotDir) (err error) {
	defer func() {
		// Running stop procedure in a different context because original may be canceled
		stopCtx := context.Background()

		// On error stop job
		if err != nil {
			w.Logger.Info(ctx, "Stop job", "host", d.Host, "id", id)
			if e := w.Client.RcloneJobStop(stopCtx, d.Host, id); e != nil {
				w.Logger.Error(ctx, "Failed to stop job",
					"host", d.Host,
					"id", id,
					"error", e,
				)
			}
		}

		// On exit clear stats
		if e := w.clearJobStats(stopCtx, id, d.Host); e != nil {
			w.Logger.Error(ctx, "Failed to clear job stats",
				"host", d.Host,
				"id", id,
				"error", e,
			)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			job, err := w.Client.RcloneJobProgress(ctx, d.Host, id, w.Config.LongPollingTimeoutSeconds)
			if err != nil {
				return errors.Wrap(err, "fetch job info")
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			switch scyllaclient.RcloneJobStatus(job.Status) {
			case scyllaclient.JobError:
				return errors.Errorf("job error (%d): %s", id, job.Error)
			case scyllaclient.JobSuccess:
				w.updateProgress(ctx, d, job)
				return nil
			case scyllaclient.JobRunning:
				w.updateProgress(ctx, d, job)
			case scyllaclient.JobNotFound:
				return errJobNotFound
			}
		}
	}
}

func (w *worker) deleteTableSnapshot(ctx context.Context, h hostInfo, d snapshotDir) error {
	w.Logger.Debug(ctx, "Removing table snapshot",
		"host", h.IP,
		"keyspace", d.Keyspace,
		"table", d.Table,
		"location", h.Location,
	)
	return w.Client.DeleteTableSnapshot(ctx, d.Host, w.SnapshotTag, d.Keyspace, d.Table)
}

func (w *workerTools) clearJobStats(ctx context.Context, jobID int64, host string) error {
	w.Logger.Debug(ctx, "Clearing job stats", "host", host, "job_id", jobID)
	return errors.Wrap(w.Client.RcloneDeleteJobStats(ctx, host, jobID), "clear job stats")
}

func (w *worker) updateProgress(ctx context.Context, d snapshotDir, job *scyllaclient.RcloneJobProgress) {
	p := d.Progress

	p.StartedAt = nil
	// Set StartedAt and CompletedAt based on Job
	if t := time.Time(job.StartedAt); !t.IsZero() {
		p.StartedAt = &t
	}
	p.CompletedAt = nil
	if t := time.Time(job.CompletedAt); !t.IsZero() {
		p.CompletedAt = &t
	}

	p.Error = job.Error
	p.Uploaded = job.Uploaded
	p.Skipped = d.SkippedBytesOffset + job.Skipped
	p.Failed = job.Failed

	w.onRunProgress(ctx, p)
}

func (w *worker) onRunProgress(ctx context.Context, p *RunProgress) {
	w.Metrics.SetFilesProgress(w.ClusterID, w.Units[p.Unit].Keyspace, p.TableName, p.Host,
		p.Size, p.Uploaded, p.Skipped, p.Failed)

	if w.OnRunProgress != nil {
		w.OnRunProgress(ctx, p)
	}
}

func (w *worker) remoteSSTableDir(h hostInfo, d snapshotDir) string {
	return backupspec.RemoteSSTableVersionDir(w.ClusterID, h.DC, h.ID, d.Keyspace, d.Table, d.Version)
}
