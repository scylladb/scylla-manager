// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"go.uber.org/multierr"
)

func (w *worker) Upload(ctx context.Context, hosts []hostInfo, limits []DCLimit) (err error) {
	w.Logger.Info(ctx, "Uploading snapshot files...")
	defer func(start time.Time) {
		if err != nil {
			w.Logger.Error(ctx, "Uploading snapshot files failed see exact errors above", "duration", timeutc.Since(start))
		} else {
			w.Logger.Info(ctx, "Done uploading snapshot files", "duration", timeutc.Since(start))
		}
	}(timeutc.Now())

	return inParallelWithLimits(hosts, limits, func(h hostInfo) error {
		w.Logger.Info(ctx, "Uploading snapshot files on host", "host", h.IP)
		if err := w.uploadHost(ctx, h); err != nil {
			w.Logger.Error(ctx, "Uploading snapshot files failed on host", "host", h.IP, "error", err)
			return err
		}

		w.Logger.Info(ctx, "Done uploading snapshot files on host", "host", h.IP)
		return nil
	})
}

func (w *worker) uploadHost(ctx context.Context, h hostInfo) error {
	if err := w.setRateLimit(ctx, h); err != nil {
		return errors.Wrap(err, "set rate limit")
	}

	dirs := w.hostSnapshotDirs(h)

	for _, d := range dirs {
		// Skip snapshots that are already uploaded.
		if d.Progress.IsUploaded() {
			w.Logger.Info(ctx, "Snapshot already uploaded skipping", "host", h.IP, "keyspace", d.Keyspace, "table", d.Table)
			continue
		}
		// Check if we should attach to a previous job and wait for it to complete.
		if attached, err := w.attachToJob(ctx, h, d); err != nil {
			return errors.Wrap(err, "attach to the agent job")
		} else if attached {
			continue
		}
		// Start new upload with new job.
		if err := w.uploadSnapshotDir(ctx, h, d); err != nil {
			return errors.Wrap(err, "upload snapshot")
		}
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

	if job.Status == string(scyllaclient.JobSuccess) || job.Status == string(scyllaclient.JobRunning) {
		return p.AgentJobID
	}

	return 0
}

func (w *worker) setRateLimit(ctx context.Context, h hostInfo) error {
	w.Logger.Info(ctx, "Setting rate limit", "host", h.IP, "limit", h.RateLimit.Limit)
	return w.Client.RcloneSetBandwidthLimit(ctx, h.IP, h.RateLimit.Limit)
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
	for i := 0; i < retries; i++ {
		if err := w.uploadDataDir(ctx, dataDst, dataSrc, d); err != nil {
			if errors.Is(err, errJobNotFound) {
				continue
			}
			return errors.Wrapf(err, "copy %q to %q", dataSrc, dataDst)
		}
		break
	}

	return nil
}

func (w *worker) uploadDataDir(ctx context.Context, dst, src string, d snapshotDir) error {
	id, err := w.Client.RcloneCopyDir(ctx, d.Host, dst, src)
	if err != nil {
		return err
	}

	w.Logger.Debug(ctx, "Uploading dir", "host", d.Host, "from", src, "to", dst, "job_id", id)
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
		err = multierr.Combine(
			err,
			w.clearJobStats(ctx, id, d.Host),
		)
	}()

	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		select {
		case <-waitCtx.Done():
			// Running stop procedure in different context because original
			// is canceled.
			stopCtx := context.Background()

			err := w.Client.RcloneJobStop(stopCtx, d.Host, id)
			if err != nil {
				w.Logger.Error(waitCtx, "Failed to stop rclone job",
					"host", d.Host,
					"keyspace", d.Keyspace,
					"table", d.Table,
					"job_id", id,
					"error", err,
				)
			}
			job, err := w.Client.RcloneJobProgress(stopCtx, d.Host, id, w.Config.LongPollingTimeoutSeconds)
			if err != nil {
				w.Logger.Error(waitCtx, "Failed to fetch job info",
					"host", d.Host,
					"keyspace", d.Keyspace,
					"table", d.Table,
					"job_id", id,
					"error", err,
				)
				return err
			}
			w.updateProgress(stopCtx, d, job)
			return waitCtx.Err()
		default:
			job, err := w.Client.RcloneJobProgress(waitCtx, d.Host, id, w.Config.LongPollingTimeoutSeconds)
			if err != nil {
				w.Logger.Error(waitCtx, "Failed to fetch job info",
					"host", d.Host,
					"keyspace", d.Keyspace,
					"table", d.Table,
					"job_id", id,
					"error", err,
				)
				// In case of error execute cancel procedure
				cancel()
				continue
			}
			switch scyllaclient.RcloneJobStatus(job.Status) {
			case scyllaclient.JobError:
				return errors.Errorf("job error (%d): %s", id, job.Error)
			case scyllaclient.JobSuccess:
				w.updateProgress(waitCtx, d, job)
				return nil
			case scyllaclient.JobRunning:
				w.updateProgress(waitCtx, d, job)
			case scyllaclient.JobNotFound:
				return errJobNotFound
			}
		}
	}
}

func (w *worker) clearJobStats(ctx context.Context, jobID int64, host string) error {
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
	p.Skipped = job.Skipped
	p.Failed = job.Failed

	w.onRunProgress(ctx, p)
}

func (w *worker) onRunProgress(ctx context.Context, p *RunProgress) {
	if w.OnRunProgress != nil {
		w.OnRunProgress(ctx, p)
	}
}

func (w *worker) remoteSSTableDir(h hostInfo, d snapshotDir) string {
	return remoteSSTableVersionDir(w.ClusterID, h.DC, h.ID, d.Keyspace, d.Table, d.Version)
}
