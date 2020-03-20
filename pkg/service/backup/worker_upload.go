// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/util/retry"
	"go.uber.org/multierr"
)

func (w *worker) Upload(ctx context.Context, hosts []hostInfo, limits []DCLimit) (err error) {
	w.Logger.Info(ctx, "Uploading snapshot files...")
	defer func() {
		if err != nil {
			w.Logger.Error(ctx, "Uploading snapshot files failed see exact errors above")
		} else {
			w.Logger.Info(ctx, "Done uploading snapshot files")
		}
	}()

	return inParallelWithLimits(hosts, limits, func(h hostInfo) error {
		w.Logger.Info(ctx, "Uploading snapshot files on host", "host", h.IP)
		err := w.uploadHost(ctx, h)
		if err != nil {
			w.Logger.Error(ctx, "Uploading snapshot files failed on host", "host", h.IP, "error", err)
		} else {
			w.Logger.Info(ctx, "Done uploading snapshot files on host", "host", h.IP)
		}
		return err
	})
}

func (w *worker) uploadHost(ctx context.Context, h hostInfo) error {
	if err := w.setRateLimit(ctx, h); err != nil {
		return errors.Wrap(err, "set rate limit")
	}

	dirs := w.hostSnapshotDirs(h)

	for _, d := range dirs {
		// Check if we should attach to a previous job and wait for it to complete.
		if err := w.attachToJob(ctx, h, d); err != nil {
			return errors.Wrap(err, "attach to the agent job")
		}
		// Start new upload with new job.
		if err := w.uploadSnapshotDir(ctx, h, d); err != nil {
			return errors.Wrap(err, "upload snapshot")
		}
	}
	return nil
}

func (w *worker) attachToJob(ctx context.Context, h hostInfo, d snapshotDir) error {
	if jobID := w.snapshotJobID(ctx, d); jobID != 0 {
		w.Logger.Info(ctx, "Attaching to the previous agent job",
			"host", h.IP,
			"keyspace", d.Keyspace,
			"tag", w.SnapshotTag,
			"job_id", jobID,
		)
		if err := w.waitJob(ctx, jobID, d); err != nil {
			return err
		}
	}
	return nil
}

// snapshotJobID returns the id of the job that was last responsible for
// uploading the snapshot directory.
// If it's not available it will return zero.
func (w *worker) snapshotJobID(ctx context.Context, d snapshotDir) int64 {
	p := d.Progress

	if p.AgentJobID == 0 || p.Size == p.Uploaded {
		return 0
	}

	job, err := w.Client.RcloneJobInfo(ctx, d.Host, p.AgentJobID)
	if err != nil {
		w.Logger.Error(ctx, "Failed to fetch job info",
			"host", d.Host,
			"job_id", p.AgentJobID,
			"error", err,
		)
		return 0
	}

	if s := w.Client.RcloneStatusOfJob(job.Job); s == scyllaclient.JobSuccess || s == scyllaclient.JobRunning {
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
	)
	if err := w.uploadDataDir(ctx, dataDst, dataSrc, d); err != nil {
		return errors.Wrapf(err, "copy %q to %q", dataSrc, dataDst)
	}

	return nil
}

func (w *worker) uploadDataDir(ctx context.Context, dst, src string, d snapshotDir) error {
	// We set longer exponential backoff for upload due to chance that upload
	// might be throttled by storage service providers or temporary
	// network failures can occur.
	b := uploadBackoff()
	notify := func(err error, wait time.Duration) {
		w.Logger.Error(ctx, "Upload failed, retrying", "host", d.Host, "from", src, "to", dst, "error", err, "wait", wait)

		backupUploadRetries.With(prometheus.Labels{
			"cluster":  w.ClusterName,
			"task":     w.TaskID.String(),
			"host":     d.Host,
			"keyspace": d.Keyspace,
		}).Inc()
	}

	return retry.WithNotify(ctx, func() error {
		id, err := w.Client.RcloneCopyDir(ctx, d.Host, dst, src)
		if err != nil {
			return err
		}

		w.Logger.Debug(ctx, "Uploading dir", "host", d.Host, "from", src, "to", dst, "job_id", id)
		d.Progress.AgentJobID = id
		w.onRunProgress(ctx, d.Progress)

		return w.waitJob(ctx, id, d)
	}, b, notify)
}

func uploadBackoff() retry.Backoff {
	initialInterval := 30 * time.Second
	maxElapsedTime := 20 * time.Minute
	maxInterval := 5 * time.Minute
	multiplier := 2.0
	randomFactor := 0.5
	return retry.NewExponentialBackoff(initialInterval, maxElapsedTime, maxInterval, multiplier, randomFactor)
}

func (w *worker) waitJob(ctx context.Context, id int64, d snapshotDir) (err error) {
	defer func() {
		err = multierr.Combine(
			err,
			w.clearJobStats(ctx, id, d.Host),
		)
	}()

	t := time.NewTicker(w.Config.PollInterval)
	defer t.Stop()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			stopCtx := context.Background()
			err := w.Client.RcloneJobStop(stopCtx, d.Host, id)
			if err != nil {
				w.Logger.Error(ctx, "Failed to stop rclone job",
					"host", d.Host,
					"keyspace", d.Keyspace,
					"table", d.Table,
					"job_id", id,
					"error", err,
				)
			}
			job, err := w.Client.RcloneJobInfo(stopCtx, d.Host, id)
			if err != nil {
				w.Logger.Error(ctx, "Failed to fetch job info",
					"host", d.Host,
					"keyspace", d.Keyspace,
					"table", d.Table,
					"job_id", id,
					"error", err,
				)
				return err
			}
			w.updateProgress(stopCtx, d, job)
			return ctx.Err()
		case <-t.C:
			job, err := w.Client.RcloneJobInfo(ctx, d.Host, id)
			if err != nil {
				w.Logger.Error(ctx, "Failed to fetch job info",
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
			switch w.Client.RcloneStatusOfJob(job.Job) {
			case scyllaclient.JobError:
				return errors.Errorf("job error (%d): %s", id, job.Job.Error)
			case scyllaclient.JobSuccess:
				w.updateProgress(ctx, d, job)
				return nil
			case scyllaclient.JobRunning:
				w.updateProgress(ctx, d, job)
			case scyllaclient.JobNotFound:
				return errors.Errorf("job not found (%d)", id)
			}
		}
	}
}

func (w *worker) clearJobStats(ctx context.Context, jobID int64, host string) error {
	w.Logger.Debug(ctx, "Clearing job stats", "host", host, "job_id", jobID)
	return errors.Wrap(w.Client.RcloneDeleteJobStats(ctx, host, jobID), "clear job stats")
}

func (w *worker) updateProgress(ctx context.Context, d snapshotDir, job *scyllaclient.RcloneJobInfo) {
	p := d.Progress

	// Build mapping for files in progress to bytes uploaded
	var transferringBytes = make(map[string]int64, len(job.Stats.Transferring))
	for _, tr := range job.Stats.Transferring {
		transferringBytes[tr.Name] = tr.Bytes
	}

	// Build mapping from file name to transfer entries
	var fileTransfers = make(map[string][]*scyllaclient.RcloneTransfer, len(job.Transferred))
	for _, tr := range job.Transferred {
		fileTransfers[tr.Name] = append(fileTransfers[tr.Name], tr)
	}

	// Clear values
	p.StartedAt = nil
	p.CompletedAt = nil
	p.Error = ""
	p.Uploaded = 0
	p.Skipped = 0
	p.Failed = 0

	// Set StartedAt and CompletedAt based on Job
	if t := time.Time(job.Job.StartTime); !t.IsZero() {
		p.StartedAt = &t
	}
	if t := time.Time(job.Job.EndTime); !t.IsZero() {
		p.CompletedAt = &t
	}

	var errs error
	for _, f := range p.Files {
		ft := fileTransfers[f]

		switch len(ft) {
		case 0:
			// Nothing in transferred so inspect transfers in progress
			p.Uploaded += transferringBytes[f]
		case 1:
			if ft[0].Error != "" {
				p.Failed += ft[0].Size - ft[0].Bytes
				errs = multierr.Append(errs, errors.Errorf("%s %s", f, ft[0].Error))
			}
			if ft[0].Checked {
				// File is already uploaded we just checked.
				p.Skipped += ft[0].Size
			} else {
				p.Uploaded += ft[0].Bytes
			}
		case 2:
			// File is found and updated on remote (check plus transfer).
			// Order Check > Transfer is expected.
			failed := false
			if ft[0].Error != "" {
				failed = true
				errs = multierr.Append(errs, errors.Errorf("%s %s", f, ft[0].Error))
			}
			if ft[1].Error != "" {
				failed = true
				errs = multierr.Append(errs, errors.Errorf("%s %s", f, ft[1].Error))
			}
			if failed {
				p.Failed += ft[1].Size - ft[1].Bytes
			}
			p.Uploaded += ft[1].Bytes
		}
	}

	if errs != nil {
		p.Error = errs.Error()
	}

	w.onRunProgress(ctx, p)
}

func (w *worker) onRunProgress(ctx context.Context, p *RunProgress) {
	if w.OnRunProgress != nil {
		w.OnRunProgress(ctx, p)
	}
}

func (w *worker) remoteManifestFile(h hostInfo, d snapshotDir) string {
	return remoteManifestFile(w.ClusterID, w.TaskID, w.SnapshotTag, h.DC, h.ID, d.Keyspace, d.Table, d.Version)
}

func (w *worker) remoteSSTableDir(h hostInfo, d snapshotDir) string {
	return remoteSSTableVersionDir(w.ClusterID, h.DC, h.ID, d.Keyspace, d.Table, d.Version)
}
