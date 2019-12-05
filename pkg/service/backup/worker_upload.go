// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"go.uber.org/multierr"
)

func (w *worker) Upload(ctx context.Context, hosts []hostInfo, limits []DCLimit) (err error) {
	w.Logger.Info(ctx, "Starting upload procedure")
	defer func() {
		if err != nil {
			w.Logger.Error(ctx, "Upload procedure completed with error(s) see exact errors above")
		} else {
			w.Logger.Info(ctx, "Upload procedure completed")
		}
	}()

	return inParallelWithLimits(hosts, limits, func(h hostInfo) error {
		w.Logger.Info(ctx, "Executing upload procedure on host", "host", h.IP)
		err := w.uploadHost(ctx, h)
		if err != nil {
			w.Logger.Error(ctx, "Upload procedure failed on host", "host", h.IP, "error", err)
		} else {
			w.Logger.Info(ctx, "Done executing upload procedure on host", "host", h.IP)
		}
		return err
	})
}

func (w *worker) uploadHost(ctx context.Context, h hostInfo) error {
	if err := w.setRateLimit(ctx, h); err != nil {
		return errors.Wrap(err, "set rate limit")
	}

	dirs := w.hostSnapshotDirs(h)
	if len(dirs) == 0 {
		var err error
		dirs, err = w.findSnapshotDirs(ctx, h)
		if err != nil {
			return errors.Wrap(err, "list snapshot dirs")
		}
	}

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
// If it's not available it will return uuid.Nil
func (w *worker) snapshotJobID(ctx context.Context, d snapshotDir) int64 {
	for _, p := range d.Progress {
		if p.AgentJobID == 0 || p.Size == p.Uploaded {
			continue
		}
		status, _ := w.getJobStatus(ctx, p.AgentJobID, d) //nolint:errcheck
		switch status {
		case jobError:
			return 0
		case jobNotFound:
			return 0
		case jobSuccess:
			return p.AgentJobID
		case jobRunning:
			return p.AgentJobID
		}
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
	id, err := w.Client.RcloneCopyDir(ctx, d.Host, dst, src)
	if err != nil {
		return err
	}

	w.Logger.Debug(ctx, "Uploading dir", "host", d.Host, "from", src, "to", dst, "job_id", id)
	for _, p := range d.Progress {
		if p.FileName != manifest {
			p.AgentJobID = id
			w.onRunProgress(ctx, p)
		}
	}
	return w.waitJob(ctx, id, d)
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

	for {
		select {
		case <-ctx.Done():
			err := w.Client.RcloneJobStop(context.Background(), d.Host, id)
			if err != nil {
				w.Logger.Error(ctx, "Failed to stop rclone job",
					"error", err,
					"host", d.Host,
					"unit", d.Unit,
					"job_id", id,
					"table", d.Table,
				)
			}
			w.updateProgress(ctx, id, d)
			return ctx.Err()
		case <-t.C:
			status, err := w.getJobStatus(ctx, id, d)
			switch status {
			case jobError:
				return err
			case jobNotFound:
				return errors.Errorf("job not found (%d)", id)
			case jobSuccess:
				w.updateProgress(ctx, id, d)
				return nil
			case jobRunning:
				w.updateProgress(ctx, id, d)
			}
		}
	}
}

func (w *worker) clearJobStats(ctx context.Context, jobID int64, host string) error {
	w.Logger.Debug(ctx, "Clearing job stats", "host", host, "job_id", jobID)
	return errors.Wrap(w.Client.RcloneStatsReset(ctx, host, scyllaclient.RcloneDefaultGroup(jobID)), "clear job stats")
}

func (w *worker) getJobStatus(ctx context.Context, jobID int64, d snapshotDir) (jobStatus, error) {
	s, err := w.Client.RcloneJobStatus(ctx, d.Host, jobID)
	if err != nil {
		w.Logger.Error(ctx, "Failed to fetch job status",
			"error", err,
			"host", d.Host,
			"unit", d.Unit,
			"job_id", jobID,
			"table", d.Table,
		)
		if strings.Contains(err.Error(), "job not found") {
			// If job is no longer available fail.
			return jobNotFound, nil
		}
		return jobError, err
	}
	if s.Finished {
		if s.Success {
			return jobSuccess, nil
		}
		return jobError, errors.New(s.Error)
	}
	return jobRunning, nil
}

func (w *worker) updateProgress(ctx context.Context, jobID int64, d snapshotDir) {
	group := scyllaclient.RcloneDefaultGroup(jobID)

	transferred, err := w.Client.RcloneTransferred(ctx, d.Host, group)
	if err != nil {
		w.Logger.Error(ctx, "Failed to get transferred files",
			"error", err,
			"host", d.Host,
			"job_id", jobID,
		)
		return
	}
	stats, err := w.Client.RcloneStats(ctx, d.Host, group)
	if err != nil {
		w.Logger.Error(ctx, "Failed to get transfer stats",
			"error", err,
			"host", d.Host,
			"job_id", jobID,
		)
		return
	}

	for _, p := range d.Progress {
		if p.AgentJobID != jobID || (p.CompletedAt != nil && !p.CompletedAt.IsZero()) {
			continue
		}
		trs := scyllaclient.TransferredByFilename(p.FileName, transferred)
		switch len(trs) {
		case 0:
			// Nothing in transferred so inspect transfers in progress.
			for _, tr := range stats.Transferring {
				if tr.Name == p.FileName {
					p.Uploaded = tr.Bytes
					w.onRunProgress(ctx, p)
					break
				}
			}
		case 1:
			// Only one transfer or one check.
			w.setProgressDates(ctx, p, d, jobID, trs[0].StartedAt, trs[0].CompletedAt)
			if trs[0].Error != "" {
				p.Error = trs[0].Error
				p.Failed = trs[0].Size - trs[0].Bytes
			}
			if trs[0].Checked {
				// File is already uploaded we just checked.
				p.Skipped = trs[0].Size
			} else {
				p.Uploaded = trs[0].Bytes
			}
			w.onRunProgress(ctx, p)
		case 2:
			// File is found and updated on remote (check plus transfer).
			// Order Check > Transfer is expected.
			// Taking start time from the check.
			w.setProgressDates(ctx, p, d, jobID, trs[0].StartedAt, trs[1].CompletedAt)
			if trs[0].Error != "" {
				p.Error = trs[0].Error
			}
			if trs[1].Error != "" {
				p.Error = fmt.Sprintf("%s %s", p.Error, trs[1].Error)
			}
			if p.Error != "" {
				p.Failed = trs[1].Size - trs[1].Bytes
			}
			p.Uploaded = trs[1].Bytes
			w.onRunProgress(ctx, p)
		}
	}
}

func (w *worker) onRunProgress(ctx context.Context, p *RunProgress) {
	if w.OnRunProgress != nil {
		w.OnRunProgress(ctx, p)
	}
}

func (w *worker) setProgressDates(ctx context.Context, p *RunProgress, d snapshotDir, jobID int64, start, end string) {
	startedAt, err := timeutc.Parse(time.RFC3339, start)
	if err != nil {
		w.Logger.Error(ctx, "Failed to parse start time",
			"error", err,
			"host", d.Host,
			"job_id", jobID,
			"value", start,
		)
	}
	if !startedAt.IsZero() {
		p.StartedAt = &startedAt
	}
	completedAt, err := timeutc.Parse(time.RFC3339, end)
	if err != nil {
		w.Logger.Error(ctx, "Failed to parse complete time",
			"error", err,
			"host", d.Host,
			"job_id", jobID,
			"value", end,
		)
	}
	if !completedAt.IsZero() {
		p.CompletedAt = &completedAt
	}
}

func (w *worker) remoteManifestFile(h hostInfo, d snapshotDir) string {
	return remoteManifestFile(w.ClusterID, w.TaskID, w.SnapshotTag, h.DC, h.ID, d.Keyspace, d.Table, d.Version)
}

func (w *worker) remoteSSTableDir(h hostInfo, d snapshotDir) string {
	return remoteSSTableVersionDir(w.ClusterID, h.DC, h.ID, d.Keyspace, d.Table, d.Version)
}
