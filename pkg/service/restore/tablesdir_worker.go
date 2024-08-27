// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func (w *tablesWorker) restoreBatch(ctx context.Context, host string, workerID uint, b batch, pr *RunProgress) (err error) {
	defer func() {
		// Run cleanup on non-pause error
		if err != nil && ctx.Err() == nil {
			w.SetRestoreStateMetric(host, workerID, metrics.RestoreStateError)
			w.deleteRunProgress(context.Background(), pr)
		} else {
			w.SetRestoreStateMetric(host, workerID, metrics.RestoreStateIdle)
		}
	}()

	// Download has already been started on RunProgress creation.
	// Skip steps already done in the previous run.
	if !validateTimeIsSet(pr.DownloadCompletedAt) {
		if err := w.waitJob(ctx, host, workerID, pr); err != nil {
			return errors.Wrap(err, "wait for job")
		}
	}

	if !validateTimeIsSet(pr.RestoreCompletedAt) {
		w.SetRestoreStateMetric(host, workerID, metrics.RestoreStateLoading)
		if !validateTimeIsSet(pr.RestoreStartedAt) {
			pr.setRestoreStartedAt()
			w.insertRunProgress(ctx, pr)
		}

		err := w.worker.restoreSSTables(ctx, host, b.Keyspace, b.Table, true, !w.target.StreamToAllReplicas)
		if err == nil {
			pr.setRestoreCompletedAt()
			w.insertRunProgress(ctx, pr)
			w.DecreaseRemainingBytesMetric(b)
			w.logger.Info(ctx, "Restored batch", "host", pr.Host, "worker ID", workerID)
		}
		return err
	}

	return nil
}

// waitJob waits for rclone job to finish while updating its progress.
func (w *tablesWorker) waitJob(ctx context.Context, host string, workerID uint, pr *RunProgress) (err error) {
	// Agent job id might be empty when all the files are versioned.
	if pr.AgentJobID == 0 {
		return nil
	}

	w.SetRestoreStateMetric(host, workerID, metrics.RestoreStateDownloading)
	w.logger.Info(ctx, "Waiting for job", "host", host, "job_id", pr.AgentJobID)

	defer func() {
		cleanCtx := context.Background()
		// On error stop job
		if err != nil {
			w.stopJob(cleanCtx, pr.AgentJobID, host)
		}
		// On exit clear stats
		w.clearJobStats(cleanCtx, pr.AgentJobID, host)
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		job, err := w.client.RcloneJobProgress(ctx, host, pr.AgentJobID, w.config.LongPollingTimeoutSeconds)
		if err != nil {
			return errors.Wrap(err, "fetch job info")
		}

		switch scyllaclient.RcloneJobStatus(job.Status) {
		case scyllaclient.JobError:
			return errors.Errorf("job error (%d): %s", pr.AgentJobID, job.Error)
		case scyllaclient.JobSuccess:
			w.updateDownloadProgress(ctx, pr, job)
			w.logger.Info(ctx, "Batch download completed", "host", host, "job_id", pr.AgentJobID)
			return nil
		case scyllaclient.JobRunning:
			w.updateDownloadProgress(ctx, pr, job)
		case scyllaclient.JobNotFound:
			return errors.New("job not found")
		}
	}
}

func (w *tablesWorker) updateDownloadProgress(ctx context.Context, pr *RunProgress, job *scyllaclient.RcloneJobProgress) {
	// Set StartedAt and CompletedAt based on Job
	if t := time.Time(job.StartedAt); !t.IsZero() {
		pr.DownloadStartedAt = &t
	}
	if t := time.Time(job.CompletedAt); !t.IsZero() {
		pr.DownloadCompletedAt = &t
	}

	pr.Error = job.Error
	pr.Downloaded = job.Uploaded
	pr.Skipped = job.Skipped
	pr.Failed = job.Failed
	w.insertRunProgress(ctx, pr)
}

// newRunProgress creates RunProgress by assembling batch and starting download to host's upload dir.
func (w *worker) newRunProgress(ctx context.Context, host string, workerID uint, b batch) (*RunProgress, error) {
	if err := w.checkAvailableDiskSpace(ctx, host); err != nil {
		return nil, errors.Wrap(err, "validate free disk space")
	}
	if err := w.cleanUploadDir(ctx, host, b.UploadDir(), nil); err != nil {
		return nil, errors.Wrapf(err, "clean upload dir of host %s", host)
	}
	jobID, versionedPr, err := w.startDownload(ctx, host, workerID, b)
	if err != nil {
		return nil, err
	}

	pr := &RunProgress{
		ClusterID:         w.run.ClusterID,
		TaskID:            w.run.TaskID,
		RunID:             w.run.ID,
		RemoteSSTableDir:  b.RemoteSSTableDir,
		Keyspace:          b.Keyspace,
		Table:             b.Table,
		Host:              host,
		AgentJobID:        jobID,
		SSTableID:         b.IDs(),
		VersionedProgress: versionedPr,
	}
	if jobID == -1 {
		now := timeutc.Now()
		pr.DownloadStartedAt = &now
		pr.DownloadCompletedAt = &now
	}

	w.insertRunProgress(ctx, pr)
	return pr, nil
}

// startDownload creates rclone job responsible for downloading batch of SSTables.
// Downloading of versioned files happens first in a synchronous way.
// It returns jobID for asynchronous download of the newest versions of files
// alongside with the size of the already downloaded versioned files.
func (w *worker) startDownload(ctx context.Context, host string, workerID uint, b batch) (jobID, versionedPr int64, err error) {
	allFiles := b.Files()
	versionedFiles := b.VersionedFiles()
	versionedSize := b.VersionedSize()
	if len(versionedFiles) > 0 {
		s := strset.New(allFiles...)
		s.Remove(versionedFiles...)
		allFiles = s.List()

		// Downloading versioned files requires us to rename them (strip version extension)
		// and function RcloneCopyPaths lacks this option. In order to achieve that, we copy
		// all versioned files one by one with RcloneCopyFile (which supports renaming files).
		// The assumption is that the existence of versioned files is low and that they
		// are rather small, so we can do it in a synchronous way.
		// Copying files can be done in full parallel because of rclone ability to limit transfers.
		f := func(i int) error {
			file := versionedFiles[i]
			name, _ := backupspec.SplitNameAndVersion(file)

			// Restore file without its version extension
			dst := path.Join(b.UploadDir(), name)
			src := path.Join(b.RemoteSSTableDir, file)

			if err := w.client.RcloneCopyFile(ctx, host, dst, src); err != nil {
				return parallel.Abort(errors.Wrapf(err, "host %s: download versioned file %s into %s", host, src, dst))
			}

			w.logger.Info(ctx, "Downloaded versioned file",
				"host", host,
				"src", src,
				"dst", dst,
			)

			return nil
		}

		notify := func(i int, err error) {
			file := versionedFiles[i]
			name, _ := backupspec.SplitNameAndVersion(file)
			dst := path.Join(b.UploadDir(), name)
			src := path.Join(b.RemoteSSTableDir, file)
			w.logger.Error(ctx, "Failed to download versioned file",
				"file", file,
				"dst", dst,
				"src", src,
				"error", err,
			)
		}

		if err := parallel.Run(len(versionedFiles), parallel.NoLimit, f, notify); err != nil {
			return 0, 0, err
		}
	}

	if len(allFiles) > 0 {
		// Start asynchronous job for downloading the newest versions of remaining files
		jobID, err = w.client.RcloneCopyPaths(ctx, host, b.UploadDir(), b.RemoteSSTableDir, allFiles)
		if err != nil {
			return 0, 0, errors.Wrap(err, "download batch to upload dir")
		}
		w.logger.Info(ctx, "Started downloading files",
			"host", host,
			"worker ID", workerID,
			"job_id", jobID,
		)
	} else {
		jobID = -1
	}
	return jobID, versionedSize, nil
}
