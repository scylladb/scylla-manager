// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func (w *tablesWorker) restoreBatch(ctx context.Context, b batch, pr *RunProgress) (err error) {
	defer func() {
		// Run cleanup on non-pause error
		if err != nil {
			w.metrics.SetRestoreState(w.run.ClusterID, b.Location, w.run.SnapshotTag, pr.Host, metrics.RestoreStateError)
			w.cleanupRunProgress(context.Background(), pr)
		}
	}()

	// Download has already been started on RunProgress creation.
	// Skip steps already done in the previous run.
	if !validateTimeIsSet(pr.DownloadCompletedAt) {
		if err := w.waitJob(ctx, b, pr); err != nil {
			return errors.Wrap(err, "wait for job")
		}
	}

	if !validateTimeIsSet(pr.RestoreCompletedAt) {
		if err := w.restoreSSTables(ctx, b, pr); err != nil {
			return errors.Wrap(err, "call load and stream")
		}
	}
	return nil
}

// waitJob waits for rclone job to finish while updating its progress.
func (w *tablesWorker) waitJob(ctx context.Context, b batch, pr *RunProgress) (err error) {
	w.logger.Info(ctx, "Waiting for job", "host", pr.Host, "job_id", pr.AgentJobID)

	defer func() {
		cleanCtx := context.Background()
		// On error stop job
		if err != nil {
			w.stopJob(cleanCtx, pr.AgentJobID, pr.Host)
		}
		// On exit clear stats
		w.clearJobStats(cleanCtx, pr.AgentJobID, pr.Host)
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		job, err := w.client.RcloneJobProgress(ctx, pr.Host, pr.AgentJobID, w.config.LongPollingTimeoutSeconds)
		if err != nil {
			return errors.Wrap(err, "fetch job info")
		}

		switch scyllaclient.RcloneJobStatus(job.Status) {
		case scyllaclient.JobError:
			return errors.Errorf("job error (%d): %s", pr.AgentJobID, job.Error)
		case scyllaclient.JobSuccess:
			w.onDownloadUpdate(ctx, b, pr, job)
			return nil
		case scyllaclient.JobRunning:
			w.onDownloadUpdate(ctx, b, pr, job)
		case scyllaclient.JobNotFound:
			return errors.New("job not found")
		}
	}
}

func (w *tablesWorker) restoreSSTables(ctx context.Context, b batch, pr *RunProgress) error {
	w.onLasStart(ctx, b, pr)
	err := w.worker.restoreSSTables(ctx, pr.Host, pr.Keyspace, pr.Table, true, true)
	if err == nil {
		w.onLasEnd(ctx, b, pr)
	}
	return err
}

// newRunProgress creates RunProgress by starting download to host's upload dir.
func (w *tablesWorker) newRunProgress(ctx context.Context, hi HostInfo, b batch) (*RunProgress, error) {
	uploadDir := UploadTableDir(b.Keyspace, b.Table, w.tableVersion[b.TableName])
	if err := w.cleanUploadDir(ctx, hi.Host, uploadDir, nil); err != nil {
		return nil, errors.Wrapf(err, "clean upload dir of host %s", hi.Host)
	}

	jobID, versionedDownloaded, err := w.startDownload(ctx, hi, b)
	if err != nil {
		return nil, err
	}

	pr := &RunProgress{
		ClusterID:           w.run.ClusterID,
		TaskID:              w.run.TaskID,
		RunID:               w.run.ID,
		RemoteSSTableDir:    b.RemoteSSTableDir,
		Keyspace:            b.Keyspace,
		Table:               b.Table,
		Host:                hi.Host,
		ShardCnt:            int64(w.hostShardCnt[hi.Host]),
		AgentJobID:          jobID,
		SSTableID:           b.IDs(),
		VersionedDownloaded: versionedDownloaded,
	}
	w.onDownloadStart(ctx, b, pr)
	return pr, nil
}

// startDownload creates rclone job responsible for downloading batch of SSTables.
// Downloading of versioned files happens first in a synchronous way.
// It returns jobID for asynchronous download of the newest versions of files
// alongside with the size of the already downloaded versioned files.
func (w *tablesWorker) startDownload(ctx context.Context, hi HostInfo, b batch) (jobID, versionedDownloaded int64, err error) {
	uploadDir := UploadTableDir(b.Keyspace, b.Table, w.tableVersion[b.TableName])
	sstables := b.NotVersionedSSTables()
	versioned := b.VersionedSSTables()
	versionedSize := b.VersionedSize()
	if len(versioned) > 0 {
		if err := w.downloadVersioned(ctx, hi.Host, b.RemoteSSTableDir, uploadDir, versioned); err != nil {
			return 0, 0, errors.Wrapf(err, "download versioned sstabled on host %s", hi.Host)
		}
	}

	// Start asynchronous job for downloading the newest versions of remaining files
	files := make([]string, 0)
	for _, sst := range sstables {
		files = append(files, sst.Files...)
	}
	jobID, err = w.client.RcloneCopyPaths(ctx, hi.Host, hi.Transfers, hi.RateLimit, uploadDir, b.RemoteSSTableDir, files)
	if err != nil {
		return 0, 0, errors.Wrap(err, "download batch to upload dir")
	}
	return jobID, versionedSize, nil
}

// Downloading versioned files requires us to rename them (strip version extension)
// and function RcloneCopyPaths lacks this option. In order to achieve that, we copy
// all versioned files one by one with RcloneCopyFile (which supports renaming files).
// The assumption is that the existence of versioned files is low and that they
// are rather small, so we can do it in a synchronous way.
// Copying files can be done in full parallel because of rclone ability to limit transfers.
func (w *tablesWorker) downloadVersioned(ctx context.Context, host, srcDir, dstDir string, versioned []RemoteSSTable) error {
	f := func(i int) error {
		sst := versioned[i]
		for _, file := range sst.Files {
			name, _ := SplitNameAndVersion(file)
			// Restore file without its version extension
			dst := path.Join(dstDir, name)
			src := path.Join(srcDir, file)
			if err := w.client.RcloneCopyFile(ctx, host, dst, src); err != nil {
				return parallel.Abort(errors.Wrapf(err, "host %s: download versioned file %s into %s", host, src, dst))
			}
		}
		w.logger.Info(ctx, "Downloaded versioned sstable",
			"host", host,
			"sstable ID", sst.ID,
			"src dir", srcDir,
			"dst dir", dstDir,
		)
		return nil
	}

	notify := func(i int, err error) {
		sst := versioned[i]
		w.logger.Error(ctx, "Failed to download versioned sstable",
			"host", host,
			"sstable ID", sst.ID,
			"src dir", srcDir,
			"dst dir", dstDir,
			"error", err,
		)
	}

	return parallel.Run(len(versioned), parallel.NoLimit, f, notify)
}

func (w *tablesWorker) cleanupRunProgress(ctx context.Context, pr *RunProgress) {
	w.deleteRunProgress(ctx, pr)
	tn := TableName{
		Keyspace: pr.Keyspace,
		Table:    pr.Table,
	}
	if cleanErr := w.cleanUploadDir(ctx, pr.Host, UploadTableDir(pr.Keyspace, pr.Table, w.tableVersion[tn]), nil); cleanErr != nil {
		w.logger.Error(ctx, "Couldn't clear destination directory", "host", pr.Host, "error", cleanErr)
	}
}

func (w *tablesWorker) onBatchDispatch(ctx context.Context, b batch, host string) {
	w.metrics.IncreaseBatchSize(w.run.ClusterID, host, b.Size)
	w.logger.Info(ctx, "Got batch to restore",
		"host", host,
		"keyspace", b.Keyspace,
		"table", b.Table,
		"size", b.Size,
		"sstable count", len(b.SSTables),
	)
}

func (w *tablesWorker) onDownloadStart(ctx context.Context, b batch, pr *RunProgress) {
	w.metrics.SetRestoreState(w.run.ClusterID, b.Location, w.run.SnapshotTag, pr.Host, metrics.RestoreStateDownloading)
	w.logger.Info(ctx, "Started downloading batch", "host", pr.Host, "job_id", pr.AgentJobID)
	w.insertRunProgress(ctx, pr)
}

func (w *tablesWorker) onDownloadUpdate(ctx context.Context, b batch, pr *RunProgress, job *scyllaclient.RcloneJobProgress) {
	// As we update metrics on download update,
	// we need to remember to update just the delta.
	w.metrics.IncreaseRestoreDownloadedBytes(w.run.ClusterID, b.Location.StringWithoutDC(), pr.Host, job.Uploaded-pr.Downloaded)
	now := timeutc.Now()
	prevD := timeSub(pr.DownloadStartedAt, pr.DownloadCompletedAt, now)
	if t := time.Time(job.StartedAt); !t.IsZero() {
		pr.DownloadStartedAt = &t
		pr.RestoreStartedAt = &t
	}
	if t := time.Time(job.CompletedAt); !t.IsZero() {
		pr.DownloadCompletedAt = &t
	}
	currD := timeSub(pr.DownloadStartedAt, pr.DownloadCompletedAt, now)
	w.metrics.IncreaseRestoreDownloadDuration(w.run.ClusterID, b.Location.StringWithoutDC(), pr.Host, currD-prevD)

	pr.Error = job.Error
	// Skipped should be equal to 0,
	// as we don't perform any deduplication.
	pr.Downloaded = job.Uploaded + job.Skipped
	pr.Failed = job.Failed
	w.insertRunProgress(ctx, pr)

	if scyllaclient.RcloneJobStatus(job.Status) == scyllaclient.JobSuccess {
		w.logger.Info(ctx, "Downloaded batch", "host", pr.Host, "job id", pr.AgentJobID)
	}
}

func (w *tablesWorker) onLasStart(ctx context.Context, b batch, pr *RunProgress) {
	w.metrics.SetRestoreState(w.run.ClusterID, b.Location, w.run.SnapshotTag, pr.Host, metrics.RestoreStateLoading)
	w.logger.Info(ctx, "Started restoring batch", "host", pr.Host)
}

func (w *tablesWorker) onLasEnd(ctx context.Context, b batch, pr *RunProgress) {
	w.metrics.SetRestoreState(w.run.ClusterID, b.Location, w.target.SnapshotTag, pr.Host, metrics.RestoreStateIdle)
	pr.setRestoreCompletedAt()
	pr.Restored = pr.Downloaded + pr.VersionedDownloaded
	w.metrics.IncreaseRestoreStreamedBytes(w.run.ClusterID, pr.Host, b.Size)
	w.metrics.IncreaseRestoreStreamDuration(w.run.ClusterID, pr.Host, timeSub(pr.RestoreStartedAt, pr.RestoreCompletedAt, timeutc.Now()))

	labels := metrics.RestoreBytesLabels{
		ClusterID:   b.ClusterID.String(),
		SnapshotTag: b.SnapshotTag,
		Location:    b.Location.String(),
		DC:          b.DC,
		Node:        b.NodeID,
		Keyspace:    b.Keyspace,
		Table:       b.Table,
	}
	w.metrics.DecreaseRemainingBytes(labels, b.Size)

	progressLabels := metrics.RestoreProgressLabels{
		ClusterID:   w.run.ClusterID.String(),
		SnapshotTag: w.run.SnapshotTag,
	}
	w.progress.Update(b.Size)
	w.metrics.SetProgress(progressLabels, w.progress.CurrentProgress())

	w.logger.Info(ctx, "Restored batch", "host", pr.Host)
	w.insertRunProgress(ctx, pr)
}
