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
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/atomic"
)

// tablesDirWorker is responsible for restoring table files from manifest in parallel.
type tablesDirWorker struct {
	worker

	bundles      map[string]bundle // Maps bundle to it's ID
	bundleIDPool idPool            // SSTable IDs yet to  be restored

	dstDir string                  // "data:" prefixed path to upload dir (common for every host)
	srcDir string                  // Full path to remote directory with backed-up files
	miwc   ManifestInfoWithContent // Manifest containing fm
	fm     FilesMeta               // Describes table and it's files located in srcDir

	ongoingPr []*RunProgress // Unfinished RunProgress from previous run of each host

	// Maps original SSTable name to its existing older version
	// (with respect to currently restored snapshot tag)
	// that should be used during the restore procedure.
	versionedFiles VersionedMap
	fileSizesCache map[string]int64
	progress       *TotalRestoreProgress
}

func newTablesDirWorker(ctx context.Context, w worker, miwc ManifestInfoWithContent, fm FilesMeta, progress *TotalRestoreProgress) (tablesDirWorker, error) {
	bundles := newBundles(fm)
	bundleIDPool := newIDPool(bundles)

	version, err := query.GetTableVersion(w.clusterSession, fm.Keyspace, fm.Table)
	if err != nil {
		return tablesDirWorker{}, errors.Wrap(err, "get table version")
	}

	srcDir := miwc.LocationSSTableVersionDir(fm.Keyspace, fm.Table, fm.Version)
	dstDir := UploadTableDir(fm.Keyspace, fm.Table, version)
	w.logger.Info(ctx, "Found table's src and dst dirs",
		"keyspace", fm.Keyspace,
		"table", fm.Table,
		"src_dir", srcDir,
		"dst_dir", dstDir,
	)

	hosts := w.target.locationHosts[miwc.Location]
	versionedFiles, err := ListVersionedFiles(ctx, w.client, w.run.SnapshotTag, hosts[0], srcDir)
	if err != nil {
		return tablesDirWorker{}, errors.Wrap(err, "initialize versioned SSTables")
	}
	if len(versionedFiles) > 0 {
		w.logger.Info(ctx, "Chosen versioned SSTables",
			"dir", srcDir,
			"versioned_files", versionedFiles,
		)
	}

	fileSizesCache, err := buildFilesSizesCache(ctx, w.client, hosts[0], srcDir, versionedFiles)
	if err != nil {
		return tablesDirWorker{}, errors.Wrap(err, "build files sizes cache")
	}

	return tablesDirWorker{
		worker:         w,
		bundles:        bundles,
		bundleIDPool:   bundleIDPool,
		dstDir:         dstDir,
		srcDir:         srcDir,
		miwc:           miwc,
		fm:             fm,
		ongoingPr:      make([]*RunProgress, len(hosts)),
		versionedFiles: versionedFiles,
		fileSizesCache: fileSizesCache,
		progress:       progress,
	}, nil
}

// restore SSTables of receivers manifest/table in parallel.
func (w *tablesDirWorker) restore(ctx context.Context) error {
	// Count of SSTable IDs yet to be successfully restored
	ctr := atomic.NewInt64(w.bundleIDPool.size())
	for _, pr := range w.ongoingPr {
		if pr != nil {
			ctr.Add(pr.idCnt())
		}
	}

	if ctr.Load() == 0 {
		w.logger.Info(ctx, "Table does not have any more SSTables to restore",
			"keyspace", w.fm.Keyspace,
			"table", w.fm.Table,
		)
		return nil
	}

	hosts := w.target.locationHosts[w.miwc.Location]
	f := func(n int) (err error) {
		h := hosts[n]
		ongoingPr := w.ongoingPr[n]

		// First handle ongoing restore
		if ongoingPr != nil {
			if err := w.reactivateRunProgress(ctx, ongoingPr); err != nil {
				return errors.Wrap(err, "reactivate run progress")
			}

			if err := w.restoreBatch(ctx, ongoingPr); err != nil {
				return errors.Wrap(err, "restore reactivated batch")
			}

			if ctr.Sub(ongoingPr.idCnt()) <= 0 {
				close(w.bundleIDPool)
			}
		}

		for {
			pr, err := w.newRunProgress(ctx, h)
			if err != nil {
				return errors.Wrap(err, "create run progress")
			}
			if pr == nil {
				w.logger.Info(ctx, "No more batches to restore", "host", h)
				return nil
			}

			if err := w.restoreBatch(ctx, pr); err != nil {
				return errors.Wrap(err, "restore batch")
			}

			if ctr.Sub(pr.idCnt()) <= 0 {
				close(w.bundleIDPool)
			}
		}
	}

	notify := func(n int, err error) {
		w.logger.Error(ctx, "Failed to restore files on host",
			"host", hosts[n],
			"error", err,
		)
	}

	return parallel.Run(len(hosts), w.target.Parallel, f, notify)
}

func (w *tablesDirWorker) restoreBatch(ctx context.Context, pr *RunProgress) (err error) {
	defer func() {
		// Run cleanup on non-pause error
		if err != nil && ctx.Err() == nil {
			w.metrics.SetRestoreState(w.run.ClusterID, w.miwc.Location, w.run.SnapshotTag, pr.Host, metrics.RestoreStateError)
			w.cleanupRunProgress(context.Background(), pr)
		} else {
			w.metrics.SetRestoreState(w.run.ClusterID, w.miwc.Location, w.run.SnapshotTag, pr.Host, metrics.RestoreStateIdle)
		}
	}()

	// Download has already been started on RunProgress creation.
	// Skip steps already done in the previous run.
	if !validateTimeIsSet(pr.DownloadCompletedAt) {
		if err := w.waitJob(ctx, pr); err != nil {
			return errors.Wrap(err, "wait for job")
		}
	}

	if !validateTimeIsSet(pr.RestoreCompletedAt) {
		if err := w.restoreSSTables(ctx, pr); err != nil {
			return errors.Wrap(err, "call load and stream")
		}
	}

	w.decreaseRemainingBytesMetric(pr.Downloaded + pr.Skipped + pr.VersionedProgress)
	w.logger.Info(ctx, "Restored batch", "host", pr.Host, "sstable_id", pr.SSTableID)
	return nil
}

// waitJob waits for rclone job to finish while updating its progress.
func (w *tablesDirWorker) waitJob(ctx context.Context, pr *RunProgress) (err error) {
	w.metrics.SetRestoreState(w.run.ClusterID, w.miwc.Location, w.miwc.SnapshotTag, pr.Host, metrics.RestoreStateDownloading)
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
			w.updateDownloadProgress(ctx, pr, job)
			w.logger.Info(ctx, "Batch download completed", "host", pr.Host, "job_id", pr.AgentJobID)
			return nil
		case scyllaclient.JobRunning:
			w.updateDownloadProgress(ctx, pr, job)
		case scyllaclient.JobNotFound:
			return errors.New("job not found")
		}
	}
}

func (w *tablesDirWorker) updateDownloadProgress(ctx context.Context, pr *RunProgress, job *scyllaclient.RcloneJobProgress) {
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

func (w *tablesDirWorker) restoreSSTables(ctx context.Context, pr *RunProgress) error {
	w.metrics.SetRestoreState(w.run.ClusterID, w.miwc.Location, w.run.SnapshotTag, pr.Host, metrics.RestoreStateLoading)
	if !validateTimeIsSet(pr.RestoreStartedAt) {
		pr.setRestoreStartedAt()
		w.insertRunProgress(ctx, pr)
	}

	err := w.worker.restoreSSTables(ctx, pr.Host, pr.Keyspace, pr.Table, true, true)
	if err == nil {
		pr.setRestoreCompletedAt()
		w.insertRunProgress(ctx, pr)
	}
	return err
}

func (w *tablesDirWorker) resumePrevProgress() error {
	bind := &RunProgress{
		ClusterID:    w.run.ClusterID,
		TaskID:       w.run.TaskID,
		RunID:        w.run.ID,
		ManifestPath: w.miwc.Path(),
		Keyspace:     w.fm.Keyspace,
		Table:        w.fm.Table,
	}

	// All bundles IDs started in the previous run
	startedID := strset.New()
	// All unfinished RunProgress started in the previous run
	ongoingPr := make(map[string]*RunProgress)
	err := bind.ForEachTableProgress(w.session, func(pr *RunProgress) {
		startedID.Add(pr.SSTableID...)
		if !validateTimeIsSet(pr.RestoreCompletedAt) {
			cp := *pr
			ongoingPr[pr.Host] = &cp
		}
	})
	if err != nil {
		return err
	}

	// Remove already started ID from the pool
	ids := w.bundleIDPool.drain()
	for _, id := range ids {
		if !startedID.Has(id) {
			w.bundleIDPool <- id
		}
	}

	hosts := w.target.locationHosts[w.miwc.Location]
	// Set ongoing RunProgress so that they can be resumed
	for i, h := range hosts {
		w.ongoingPr[i] = ongoingPr[h]
	}
	return nil
}

// reactivateRunProgress preserves batch assembled in the previous run and tries to reuse its unfinished rclone job.
func (w *tablesDirWorker) reactivateRunProgress(ctx context.Context, pr *RunProgress) error {
	// Nothing to do if download has already finished
	if validateTimeIsSet(pr.DownloadCompletedAt) {
		return nil
	}

	job, err := w.client.RcloneJobProgress(ctx, pr.Host, pr.AgentJobID, w.config.LongPollingTimeoutSeconds)
	if err != nil {
		return errors.Wrapf(err, "get progress of rclone job %d", pr.AgentJobID)
	}
	// Nothing to do if rclone job is still running
	if scyllaclient.WorthWaitingForJob(job.Status) {
		return nil
	}

	// Recreate rclone job
	batch := w.batchFromIDs(pr.SSTableID)
	if err := w.cleanUploadDir(ctx, pr.Host, w.dstDir, batch); err != nil {
		return errors.Wrapf(err, "clean upload dir of host %s", pr.Host)
	}

	jobID, versionedPr, err := w.startDownload(ctx, pr.Host, batch)
	if err != nil {
		w.deleteRunProgress(ctx, pr)
		w.returnBatchToPool(pr.SSTableID, pr.Host)
		return err
	}

	pr.AgentJobID = jobID
	pr.VersionedProgress = versionedPr
	w.insertRunProgress(ctx, pr)
	return nil
}

// newRunProgress creates RunProgress by assembling batch and starting download to host's upload dir.
func (w *tablesDirWorker) newRunProgress(ctx context.Context, host string) (*RunProgress, error) {
	if err := w.checkAvailableDiskSpace(ctx, host); err != nil {
		return nil, errors.Wrap(err, "validate free disk space")
	}

	takenIDs := w.chooseIDsForBatch(ctx, w.target.BatchSize, host)
	if ctx.Err() != nil {
		w.returnBatchToPool(takenIDs, host)
		return nil, ctx.Err()
	}
	if takenIDs == nil {
		return nil, nil //nolint: nilnil
	}

	w.logger.Info(ctx, "Created new batch",
		"host", host,
		"sstable_id", takenIDs,
	)

	batch := w.batchFromIDs(takenIDs)
	if err := w.cleanUploadDir(ctx, host, w.dstDir, nil); err != nil {
		return nil, errors.Wrapf(err, "clean upload dir of host %s", host)
	}

	jobID, versionedPr, err := w.startDownload(ctx, host, batch)
	if err != nil {
		w.returnBatchToPool(takenIDs, host)
		return nil, err
	}

	pr := &RunProgress{
		ClusterID:         w.run.ClusterID,
		TaskID:            w.run.TaskID,
		RunID:             w.run.ID,
		ManifestPath:      w.miwc.Path(),
		Keyspace:          w.fm.Keyspace,
		Table:             w.fm.Table,
		Host:              host,
		AgentJobID:        jobID,
		SSTableID:         takenIDs,
		VersionedProgress: versionedPr,
	}

	w.insertRunProgress(ctx, pr)
	return pr, nil
}

// startDownload creates rclone job responsible for downloading batch of SSTables.
// Downloading of versioned files happens first in a synchronous way.
// It returns jobID for asynchronous download of the newest versions of files
// alongside with the size of the already downloaded versioned files.
func (w *tablesDirWorker) startDownload(ctx context.Context, host string, batch []string) (jobID, versionedPr int64, err error) {
	var (
		regularBatch   = make([]string, 0)
		versionedBatch = make([]VersionedSSTable, 0)
	)
	// Decide which files require to be downloaded in their older version
	for _, file := range batch {
		if v, ok := w.versionedFiles[file]; ok {
			versionedBatch = append(versionedBatch, v)
			versionedPr += v.Size
		} else {
			regularBatch = append(regularBatch, file)
		}
	}
	// Downloading versioned files requires us to rename them (strip version extension)
	// and function RcloneCopyPaths lacks this option. In order to achieve that, we copy
	// all versioned files one by one with RcloneCopyFile (which supports renaming files).
	// The assumption is that the existence of versioned files is low and that they
	// are rather small, so we can do it in a synchronous way.
	// Copying files can be done in full parallel because of rclone ability to limit transfers.
	f := func(i int) error {
		file := versionedBatch[i]
		// Restore file without its version extension
		dst := path.Join(w.dstDir, file.Name)
		src := path.Join(w.srcDir, file.FullName())

		if err := w.client.RcloneCopyFile(ctx, host, dst, src); err != nil {
			return parallel.Abort(errors.Wrapf(err, "host %s: download versioned file %s into %s", host, src, dst))
		}

		w.logger.Info(ctx, "Downloaded versioned file",
			"host", host,
			"src", src,
			"dst", dst,
			"size", file.Size,
		)

		return nil
	}

	notify := func(i int, err error) {
		file := versionedBatch[i]
		dst := path.Join(w.dstDir, file.Name)
		src := path.Join(w.srcDir, file.FullName())
		w.logger.Error(ctx, "Failed to download versioned SSTable",
			"file", file,
			"dst", dst,
			"src", src,
			"error", err,
		)
	}

	if err := parallel.Run(len(versionedBatch), parallel.NoLimit, f, notify); err != nil {
		return 0, 0, err
	}
	// Start asynchronous job for downloading the newest versions of remaining files
	jobID, err = w.client.RcloneCopyPaths(ctx, host, w.dstDir, w.srcDir, regularBatch)
	if err != nil {
		return 0, 0, errors.Wrap(err, "download batch to upload dir")
	}

	w.logger.Info(ctx, "Started downloading files",
		"host", host,
		"job_id", jobID,
		"batch", regularBatch,
	)

	return jobID, versionedPr, nil
}

// chooseIDsForBatch returns slice of IDs of SSTables that the batch consists of.
func (w *tablesDirWorker) chooseIDsForBatch(ctx context.Context, size int, host string) (takenIDs []string) {
	defer func() {
		w.increaseBatchSizeMetric(w.run.ClusterID, w.batchFromIDs(takenIDs), host)
	}()

	// All hosts are trying to get IDs for batch from the pool.
	// Pool is closed after the whole table has been restored.

	// Take at most size IDs
	for i := 0; i < size; i++ {
		select {
		case <-ctx.Done():
			return takenIDs
		default:
		}

		select {
		case id, ok := <-w.bundleIDPool:
			if !ok {
				return takenIDs
			}
			takenIDs = append(takenIDs, id)
		default:
			// Don't wait for more IDs if the pool is empty
			// and host already has something to restore.
			if len(takenIDs) > 0 {
				return takenIDs
			}
			// Here host hasn't taken any IDs and pool is empty,
			// so it waits for the whole table to be restored or
			// for IDs that might return to the pool in case of error on the other hosts.
			select {
			case id, ok := <-w.bundleIDPool:
				if !ok {
					return takenIDs
				}
				takenIDs = append(takenIDs, id)
			case <-ctx.Done():
				return takenIDs
			}
		}
	}

	return takenIDs
}

func (w *tablesDirWorker) decreaseRemainingBytesMetric(bytes int64) {
	labels := metrics.RestoreBytesLabels{
		ClusterID:   w.run.ClusterID.String(),
		SnapshotTag: w.run.SnapshotTag,
		Location:    w.miwc.Location.String(),
		DC:          w.miwc.DC,
		Node:        w.miwc.NodeID,
		Keyspace:    w.fm.Keyspace,
		Table:       w.fm.Table,
	}
	w.metrics.DecreaseRemainingBytes(labels, bytes)
	w.progress.Update(bytes)

	progressLabels := metrics.RestoreProgressLabels{
		ClusterID:   w.run.ClusterID.String(),
		SnapshotTag: w.run.SnapshotTag,
	}
	w.metrics.SetProgress(progressLabels, w.progress.CurrentProgress())
}

func (w *tablesDirWorker) increaseBatchSizeMetric(clusterID uuid.UUID, batch []string, host string) {
	w.metrics.IncreaseBatchSize(clusterID, host, w.countBatchSize(batch))
}

func (w *tablesDirWorker) decreaseBatchSizeMetric(clusterID uuid.UUID, batch []string, host string) {
	w.metrics.DecreaseBatchSize(clusterID, host, w.countBatchSize(batch))
}

func (w *tablesDirWorker) cleanupRunProgress(ctx context.Context, pr *RunProgress) {
	w.deleteRunProgress(ctx, pr)
	w.returnBatchToPool(pr.SSTableID, pr.Host)

	if cleanErr := w.cleanUploadDir(ctx, pr.Host, w.dstDir, nil); cleanErr != nil {
		w.logger.Error(ctx, "Couldn't clear destination directory", "host", pr.Host, "error", cleanErr)
	}
}

func (w *tablesDirWorker) returnBatchToPool(ids []string, host string) {
	w.decreaseBatchSizeMetric(w.run.ClusterID, w.batchFromIDs(ids), host)
	for _, id := range ids {
		w.bundleIDPool <- id
	}
}

func (w *tablesDirWorker) batchFromIDs(ids []string) []string {
	var batch []string
	for _, id := range ids {
		batch = append(batch, w.bundles[id]...)
	}
	return batch
}

func (w *tablesDirWorker) countBatchSize(batch []string) int64 {
	var batchSize int64
	for _, file := range batch {
		batchSize += w.fileSizesCache[file]
	}
	return batchSize
}

// bundle represents SSTables with the same ID.
type bundle []string

func newBundles(fm FilesMeta) map[string]bundle {
	bundles := make(map[string]bundle)
	for _, f := range fm.Files {
		id := sstable.ExtractID(f)
		bundles[id] = append(bundles[id], f)
	}
	return bundles
}

// idPool represents pool of SSTableIDs yet to be restored.
type idPool chan string

func (p idPool) drain() []string {
	var out []string
	for len(p) > 0 {
		out = append(out, <-p)
	}
	return out
}

func (p idPool) size() int64 {
	return int64(len(p))
}

func newIDPool(bundles map[string]bundle) chan string {
	bundleIDPool := make(chan string, len(bundles))
	for id := range bundles {
		bundleIDPool <- id
	}
	return bundleIDPool
}
