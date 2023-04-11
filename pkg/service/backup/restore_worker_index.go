// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/atomic"
)

// restoreHost represents host that can be used for restoring files.
// If set, OngoingRunProgress represents unfinished RestoreRunProgress created in previous run.
type restoreHost struct {
	Host               string
	OngoingRunProgress *RestoreRunProgress
}

// bundle represents SSTables with the same ID.
type bundle []string

type indexWorker struct {
	restoreWorkerTools

	bundles      map[string]bundle       // Maps bundle to it's ID
	bundleIDPool chan string             // IDs of the bundles that are yet to be restored
	resumed      bool                    // Set to true if current run has already skipped all tables restored in previous run
	hosts        []restoreHost           // Restore units created for currently restored location
	miwc         ManifestInfoWithContent // Currently restored manifest
	// Maps original SSTable name to its existing older version (with respect to currently restored snapshot tag)
	// that should be used during the restore procedure. It should be initialized per each restored table.
	versionedFiles VersionedMap
	fileSizesCache map[string]int64
}

func (w *indexWorker) filesMetaRestoreHandler(ctx context.Context, run *RestoreRun, target RestoreTarget) func(fm FilesMeta) error {
	return func(fm FilesMeta) error {
		if !w.resumed {
			// Check if table has already been processed in previous run
			if run.Keyspace != fm.Keyspace || run.Table != fm.Table {
				w.Logger.Info(ctx, "Skipping table", "keyspace", fm.Keyspace, "table", fm.Table)
				return nil
			}
		}

		w.Logger.Info(ctx, "Restoring table", "keyspace", fm.Keyspace, "table", fm.Table)
		defer w.Logger.Info(ctx, "Restoring table finished", "keyspace", fm.Keyspace, "table", fm.Table)

		run.Table = fm.Table
		run.Keyspace = fm.Keyspace
		w.insertRun(ctx, run)

		w.initBundlePool(ctx, run, fm.Files)
		// Set resumed only after all initializations as they depend on knowing
		// if current table have been processed by the previous run.
		w.resumed = true

		if err := w.workFunc(ctx, run, target, fm); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// In case all SSTables have been restored, restore can proceed even
			// with errors from some hosts.
			if len(w.bundleIDPool) > 0 {
				return errors.Wrapf(err, "not restored bundles %v", w.drainBundleIDPool())
			}

			w.Logger.Error(ctx, "Restore table failed on some hosts but restore will proceed",
				"keyspace", run.Keyspace,
				"table", run.Table,
				"error", err,
			)
		}

		return nil
	}
}

// workFunc is responsible for creating and restoring batches on multiple hosts (possibly in parallel).
// It requires previous initialization of restore worker components.
func (w *indexWorker) workFunc(ctx context.Context, run *RestoreRun, target RestoreTarget, fm FilesMeta) error { // nolint: gocognit
	version, err := w.GetTableVersion(ctx, fm.Keyspace, fm.Table)
	if err != nil {
		return err
	}

	var (
		srcDir = w.location.RemotePath(w.miwc.SSTableVersionDir(fm.Keyspace, fm.Table, fm.Version))
		dstDir = UploadTableDir(fm.Keyspace, fm.Table, version)
	)

	w.Logger.Info(ctx, "Found table's source and destination directory",
		"keyspace", fm.Keyspace,
		"table", fm.Table,
		"src_dir", srcDir,
		"dst_dir", dstDir,
	)

	ctr := newSSTableCounter(int64(len(w.bundleIDPool)), w.hosts)
	if ctr.RestoreTables(0) == 0 {
		w.Logger.Info(ctx, "Table does not have any more SSTables to restore",
			"keyspace", fm.Keyspace,
			"table", fm.Table,
		)
		return nil
	}
	w.versionedFiles, err = ListVersionedFiles(ctx, w.Client, w.SnapshotTag, w.hosts[0].Host, srcDir, w.Logger)
	if err != nil {
		return errors.Wrap(err, "initialize versioned SSTables")
	}
	w.fileSizesCache, err = buildFilesSizesCache(ctx, w.Client, w.hosts[0].Host, srcDir, w.versionedFiles)
	if err != nil {
		return errors.Wrap(err, "build files sizes cache")
	}

	// Every host has its personal goroutine which is responsible
	// for creating and downloading batches.
	// Goroutine returns only in case of error or
	// if the whole table has been restored.
	return parallel.Run(len(w.hosts), target.Parallel, func(n int) (err error) {
		// Current goroutine's host
		h := &w.hosts[n]
		defer func() {
			if err != nil {
				w.metrics.SetRestoreState(run.ClusterID, w.location, target.SnapshotTag, h.Host, metrics.RestoreStateError)
				return
			}
			w.metrics.SetRestoreState(run.ClusterID, w.location, target.SnapshotTag, h.Host, metrics.RestoreStateIdle)
		}()
		for {
			pr, err := w.prepareRunProgress(ctx, run, target, h, dstDir, srcDir)
			w.metrics.SetRestoreState(w.ClusterID, w.location, w.miwc.SnapshotTag, h.Host, metrics.RestoreStateDownloading)

			if ctx.Err() != nil {
				w.Logger.Info(ctx, "Canceled context", "host", h.Host)
				return parallel.Abort(ctx.Err())
			}
			if err != nil {
				return errors.Wrap(err, "prepare run progress")
			}
			if pr == nil {
				w.Logger.Info(ctx, "No more batches to restore", "host", h.Host)
				return nil
			}

			// Check if download hasn't already completed in previous run
			if !validateTimeIsSet(pr.DownloadCompletedAt) {
				w.Logger.Info(ctx, "Waiting for job", "host", h.Host, "job_id", pr.AgentJobID)

				if err = w.waitJob(ctx, pr); err != nil {
					if ctx.Err() != nil {
						return parallel.Abort(ctx.Err())
					}
					// As run progress could have already been inserted
					// into the database, it should be deleted.
					w.deleteRunProgress(ctx, pr)
					w.returnBatchToPool(pr.SSTableID, h.Host)
					if cleanErr := w.cleanUploadDir(ctx, h.Host, dstDir, nil); cleanErr != nil {
						w.Logger.Error(ctx, "Couldn't clear destination directory", "host", h.Host, "error", cleanErr)
					}

					return errors.Wrapf(err, "wait on rclone job, id: %d, host: %s", pr.AgentJobID, h.Host)
				}
			}
			w.metrics.SetRestoreState(run.ClusterID, w.location, target.SnapshotTag, h.Host, metrics.RestoreStateLoading)

			if !validateTimeIsSet(pr.RestoreStartedAt) {
				pr.setRestoreStartedAt()
				w.insertRunProgress(ctx, pr)
			}

			if err = w.restoreSSTables(ctx, h.Host, fm.Keyspace, fm.Table, true, true); err != nil {
				if ctx.Err() != nil {
					w.Logger.Info(ctx, "Stop load and stream: canceled context", "host", h.Host)
					return parallel.Abort(ctx.Err())
				}
				w.deleteRunProgress(ctx, pr)
				w.returnBatchToPool(pr.SSTableID, h.Host)
				if cleanErr := w.cleanUploadDir(ctx, h.Host, dstDir, nil); cleanErr != nil {
					w.Logger.Error(ctx, "Couldn't clear destination directory", "host", h.Host, "error", cleanErr)
				}

				return errors.Wrapf(err, "call load and stream, host: %s", h.Host)
			}
			w.metrics.SetRestoreState(run.ClusterID, w.location, target.SnapshotTag, h.Host, metrics.RestoreStateIdle)

			pr.setRestoreCompletedAt()
			w.insertRunProgress(ctx, pr)

			restoredBytes := pr.Downloaded + pr.Skipped + pr.VersionedProgress
			w.metrics.DecreaseRemainingBytes(w.ClusterID, target.SnapshotTag, w.location, w.miwc.DC, w.miwc.NodeID,
				pr.Keyspace, pr.Table, restoredBytes)

			w.Logger.Info(ctx, "Restored batch", "host", h.Host, "sstable_id", pr.SSTableID)
			// Close pool and free hosts awaiting on it if all SSTables have been successfully restored.
			if ctr.RestoreTables(len(pr.SSTableID)) == 0 {
				close(w.bundleIDPool)
			}
		}
	})
}

// initBundlePool creates bundles and pool of their IDs that have yet to be restored.
// (It does not include ones that are currently being restored).
func (w *indexWorker) initBundlePool(ctx context.Context, run *RestoreRun, sstables []string) {
	w.bundles = make(map[string]bundle)

	for _, f := range sstables {
		id := sstableID(f)
		w.bundles[id] = append(w.bundles[id], f)
	}

	w.bundleIDPool = make(chan string, len(w.bundles))
	takenIDs := make([]string, 0)
	processed := strset.New()

	if !w.resumed {
		cb := func(pr *RestoreRunProgress) {
			processed.Add(pr.SSTableID...)
		}
		w.ForEachTableProgress(ctx, run, cb)
	}

	for id := range w.bundles {
		if !processed.Has(id) {
			w.bundleIDPool <- id
			takenIDs = append(takenIDs, id)
		}
	}

	w.Logger.Info(ctx, "Initialized SSTable bundle pool", "sstable_ids", takenIDs)
}

// prepareRunProgress either reactivates RestoreRunProgress created in previous run
// or it creates a brand new RestoreRunProgress.
func (w *indexWorker) prepareRunProgress(ctx context.Context, run *RestoreRun, target RestoreTarget, h *restoreHost, dstDir, srcDir string,
) (pr *RestoreRunProgress, err error) {
	if h.OngoingRunProgress != nil {
		pr = h.OngoingRunProgress
		// Mark run progress as resumed
		h.OngoingRunProgress = nil
		if err = w.reactivateRunProgress(ctx, pr, dstDir, srcDir); err != nil {
			return nil, errors.Wrapf(err, "reactivate run progress: %v", *pr)
		}
	} else {
		pr, err = w.newRunProgress(ctx, run, target, h, dstDir, srcDir)
		if err != nil {
			return nil, errors.Wrapf(err, "create run progress for host: %s", h.Host)
		}
	}

	return
}

// reactivateRunProgress preserves batch assembled in the previous run and tries to reuse its unfinished rclone job.
// In case that's impossible, it has to be recreated (rclone jobs cannot be resumed).
func (w *indexWorker) reactivateRunProgress(ctx context.Context, pr *RestoreRunProgress, dstDir, srcDir string) error {
	// Nothing to do if download has already finished
	if validateTimeIsSet(pr.DownloadCompletedAt) {
		return nil
	}
	// Nothing to do if rclone job is still running
	if job, err := w.Client.RcloneJobProgress(ctx, pr.Host, pr.AgentJobID, w.Config.LongPollingTimeoutSeconds); err != nil {
		if scyllaclient.WorthWaitingForJob(job.Status) {
			return nil
		}
	}
	// Recreate rclone job
	batch := w.batchFromIDs(pr.SSTableID)
	if err := w.cleanUploadDir(ctx, pr.Host, dstDir, batch); err != nil {
		w.Logger.Error(ctx, "Couldn't clear destination directory", "host", pr.Host, "error", err)
	}

	jobID, versionedPr, err := w.startDownload(ctx, pr.Host, dstDir, srcDir, batch)
	if err != nil {
		w.deleteRunProgress(ctx, pr)
		w.returnBatchToPool(pr.SSTableID, pr.Host)
		return err
	}

	pr.AgentJobID = jobID
	pr.VersionedProgress = versionedPr

	w.insertRunProgress(ctx, pr)
	// Treat versioned progress as skipped as those files had to be downloaded during previous run
	w.metrics.UpdateFilesProgress(pr.ClusterID, pr.ManifestPath, pr.Keyspace, pr.Table, 0, pr.VersionedProgress, 0)

	return nil
}

// newRunProgress creates RestoreRunProgress by assembling batch and starting download to host's upload dir.
func (w *indexWorker) newRunProgress(ctx context.Context, run *RestoreRun, target RestoreTarget, h *restoreHost, dstDir, srcDir string,
) (*RestoreRunProgress, error) {
	if err := w.checkAvailableDiskSpace(ctx, hostInfo{IP: h.Host}); err != nil {
		return nil, errors.Wrap(err, "validate free disk space")
	}

	takenIDs := w.chooseIDsForBatch(ctx, target.BatchSize, h.Host)
	if ctx.Err() != nil {
		w.returnBatchToPool(takenIDs, h.Host)
		return nil, ctx.Err()
	}
	if takenIDs == nil {
		return nil, nil //nolint: nilnil
	}

	w.Logger.Info(ctx, "Created new batch",
		"host", h.Host,
		"sstable_id", takenIDs,
	)

	batch := w.batchFromIDs(takenIDs)
	if err := w.cleanUploadDir(ctx, h.Host, dstDir, nil); err != nil {
		w.Logger.Error(ctx, "Couldn't clear destination directory", "host", h.Host, "error", err)
	}

	jobID, versionedPr, err := w.startDownload(ctx, h.Host, dstDir, srcDir, batch)
	if err != nil {
		w.returnBatchToPool(takenIDs, h.Host)
		return nil, err
	}

	pr := &RestoreRunProgress{
		ClusterID:         run.ClusterID,
		TaskID:            run.TaskID,
		RunID:             run.ID,
		ManifestPath:      run.ManifestPath,
		Keyspace:          run.Keyspace,
		Table:             run.Table,
		Host:              h.Host,
		AgentJobID:        jobID,
		SSTableID:         takenIDs,
		VersionedProgress: versionedPr,
	}

	w.insertRunProgress(ctx, pr)
	w.metrics.UpdateFilesProgress(pr.ClusterID, pr.ManifestPath, pr.Keyspace, pr.Table, pr.VersionedProgress, 0, 0)

	return pr, nil
}

func (w *indexWorker) countBatchSize(batch []string) int64 {
	var batchSize int64
	for _, file := range batch {
		batchSize += w.fileSizesCache[file]
	}
	return batchSize
}

func (w *indexWorker) increaseBatchSizeMetric(clusterID uuid.UUID, batch []string, host string) {
	w.metrics.IncreaseBatchSize(clusterID, host, w.countBatchSize(batch))
}

func (w *indexWorker) decreaseBatchSizeMetric(clusterID uuid.UUID, batch []string, host string) {
	w.metrics.DecreaseBatchSize(clusterID, host, w.countBatchSize(batch))
}

// startDownload creates rclone job responsible for downloading batch of SSTables.
// Downloading of versioned files happens first in a synchronous way.
// It returns jobID for asynchronous download of the newest versions of files
// alongside with the size of the already downloaded versioned files.
func (w *indexWorker) startDownload(ctx context.Context, host, dstDir, srcDir string, batch []string) (int64, int64, error) {
	var (
		regularBatch   = make([]string, 0)
		versionedBatch = make([]VersionedSSTable, 0)
		versionedPr    int64
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
	err := parallel.Run(len(versionedBatch), parallel.NoLimit, func(i int) error {
		file := versionedBatch[i]
		// Restore file without its version extension
		dst := path.Join(dstDir, file.Name)
		src := path.Join(srcDir, file.FullName())

		if err := w.Client.RcloneCopyFile(ctx, host, dst, src); err != nil {
			return parallel.Abort(errors.Wrapf(err, "host %s: download versioned file %s into %s", host, src, dst))
		}

		w.Logger.Info(ctx, "Downloaded versioned file",
			"host", host,
			"src", src,
			"dst", dst,
			"size", file.Size,
		)

		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	// Start asynchronous job for downloading the newest versions of remaining files
	jobID, err := w.Client.RcloneCopyPaths(ctx, host, dstDir, srcDir, regularBatch)
	if err != nil {
		return 0, 0, errors.Wrap(err, "download batch to upload dir")
	}

	w.Logger.Info(ctx, "Started downloading files",
		"host", host,
		"job_id", jobID,
		"batch", regularBatch,
	)

	return jobID, versionedPr, nil
}

// chooseIDsForBatch returns slice of IDs of SSTables that the batch consists of.
func (w *indexWorker) chooseIDsForBatch(ctx context.Context, size int, host string) (takenIDs []string) {
	defer func() {
		w.increaseBatchSizeMetric(w.ClusterID, w.batchFromIDs(takenIDs), host)
	}()

	// All restore hosts are trying to get IDs for batch from the pool.
	// Pool is closed after the whole table has been restored.

	// Take at most batchSize IDs
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

// waitJob waits for rclone job to finish while updating its progress.
func (w *indexWorker) waitJob(ctx context.Context, pr *RestoreRunProgress) (err error) {
	defer func() {
		// On error stop job
		if err != nil {
			w.Logger.Info(ctx, "Stop job", "host", pr.Host, "run_progress", *pr, "error", err)
			if e := w.Client.RcloneJobStop(context.Background(), pr.Host, pr.AgentJobID); e != nil {
				w.Logger.Error(ctx, "Failed to stop job",
					"host", pr.Host,
					"id", pr.AgentJobID,
					"error", e,
				)
			}
		}
		// On exit clear stats
		if e := w.clearJobStats(context.Background(), pr.AgentJobID, pr.Host); e != nil {
			w.Logger.Error(ctx, "Failed to clear job stats",
				"host", pr.Host,
				"id", pr.AgentJobID,
				"error", e,
			)
		}
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		job, err := w.Client.RcloneJobProgress(ctx, pr.Host, pr.AgentJobID, w.Config.LongPollingTimeoutSeconds)
		if err != nil {
			return errors.Wrap(err, "fetch job info")
		}

		switch scyllaclient.RcloneJobStatus(job.Status) {
		case scyllaclient.JobError:
			return errors.Errorf("job error (%d): %s", pr.AgentJobID, job.Error)
		case scyllaclient.JobSuccess:
			w.updateDownloadProgress(ctx, pr, job)
			w.Logger.Info(ctx, "Batch download completed", "host", pr.Host, "job_id", pr.AgentJobID)
			return nil
		case scyllaclient.JobRunning:
			w.updateDownloadProgress(ctx, pr, job)
		case scyllaclient.JobNotFound:
			return errJobNotFound
		}
	}
}

func (w *indexWorker) updateDownloadProgress(ctx context.Context, pr *RestoreRunProgress, job *scyllaclient.RcloneJobProgress) {
	pr.DownloadStartedAt = nil
	// Set StartedAt and CompletedAt based on Job
	if t := time.Time(job.StartedAt); !t.IsZero() {
		pr.DownloadStartedAt = &t
	}
	pr.DownloadCompletedAt = nil
	if t := time.Time(job.CompletedAt); !t.IsZero() {
		pr.DownloadCompletedAt = &t
	}

	var (
		deltaDownloaded = job.Uploaded - pr.Downloaded
		deltaSkipped    = job.Skipped - pr.Skipped
		deltaFailed     = job.Failed - pr.Failed
	)

	pr.Error = job.Error
	pr.Downloaded = job.Uploaded
	pr.Skipped = job.Skipped
	pr.Failed = job.Failed

	w.metrics.UpdateFilesProgress(w.ClusterID, pr.ManifestPath, pr.Keyspace, pr.Table,
		deltaDownloaded, deltaSkipped, deltaFailed)

	w.insertRunProgress(ctx, pr)
}

func (w *indexWorker) restoreSSTables(ctx context.Context, host, keyspace, table string, loadAndStream, primaryReplicaOnly bool) error {
	const repeatInterval = 10 * time.Second

	w.Logger.Info(ctx, "Load SSTables for the first time",
		"host", host,
		"load_and_stream", loadAndStream,
		"primary_replica_only", primaryReplicaOnly,
	)

	running, err := w.Client.LoadSSTables(ctx, host, keyspace, table, loadAndStream, primaryReplicaOnly)
	if err == nil {
		w.Logger.Info(ctx, "Loading SSTables finished with success", "host", host)
		return nil
	}
	if !running {
		return err
	}

	w.Logger.Info(ctx, "Waiting for SSTables loading to finish, retry every 10 seconds",
		"host", host,
		"error", err,
	)

	ticker := time.NewTicker(repeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		running, err := w.Client.LoadSSTables(ctx, host, keyspace, table, loadAndStream, primaryReplicaOnly)
		if err == nil {
			w.Logger.Info(ctx, "Loading SSTables finished with success", "host", host)
			return nil
		}
		if running {
			w.Logger.Info(ctx, "Waiting for SSTables loading to finish",
				"host", host,
				"error", err,
			)
			continue
		}
		return err
	}
}

// batchFromIDs creates batch of SSTables with IDs present in ids.
func (w *indexWorker) batchFromIDs(ids []string) []string {
	var batch []string

	for _, id := range ids {
		batch = append(batch, w.bundles[id]...)
	}

	return batch
}

func (w *indexWorker) returnBatchToPool(ids []string, host string) {
	defer func() {
		w.decreaseBatchSizeMetric(w.ClusterID, w.batchFromIDs(ids), host)
	}()

	for _, id := range ids {
		w.bundleIDPool <- id
	}
}

func (w *indexWorker) drainBundleIDPool() []string {
	content := make([]string, 0)

	for len(w.bundleIDPool) > 0 {
		content = append(content, <-w.bundleIDPool)
	}

	return content
}

// SSTableCounter is concurrently safe counter used for checking
// how many SSTables are yet to be restored.
type SSTableCounter struct {
	*atomic.Int64
}

// newSSTableCounter returns new counter with the amount of tables to be restored.
// counter creates and initializes SSTableCounter
// for currently restored table.
func newSSTableCounter(value int64, hosts []restoreHost) SSTableCounter {
	ctr := SSTableCounter{atomic.NewInt64(value)}

	for _, h := range hosts {
		if h.OngoingRunProgress != nil {
			ctr.AddTables(len(h.OngoingRunProgress.SSTableID))
		}
	}

	return ctr
}

// AddTables increases counter by the amount of tables to be restored
// and returns counter's updated value.
func (ctr SSTableCounter) AddTables(v int) int {
	return int(ctr.Add(int64(v)))
}

// RestoreTables decreases counter by the amount of restored tables
// and returns counter's updated value.
func (ctr SSTableCounter) RestoreTables(v int) int {
	return int(ctr.Sub(int64(v)))
}
