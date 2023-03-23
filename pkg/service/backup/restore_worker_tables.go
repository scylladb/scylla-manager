// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"go.uber.org/atomic"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// restoreHost represents host that can be used for restoring files.
// If set, OngoingRunProgress represents unfinished RestoreRunProgress created in previous run.
type restoreHost struct {
	Host               string
	OngoingRunProgress *RestoreRunProgress
}

// bundle represents SSTables with the same ID.
type bundle []string

type tablesWorker struct {
	restoreWorkerTools

	hosts        []restoreHost     // Restore units created for currently restored location
	bundles      map[string]bundle // Maps bundle to it's ID
	bundleIDPool chan string       // IDs of the bundles that are yet to be restored
	resumed      bool              // Set to true if current run has already skipped all tables restored in previous run
}

// restoreData restores files from every location specified in restore target.
func (w *tablesWorker) restore(ctx context.Context, run *RestoreRun, target RestoreTarget) error {
	w.AwaitSchemaAgreement(ctx, w.clusterSession)
	w.initRemainingBytesMetric(ctx, run, target)

	w.Logger.Info(ctx, "Started restoring tables")
	defer w.Logger.Info(ctx, "Restoring tables finished")
	// Disable gc_grace_seconds
	for _, u := range run.Units {
		for _, t := range u.Tables {
			if err := w.DisableTableGGS(ctx, u.Keyspace, t.Table); err != nil {
				return err
			}
		}
	}
	// Restore files
	for _, l := range target.Location {
		if !w.resumed && run.Location != l.String() {
			w.Logger.Info(ctx, "Skipping location", "location", l)
			continue
		}
		if err := w.locationRestoreHandler(ctx, run, target, l); err != nil {
			return err
		}
	}

	return nil
}

func (w *tablesWorker) locationRestoreHandler(ctx context.Context, run *RestoreRun, target RestoreTarget, location Location) error {
	if !w.resumed && run.Location != location.String() {
		w.Logger.Info(ctx, "Skipping location", "location", location)
		return nil
	}

	w.Logger.Info(ctx, "Restoring location", "location", location)
	defer w.Logger.Info(ctx, "Restoring location finished", "location", location)

	w.location = location
	run.Location = location.String()

	if err := w.initHosts(ctx, run); err != nil {
		return errors.Wrap(err, "initialize hosts")
	}

	manifestHandler := func(miwc ManifestInfoWithContent) error {
		// Check if manifest has already been processed in previous run
		if !w.resumed && run.ManifestPath != miwc.Path() {
			w.Logger.Info(ctx, "Skipping manifest", "manifest", miwc.ManifestInfo)
			return nil
		}

		w.Logger.Info(ctx, "Restoring manifest", "manifest", miwc.ManifestInfo)
		defer w.Logger.Info(ctx, "Restoring manifest finished", "manifest", miwc.ManifestInfo)

		w.miwc = miwc
		run.ManifestPath = miwc.Path()

		return miwc.ForEachIndexIterWithError(target.Keyspace, w.filesMetaRestoreHandler(ctx, run, target))
	}

	return w.forEachRestoredManifest(ctx, w.location, manifestHandler)
}

func (w *tablesWorker) initRemainingBytesMetric(ctx context.Context, run *RestoreRun, target RestoreTarget) {
	for _, location := range target.Location {
		var restorePerLocationDataSize int64
		err := w.forEachRestoredManifest(
			ctx,
			location,
			func(miwc ManifestInfoWithContent) error {
				return miwc.ForEachIndexIterWithError(
					target.Keyspace,
					func(fm FilesMeta) error {
						restorePerLocationDataSize += fm.Size
						return nil
					})
			})
		if err != nil {
			w.Logger.Info(ctx, "Couldn't count restore data size", "location", w.location)
			continue
		}
		w.metrics.SetRemainingBytes(run.ClusterID, location, target.SnapshotTag, target.Keyspace, restorePerLocationDataSize)
	}
}

func (w *tablesWorker) filesMetaRestoreHandler(ctx context.Context, run *RestoreRun, target RestoreTarget) func(fm FilesMeta) error {
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
func (w *tablesWorker) workFunc(ctx context.Context, run *RestoreRun, target RestoreTarget, fm FilesMeta) error {
	version, err := w.GetTableVersion(ctx, fm.Keyspace, fm.Table)
	if err != nil {
		return err
	}

	var (
		srcDir = w.location.RemotePath(w.miwc.SSTableVersionDir(fm.Keyspace, fm.Table, fm.Version))
		dstDir = uploadTableDir(fm.Keyspace, fm.Table, version)
	)

	w.Logger.Info(ctx, "Found table's source and destination directory",
		"keyspace", fm.Keyspace,
		"table", fm.Table,
		"src_dir", srcDir,
		"dst_dir", dstDir,
	)

	ctr := w.newCounter()
	if ctr.RestoreTables(0) == 0 {
		w.Logger.Info(ctx, "Table does not have any more SSTables to restore",
			"keyspace", fm.Keyspace,
			"table", fm.Table,
		)
		return nil
	}
	if err = w.initVersionedFiles(ctx, w.hosts[0].Host, srcDir); err != nil {
		return err
	}
	// Every host has its personal goroutine which is responsible
	// for creating and downloading batches.
	// Goroutine returns only in case of error or
	// if the whole table has been restored.
	return parallel.Run(len(w.hosts), target.Parallel, func(n int) error {
		// Current goroutine's host
		h := &w.hosts[n]
		for {
			pr, err := w.prepareRunProgress(ctx, run, target, h, dstDir, srcDir)
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
					w.returnBatchToPool(pr.SSTableID)
					if cleanErr := w.cleanUploadDir(ctx, h.Host, dstDir, nil); cleanErr != nil {
						w.Logger.Error(ctx, "Couldn't clear destination directory", "host", h.Host, "error", cleanErr)
					}

					return errors.Wrapf(err, "wait on rclone job, id: %d, host: %s", pr.AgentJobID, h.Host)
				}
			}

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
				w.returnBatchToPool(pr.SSTableID)
				if cleanErr := w.cleanUploadDir(ctx, h.Host, dstDir, nil); cleanErr != nil {
					w.Logger.Error(ctx, "Couldn't clear destination directory", "host", h.Host, "error", cleanErr)
				}

				return errors.Wrapf(err, "call load and stream, host: %s", h.Host)
			}

			pr.setRestoreCompletedAt()
			w.insertRunProgress(ctx, pr)

			restoredBytes := pr.Downloaded + pr.Skipped
			w.metrics.UpdateRestoreProgress(w.ClusterID, pr.ManifestPath, pr.Keyspace, pr.Table, restoredBytes)
			w.metrics.DecreaseRemainingBytes(w.ClusterID, w.location, w.SnapshotTag, target.Keyspace, restoredBytes)

			w.Logger.Info(ctx, "Restored batch", "host", h.Host, "sstable_id", pr.SSTableID)
			// Close pool and free hosts awaiting on it if all SSTables have been successfully restored.
			if ctr.RestoreTables(len(pr.SSTableID)) == 0 {
				close(w.bundleIDPool)
			}
		}
	})
}

// initHosts creates hosts living in currently restored location's dc and with access to it.
// All running hosts are located at the beginning of the result slice.
func (w *tablesWorker) initHosts(ctx context.Context, run *RestoreRun) error {
	status, err := w.Client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get client status")
	}

	var (
		remotePath     = w.location.RemotePath("")
		locationStatus = status
	)
	// In case location does not have specified dc, use nodes from all dcs.
	if w.location.DC != "" {
		locationStatus = status.Datacenter([]string{w.location.DC})
		if len(locationStatus) == 0 {
			return errors.Errorf("no nodes in location's datacenter: %s", w.location)
		}
	}

	checkedNodes, err := w.Client.GetLiveNodesWithLocationAccess(ctx, locationStatus, remotePath)
	if err != nil {
		return errors.Wrap(err, "no live nodes in location's dc")
	}

	w.hosts = make([]restoreHost, 0)
	hostsInPool := strset.New()

	if !w.resumed {
		// Place hosts with unfinished jobs at the beginning
		cb := func(pr *RestoreRunProgress) {
			if !validateTimeIsSet(pr.RestoreCompletedAt) {
				// Pointer cannot be stored directly because it is overwritten in each
				// iteration of ForEachTableProgress.
				ongoing := *pr
				// Reset rclone stats for unfinished rclone jobs - they will be recreated from rclone job progress.
				if !validateTimeIsSet(pr.DownloadCompletedAt) {
					pr.Downloaded = 0
					pr.Skipped = 0
					pr.Failed = 0
				}
				w.hosts = append(w.hosts, restoreHost{
					Host:               pr.Host,
					OngoingRunProgress: &ongoing,
				})

				hostsInPool.Add(pr.Host)
			}
		}

		w.ForEachTableProgress(ctx, run, cb)
	}

	// Place free hosts in the pool
	for _, n := range checkedNodes {
		if !hostsInPool.Has(n.Addr) {
			w.hosts = append(w.hosts, restoreHost{
				Host: n.Addr,
			})

			hostsInPool.Add(n.Addr)
		}
	}

	w.Logger.Info(ctx, "Initialized restore hosts", "hosts", w.hosts)

	return nil
}

// initBundlePool creates bundles and pool of their IDs that have yet to be restored.
// (It does not include ones that are currently being restored).
func (w *tablesWorker) initBundlePool(ctx context.Context, run *RestoreRun, sstables []string) {
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
func (w *tablesWorker) prepareRunProgress(ctx context.Context, run *RestoreRun, target RestoreTarget, h *restoreHost, dstDir, srcDir string,
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
func (w *tablesWorker) reactivateRunProgress(ctx context.Context, pr *RestoreRunProgress, dstDir, srcDir string) error {
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
	w.updateBatchSizeMetric(ctx, pr.ClusterID, batch, pr.Host, srcDir)

	if err := w.cleanUploadDir(ctx, pr.Host, dstDir, batch); err != nil {
		w.Logger.Error(ctx, "Couldn't clear destination directory", "host", pr.Host, "error", err)
	}

	jobID, versionedPr, err := w.startDownload(ctx, pr.Host, dstDir, srcDir, batch)
	if err != nil {
		w.deleteRunProgress(ctx, pr)
		w.returnBatchToPool(pr.SSTableID)
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
func (w *tablesWorker) newRunProgress(ctx context.Context, run *RestoreRun, target RestoreTarget, h *restoreHost, dstDir, srcDir string,
) (*RestoreRunProgress, error) {
	if err := w.checkAvailableDiskSpace(ctx, hostInfo{IP: h.Host}); err != nil {
		return nil, errors.Wrap(err, "validate free disk space")
	}

	takenIDs := w.chooseIDsForBatch(ctx, target.BatchSize)
	if ctx.Err() != nil {
		w.returnBatchToPool(takenIDs)
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
	w.updateBatchSizeMetric(ctx, run.ClusterID, batch, h.Host, srcDir)

	if err := w.cleanUploadDir(ctx, h.Host, dstDir, nil); err != nil {
		w.Logger.Error(ctx, "Couldn't clear destination directory", "host", h.Host, "error", err)
	}

	jobID, versionedPr, err := w.startDownload(ctx, h.Host, dstDir, srcDir, batch)
	if err != nil {
		w.returnBatchToPool(takenIDs)
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

func (w *tablesWorker) updateBatchSizeMetric(ctx context.Context, clusterID uuid.UUID, batch []string, host, srcDir string) {
	var batchSize int64
	for _, file := range batch {
		fi, err := w.Client.RcloneFileInfo(ctx, host, srcDir+"/"+file)
		if err != nil {
			w.Logger.Error(ctx, "Failed to check file size", "err", err)
		}
		batchSize += fi.Size
	}
	w.metrics.UpdateBatchSize(clusterID, host, batchSize)
}

// startDownload creates rclone job responsible for downloading batch of SSTables.
// Downloading of versioned files happens first in a synchronous way.
// It returns jobID for asynchronous download of the newest versions of files
// alongside with the size of the already downloaded versioned files.
func (w *tablesWorker) startDownload(ctx context.Context, host, dstDir, srcDir string, batch []string) (int64, int64, error) {
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
func (w *tablesWorker) chooseIDsForBatch(ctx context.Context, size int) []string {
	var takenIDs []string

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
func (w *tablesWorker) waitJob(ctx context.Context, pr *RestoreRunProgress) (err error) {
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

func (w *tablesWorker) updateDownloadProgress(ctx context.Context, pr *RestoreRunProgress, job *scyllaclient.RcloneJobProgress) {
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

func (w *tablesWorker) restoreSSTables(ctx context.Context, host, keyspace, table string, loadAndStream, primaryReplicaOnly bool) error {
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
func (w *tablesWorker) batchFromIDs(ids []string) []string {
	var batch []string

	for _, id := range ids {
		batch = append(batch, w.bundles[id]...)
	}

	return batch
}

func (w *tablesWorker) returnBatchToPool(ids []string) {
	for _, id := range ids {
		w.bundleIDPool <- id
	}
}

func (w *tablesWorker) drainBundleIDPool() []string {
	content := make([]string, 0)

	for len(w.bundleIDPool) > 0 {
		content = append(content, <-w.bundleIDPool)
	}

	return content
}

func (w *tablesWorker) startFromScratch() {
	w.resumed = true
}

// SSTableCounter is concurrently safe counter used for checking
// how many SSTables are yet to be restored.
type SSTableCounter struct {
	*atomic.Int64
}

// NewSSTableCounter returns new counter with the amount of tables to be restored.
func NewSSTableCounter(v int) SSTableCounter {
	return SSTableCounter{atomic.NewInt64(int64(v))}
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

// counter creates and initializes SSTableCounter
// for currently restored table.
func (w *tablesWorker) newCounter() SSTableCounter {
	ctr := NewSSTableCounter(len(w.bundleIDPool))

	for _, h := range w.hosts {
		if h.OngoingRunProgress != nil {
			ctr.AddTables(len(h.OngoingRunProgress.SSTableID))
		}
	}

	return ctr
}
