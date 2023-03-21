// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"
	"math/rand"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"go.uber.org/atomic"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
)

// restoreData restores files from every location specified in restore target.
func (w *restoreWorker) restoreData(ctx context.Context, run *RestoreRun, target RestoreTarget) error {
	w.AwaitSchemaAgreement(ctx, w.clusterSession)

	for _, w.location = range target.Location {
		w.Logger.Info(ctx, "Restoring location", "location", w.location)
		// hosts should be initialized once per location
		w.hosts = nil

		manifestHandler := func(miwc ManifestInfoWithContent) error {
			// Check if manifest has already been processed in previous run
			if !w.resumed && run.ManifestPath != miwc.Path() {
				return nil
			}

			w.Logger.Info(ctx, "Restoring manifest", "manifest", miwc.ManifestInfo)
			defer w.Logger.Info(ctx, "Restoring manifest finished", "manifest", miwc.ManifestInfo)

			w.miwc = miwc
			run.ManifestPath = miwc.Path()

			return miwc.ForEachIndexIterWithError(target.Keyspace, w.restoreFiles(ctx, run, target))
		}

		if err := w.forEachRestoredManifest(ctx, w.location, manifestHandler); err != nil {
			w.Logger.Info(ctx, "Restoring location finished", "location", w.location)
			return err
		}

		w.Logger.Info(ctx, "Restoring location finished", "location", w.location)
	}

	w.Logger.Info(ctx, "Restoring data finished")

	return nil
}

// restoreFiles returns function that restores files from manifest's table.
func (w *restoreWorker) restoreFiles(ctx context.Context, run *RestoreRun, target RestoreTarget) func(fm FilesMeta) error {
	return func(fm FilesMeta) error {
		if !w.resumed {
			// Check if table has already been processed in previous run
			if run.Keyspace != fm.Keyspace || run.Table != fm.Table {
				return nil
			}
		}

		w.Logger.Info(ctx, "Restoring table",
			"keyspace", fm.Keyspace,
			"table", fm.Table,
		)
		defer w.Logger.Info(ctx, "Restoring table finished",
			"keyspace", fm.Keyspace,
			"table", fm.Table,
		)

		run.Table = fm.Table
		run.Keyspace = fm.Keyspace
		w.insertRun(ctx, run)

		w.initBundlePool(ctx, run, fm.Files)

		// Hosts should be initialized once per location
		if w.hosts == nil {
			err := w.initHosts(ctx, run)
			if err != nil {
				return errors.Wrap(err, "initialize hosts")
			}
		}

		// Set resumed only after all initializations as they depend on knowing
		// if current table have been processed by the previous run.
		w.resumed = true

		filesHandler := func() error {
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

		// It's not possible to disable compaction and gc_grace_seconds on system table
		if target.RestoreSchema {
			return filesHandler()
		}

		return w.ExecOnDisabledTable(ctx, fm.Keyspace, fm.Table, filesHandler)
	}
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
func (w *restoreWorker) newCounter() SSTableCounter {
	ctr := NewSSTableCounter(len(w.bundleIDPool))

	for _, h := range w.hosts {
		if h.OngoingRunProgress != nil {
			ctr.AddTables(len(h.OngoingRunProgress.SSTableID))
		}
	}

	return ctr
}

// workFunc is responsible for creating and restoring batches on multiple hosts (possibly in parallel).
// It requires previous initialization of restore worker components.
func (w *restoreWorker) workFunc(ctx context.Context, run *RestoreRun, target RestoreTarget, fm FilesMeta) error {
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
	if err = w.initVersionedFiles(ctx, srcDir); err != nil {
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
				w.Logger.Info(ctx, "Waiting for job",
					"host", h.Host,
					"job_id", pr.AgentJobID,
				)

				if err = w.waitJob(ctx, pr); err != nil {
					if ctx.Err() != nil {
						return parallel.Abort(ctx.Err())
					}
					// As run progress could have already been inserted
					// into the database, it should be deleted.
					w.deleteRunProgress(ctx, pr)
					w.cleanUploadDir(ctx, h.Host, dstDir, nil)
					w.returnBatchToPool(pr.SSTableID)

					return errors.Wrapf(err, "wait on rclone job, id: %d, host: %s", pr.AgentJobID, h.Host)
				}
			}

			w.Logger.Info(ctx, "Call load and stream", "host", h.Host)

			if !validateTimeIsSet(pr.RestoreStartedAt) {
				pr.setRestoreStartedAt()
				w.insertRunProgress(ctx, pr)
			}

			if err = w.restoreSSTables(ctx, h.Host, fm.Keyspace, fm.Table); err != nil {
				if ctx.Err() != nil {
					w.Logger.Info(ctx, "Stop load and stream: canceled context", "host", h.Host)
					return parallel.Abort(ctx.Err())
				}
				w.deleteRunProgress(ctx, pr)
				w.cleanUploadDir(ctx, h.Host, dstDir, nil)
				w.returnBatchToPool(pr.SSTableID)

				return errors.Wrapf(err, "call load and stream, host: %s", h.Host)
			}

			pr.setRestoreCompletedAt()
			w.insertRunProgress(ctx, pr)

			w.metrics.UpdateRestoreProgress(w.ClusterID, pr.ManifestPath, pr.Keyspace, pr.Table, pr.Downloaded+pr.Skipped)

			w.Logger.Info(ctx, "Restored batch",
				"host", h.Host,
				"sstable_id", pr.SSTableID,
			)

			// Close pool and free hosts awaiting on it if all SSTables have been successfully restored.
			if ctr.RestoreTables(len(pr.SSTableID)) == 0 {
				close(w.bundleIDPool)
			}
		}
	})
}

// initBundlePool creates bundles and pool of their IDs that have yet to be restored.
// (It does not include ones that are currently being restored).
func (w *restoreWorker) initBundlePool(ctx context.Context, run *RestoreRun, sstables []string) {
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

// initHosts creates hosts living in currently restored location's dc and with access to it.
// All running hosts are located at the beginning of the result slice.
func (w *restoreWorker) initHosts(ctx context.Context, run *RestoreRun) error {
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
				sh, err := w.Client.ShardCount(ctx, pr.Host)
				if err != nil {
					w.Logger.Info(ctx, "Couldn't get host shard count",
						"host", pr.Host,
						"error", err,
					)
					return
				}
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
					Shards:             sh,
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
			sh, err := w.Client.ShardCount(ctx, n.Addr)
			if err != nil {
				w.Logger.Info(ctx, "Couldn't get host shard count",
					"host", n.Addr,
					"error", err,
				)
				continue
			}

			w.hosts = append(w.hosts, restoreHost{
				Host:   n.Addr,
				Shards: sh,
			})

			hostsInPool.Add(n.Addr)
		}
	}

	w.Logger.Info(ctx, "Initialized restore hosts", "hosts", w.hosts)

	return nil
}

// initVersionedFiles gathers information about existing versioned files.
func (w *restoreWorker) initVersionedFiles(ctx context.Context, dir string) error {
	w.versionedFiles = make(map[string][]versionedSSTable)
	opts := &scyllaclient.RcloneListDirOpts{
		FilesOnly:     true,
		VersionedOnly: true,
	}
	f := func(item *scyllaclient.RcloneListDirItem) {
		tagExt := path.Ext(item.Name)
		regularName := strings.TrimSuffix(item.Name, tagExt)
		w.versionedFiles[regularName] = append(w.versionedFiles[regularName], versionedSSTable{
			Name:    regularName,
			Version: tagExt[1:], // Remove dot from extension
			Size:    item.Size,
		})
	}
	// We can choose any host for remote location listing
	host := w.hosts[rand.Intn(len(w.hosts))].Host
	if err := w.Client.RcloneListDirIter(ctx, host, dir, opts, f); err != nil {
		w.versionedFiles = nil
		return errors.Wrapf(err, "host %s: listing versioned SSTables", host)
	}
	// Sort versions (ascending snapshot tag time) for easier lookup
	for _, versions := range w.versionedFiles {
		sort.Slice(versions, func(i, j int) bool {
			return versions[i].Version < versions[j].Version
		})
	}

	w.Logger.Info(ctx, "Listed versioned SSTables",
		"host", host,
		"dir", dir,
		"versionedSSTables", w.versionedFiles,
	)

	return nil
}

// prepareRunProgress either reactivates RestoreRunProgress created in previous run
// or it creates a brand new RestoreRunProgress.
func (w *restoreWorker) prepareRunProgress(ctx context.Context, run *RestoreRun, target RestoreTarget, h *restoreHost, dstDir, srcDir string,
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

// reactivateRunProgress ensures that RestoreRunProgress created in previous run can be resumed.
// In case previous run interrupted rclone job, it has to be recreated (rclone jobs cannot be resumed).
func (w *restoreWorker) reactivateRunProgress(ctx context.Context, pr *RestoreRunProgress, dstDir, srcDir string) error {
	// Nothing to do if download has already finished
	if validateTimeIsSet(pr.DownloadCompletedAt) {
		return nil
	}

	batch := w.batchFromIDs(pr.SSTableID)
	w.cleanUploadDir(ctx, pr.Host, dstDir, batch)

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
	w.metrics.UpdateFilesProgress(pr.ClusterID, pr.ManifestPath, pr.Keyspace, pr.Table, pr.Host, 0, pr.VersionedProgress, 0)

	return nil
}

// newRunProgress creates RestoreRunProgress by assembling batch and starting download to host's upload dir.
func (w *restoreWorker) newRunProgress(ctx context.Context, run *RestoreRun, target RestoreTarget, h *restoreHost, dstDir, srcDir string,
) (*RestoreRunProgress, error) {
	if err := w.checkAvailableDiskSpace(ctx, hostInfo{IP: h.Host}); err != nil {
		return nil, errors.Wrap(err, "validate free disk space")
	}

	takenIDs := w.chooseIDsForBatch(ctx, h.Shards, target.BatchSize)
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
	w.cleanUploadDir(ctx, h.Host, dstDir, nil)

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
	w.metrics.UpdateFilesProgress(pr.ClusterID, pr.ManifestPath, pr.Keyspace, pr.Table, pr.Host, pr.VersionedProgress, 0, 0)

	return pr, nil
}

// startDownload creates rclone job responsible for downloading batch of SSTables.
// Downloading of versioned files happens first in a synchronous way.
// It returns jobID for asynchronous download of the newest versions of files
// alongside with the size of the already downloaded versioned files.
func (w *restoreWorker) startDownload(ctx context.Context, host, dstDir, srcDir string, batch []string) (int64, int64, error) {
	var (
		restoreT, _    = SnapshotTagTime(w.SnapshotTag) // nolint: errcheck
		regularBatch   = make([]string, 0)
		versionedBatch = make([]versionedSSTable, 0)
		versionedPr    int64
	)
	// Decide which files require to be downloaded in their older version
	for _, file := range batch {
		if versions, ok := w.versionedFiles[file]; ok {
			var match versionedSSTable
			for _, v := range versions {
				t, _ := SnapshotTagTime(v.Version) // nolint: errcheck
				if restoreT.Before(t) {
					match = v
					break
				}
			}

			if match.Version != "" {
				versionedBatch = append(versionedBatch, match)
				versionedPr += match.Size
				continue
			}
		}

		regularBatch = append(regularBatch, file)
	}
	// Downloading versioned files requires us to rename them (strip snapshot tag suffix)
	// and function RcloneCopyPaths lacks this option. In order to achieve that, we move
	// all versioned files one by one with RcloneMoveFile (which supports renaming files).
	// The assumption is that the existence of versioned files is low and that they
	// are rather small, so we can do it in a synchronous way.
	// Moving files can be done in full parallel because of rclone ability to limit transfers.
	err := parallel.Run(len(versionedBatch), parallel.NoLimit, func(i int) error {
		file := versionedBatch[i]

		dst := path.Join(dstDir, file.Name)
		src := path.Join(srcDir, strings.Join([]string{file.Name, file.Version}, "."))

		if err := w.Client.RcloneMoveFile(ctx, host, dst, src); err != nil {
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
func (w *restoreWorker) chooseIDsForBatch(ctx context.Context, shards uint, size int) []string {
	var (
		batchSize = size * int(shards)
		takenIDs  []string
	)

	// All restore hosts are trying to get IDs for batch from the pool.
	// Pool is closed after the whole table has been restored.

	// Take at most batchSize IDs
	for i := 0; i < batchSize; i++ {
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
func (w *restoreWorker) waitJob(ctx context.Context, pr *RestoreRunProgress) (err error) {
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

func (w *restoreWorker) updateDownloadProgress(ctx context.Context, pr *RestoreRunProgress, job *scyllaclient.RcloneJobProgress) {
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

	w.metrics.UpdateFilesProgress(w.ClusterID, pr.ManifestPath, pr.Keyspace, pr.Table, pr.Host,
		deltaDownloaded, deltaSkipped, deltaFailed)

	w.insertRunProgress(ctx, pr)
}

func (w *restoreWorker) restoreSSTables(ctx context.Context, host, keyspace, tables string) error {
	const (
		initialInterval = 15 * time.Second
		maxInterval     = time.Minute
		maxElapsedTime  = time.Hour
		multiplier      = 2
		jitter          = 0.2
	)

	backoff := retry.NewExponentialBackoff(
		initialInterval,
		maxElapsedTime,
		maxInterval,
		multiplier,
		jitter,
	)

	notify := func(err error, wait time.Duration) {
		w.Logger.Info(ctx, "Previous call to load and stream is still running", "wait", wait)
	}

	return retry.WithNotify(ctx, func() error {
		err := w.Client.LoadSSTables(ctx, host, keyspace, tables, true, true)
		if err == nil {
			return nil
		}
		if strings.Contains(err.Error(), "Already loading SSTables") {
			return errors.New("already loading SSTables")
		}
		return retry.Permanent(err)
	}, backoff, notify)
}

// cleanUploadDir deletes all SSTables from host's upload directory
// except for those present in excludedFiles.
func (w *restoreWorker) cleanUploadDir(ctx context.Context, host, uploadDir string, excludedFiles []string) {
	s := strset.New(excludedFiles...)
	var filesToBeDeleted []string

	getFilesToBeDeleted := func(item *scyllaclient.RcloneListDirItem) {
		if !s.Has(item.Name) {
			filesToBeDeleted = append(filesToBeDeleted, item.Name)
		}
	}

	opts := &scyllaclient.RcloneListDirOpts{FilesOnly: true}
	if err := w.Client.RcloneListDirIter(ctx, host, uploadDir, opts, getFilesToBeDeleted); err != nil {
		w.Logger.Error(ctx, "Couldn't list upload directory - it might contain corrupted files that need to be deleted manually",
			"host", host,
			"upload_dir", uploadDir,
			"error", err,
		)
	}

	if len(filesToBeDeleted) > 0 {
		w.Logger.Info(ctx, "Delete files from host's upload directory",
			"host", host,
			"upload_dir", uploadDir,
			"files", filesToBeDeleted,
		)
	}

	for _, f := range filesToBeDeleted {
		remotePath := path.Join(uploadDir, f)
		if err := w.Client.RcloneDeleteFile(ctx, host, remotePath); err != nil {
			w.Logger.Error(ctx, "Couldn't delete file in upload directory - it has to be done manually",
				"host", host,
				"file", remotePath,
				"error", err,
			)
		}
	}
}

// batchFromIDs creates batch of SSTables with IDs present in ids.
func (w *restoreWorker) batchFromIDs(ids []string) []string {
	var batch []string

	for _, id := range ids {
		batch = append(batch, w.bundles[id]...)
	}

	return batch
}

func (w *restoreWorker) returnBatchToPool(ids []string) {
	for _, id := range ids {
		w.bundleIDPool <- id
	}
}

func (w *restoreWorker) drainBundleIDPool() []string {
	content := make([]string, 0)

	for len(w.bundleIDPool) > 0 {
		content = append(content, <-w.bundleIDPool)
	}

	return content
}

// sstableID returns ID from SSTable name.
// Supported SSTable format versions are: "mc", "md", "me", "la", "ka".
// Scylla code validating SSTable format can be found here:
// https://github.com/scylladb/scylladb/blob/2c1ef0d2b768a793c284fc68944526179bfd0171/sstables/sstables.cc#L2333
func sstableID(sstable string) string {
	parts := strings.Split(sstable, "-")

	if regexLaMx.MatchString(sstable) {
		return parts[1]
	}
	if regexKa.MatchString(sstable) {
		return parts[3]
	}

	panic("Unknown SSTable format version: " + sstable + ". Supported versions are: 'mc', 'md', 'me', 'la', 'ka'")
}

var (
	regexLaMx = regexp.MustCompile(`(la|m[cde])-(\d+)-(\w+)-(.*)`)
	regexKa   = regexp.MustCompile(`(\w+)-(\w+)-ka-(\d+)-(.*)`)
)
