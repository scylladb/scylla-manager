// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"path"
	"slices"
	"sync"
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
)

// tablesDirWorker is responsible for restoring table files from manifest in parallel.
type tablesDirWorker struct {
	worker

	hostToShardCnt map[string]uint
	dispatcher     *batchDispatcher

	dstDir string                  // "data:" prefixed path to upload dir (common for every host)
	srcDir string                  // Full path to remote directory with backed-up files
	miwc   ManifestInfoWithContent // Manifest containing fm
	fm     FilesMeta               // Describes table and it's files located in srcDir

	// Maps original SSTable name to its existing older version
	// (with respect to currently restored snapshot tag)
	// that should be used during the restore procedure.
	versionedFiles VersionedMap
	fileSizesCache map[string]int64
	progress       *TotalRestoreProgress
}

func newTablesDirWorker(ctx context.Context, w worker, miwc ManifestInfoWithContent, fm FilesMeta, progress *TotalRestoreProgress) (tablesDirWorker, error) {
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
	for _, h := range hosts {
		if err := w.client.RcloneSetBandwidthLimit(ctx, h, 0); err != nil {
			return tablesDirWorker{}, errors.Wrapf(err, "%s: disable bandwidth limit", h)
		}
	}

	hostToShardCnt, err := w.client.HostsShardCount(ctx, hosts)
	if err != nil {
		return tablesDirWorker{}, err
	}

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
		hostToShardCnt: hostToShardCnt,
		dispatcher:     newBatchDispatcher(fm, fileSizesCache, w.target.BatchSize, len(hosts)),
		dstDir:         dstDir,
		srcDir:         srcDir,
		miwc:           miwc,
		fm:             fm,
		versionedFiles: versionedFiles,
		fileSizesCache: fileSizesCache,
		progress:       progress,
	}, nil
}

// restore SSTables of receivers manifest/table in parallel.
func (w *tablesDirWorker) restore(ctx context.Context) error {
	if len(w.dispatcher.sstables) > 0 {
		small := w.dispatcher.sstables[len(w.dispatcher.sstables)-1].size
		big := w.dispatcher.sstables[0].size
		distance := big - small

		bigCut := big - distance/10
		smallCut := small + distance/10

		var bigCutCnt, smallCutCnt, avg int64
		for _, sst := range w.dispatcher.sstables {
			avg += sst.size
			if sst.size > bigCut {
				bigCutCnt++
			}
			if sst.size < smallCut {
				smallCutCnt++
			}
		}
		avg /= int64(len(w.dispatcher.sstables))

		w.logger.Info(ctx, "BATCHING STATS",
			"sstable cnt", len(w.dispatcher.sstables),
			"total size", w.dispatcher.size,
			"avg size", avg,
			"max size", big,
			"min size", small,
			"top 10 percent biggest sstable cnt", bigCutCnt,
			"top 10 percent smallest sstable cnt", smallCutCnt,
		)
	}

	hosts := w.target.locationHosts[w.miwc.Location]
	f := func(n int) (err error) {
		h := hosts[n]
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
	doneIDs := strset.New()
	err := bind.ForEachTableProgress(w.session, func(pr *RunProgress) {
		if validateTimeIsSet(pr.RestoreCompletedAt) {
			doneIDs.Add(pr.SSTableID...)
		} else {
			w.deleteRunProgress(context.Background(), pr)
		}
	})
	if err != nil {
		return err
	}

	// Remove already started ID from the pool
	w.dispatcher.dropIDs(doneIDs)
	return nil
}

// newRunProgress creates RunProgress by assembling batch and starting download to host's upload dir.
func (w *tablesDirWorker) newRunProgress(ctx context.Context, host string) (*RunProgress, error) {
	if err := w.checkAvailableDiskSpace(ctx, host); err != nil {
		return nil, errors.Wrap(err, "validate free disk space")
	}

	batch := w.createBatch(host)
	if batch == nil {
		return nil, nil //nolint: nilnil
	}

	w.logger.Info(ctx, "Created new batch",
		"host", host,
		"batch", batchIDs(batch),
		"size", batchSize(batch),
	)

	if err := w.cleanUploadDir(ctx, host, w.dstDir, nil); err != nil {
		return nil, errors.Wrapf(err, "clean upload dir of host %s", host)
	}

	jobID, versionedPr, err := w.startDownload(ctx, host, batch)
	if err != nil {
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
		SSTableID:         batchIDs(batch),
		VersionedProgress: versionedPr,
	}

	w.insertRunProgress(ctx, pr)
	return pr, nil
}

// startDownload creates rclone job responsible for downloading batch of SSTables.
// Downloading of versioned files happens first in a synchronous way.
// It returns jobID for asynchronous download of the newest versions of files
// alongside with the size of the already downloaded versioned files.
func (w *tablesDirWorker) startDownload(ctx context.Context, host string, batch []bundle) (jobID, versionedPr int64, err error) {
	var (
		regularBatch   = make([]string, 0)
		versionedBatch = make([]VersionedSSTable, 0)
	)
	// Decide which files require to be downloaded in their older version
	for _, sst := range batch {
		for _, f := range sst.files {
			if v, ok := w.versionedFiles[f]; ok {
				versionedBatch = append(versionedBatch, v)
				versionedPr += v.Size
			} else {
				regularBatch = append(regularBatch, f)
			}
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

func (w *tablesDirWorker) createBatch(host string) []bundle {
	batch := w.dispatcher.createBatch(w.hostToShardCnt[host])
	w.increaseBatchSizeMetric(w.run.ClusterID, batch, host)
	return batch
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

func (w *tablesDirWorker) increaseBatchSizeMetric(clusterID uuid.UUID, batch []bundle, host string) {
	w.metrics.IncreaseBatchSize(clusterID, host, batchSize(batch))
}

func (w *tablesDirWorker) cleanupRunProgress(ctx context.Context, pr *RunProgress) {
	w.deleteRunProgress(ctx, pr)
	if cleanErr := w.cleanUploadDir(ctx, pr.Host, w.dstDir, nil); cleanErr != nil {
		w.logger.Error(ctx, "Couldn't clear destination directory", "host", pr.Host, "error", cleanErr)
	}
}

func batchIDs(sstables []bundle) []string {
	var out []string
	for _, sst := range sstables {
		out = append(out, sst.id)
	}
	return out
}

func batchSize(sstables []bundle) int64 {
	var out int64
	for _, sst := range sstables {
		out += sst.size
	}
	return out
}

type bundle struct {
	id    string
	size  int64
	files []string
}

type batchDispatcher struct {
	mu                  sync.Mutex
	batchSize           int
	expectedPerNodeWork int64
	size                int64
	sstables            []bundle
}

func newBatchDispatcher(fm FilesMeta, sizes map[string]int64, batchSize, nodeCnt int) *batchDispatcher {
	m := make(map[string]bundle, len(fm.Files)/9)
	for _, f := range fm.Files {
		id, err := sstable.ExtractID(f)
		if err != nil {
			panic(err)
		}

		sst := m[id]
		sst.id = id
		sst.size += sizes[f]
		sst.files = append(sst.files, f)
		m[id] = sst
	}

	out := make([]bundle, 0, len(m))
	var total int64
	for _, sst := range m {
		total += sst.size
		out = append(out, sst)
	}

	slices.SortFunc(out, func(a, b bundle) int {
		switch {
		case a.size > b.size:
			return -1
		case a.size < b.size:
			return 1
		default:
			return 0
		}
	})

	return &batchDispatcher{
		mu:                  sync.Mutex{},
		batchSize:           batchSize,
		expectedPerNodeWork: total / int64(nodeCnt),
		size:                total,
		sstables:            out,
	}
}

func (b *batchDispatcher) dropIDs(ids *strset.Set) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var (
		sstables []bundle
		size     int64
	)
	for _, sst := range b.sstables {
		if !ids.Has(sst.id) {
			sstables = append(sstables, sst)
			size += sst.size
		}
	}
	b.sstables = sstables
	b.size = size
}

func (b *batchDispatcher) createBatch(shardCnt uint) []bundle {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.sstables) == 0 {
		return nil
	}

	var (
		out  []bundle
		size int64
		idx  int
	)
	for i := 0; i < b.batchSize; i++ {
		for j := 0; j < int(shardCnt); j++ {
			out = append(out, b.sstables[idx])
			size += b.sstables[idx].size
			idx++
			if idx >= len(b.sstables) {
				break
			}
		}
		if idx >= len(b.sstables) || size > b.expectedPerNodeWork/2 {
			break
		}
	}

	b.size -= size
	b.sstables = b.sstables[idx:]
	return out
}
