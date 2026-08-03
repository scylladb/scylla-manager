// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"
	stdErr "errors"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"golang.org/x/sync/errgroup"
)

// retentionLockConfig holds retention lock parameters.
type retentionLockConfig struct {
	until    time.Time
	mode     RetentionLockMode
	override bool
}

// RetentionLock sets object retention locks on all backup files.
// Schema files are locked first, then sstables and scylla manifests,
// finalizing with SM manifests.
func (w *worker) RetentionLock(egCtx context.Context, hosts []hostInfo, target Target) error {
	cfg := retentionLockConfig{
		mode:     target.RetentionLockMode,
		override: target.OverrideRetentionLock,
	}
	if cfg.mode == RetentionLockUnlocked || cfg.mode == RetentionLockLocked {
		lockUntil, err := retentionLockUntil(w.SnapshotTag, target.RetentionDays)
		if err != nil {
			return err
		}
		cfg.until = lockUntil
	}

	// Lock schema files separately, as they are per location, not per host
	if !target.SkipSchema {
		if err := w.lockSchemaFiles(egCtx, hosts, cfg); err != nil {
			return errors.Wrap(err, "lock schema files")
		}
	}

	// Lock per-host files in parallel.
	// Since it requires storing manifest contents in memory,
	// we need to limit parallelism to the same level,
	// as when uploading manifests.
	eg, egCtx := errgroup.WithContext(egCtx)
	eg.SetLimit(MaxManifestInMemory)

	for i := range hosts {
		eg.Go(func() error {
			if err := w.lockHostFiles(egCtx, &hosts[i], cfg); err != nil {
				return errors.Wrapf(err, "lock host %s files", hosts[i].IP)
			}
			return nil
		})
	}

	return eg.Wait()
}

// lockSchemaFiles locks CQL and alternator schema files for all locations.
func (w *worker) lockSchemaFiles(ctx context.Context, hosts []hostInfo, cfg retentionLockConfig) error {
	doneLocations := make(map[string]struct{})
	for i := range hosts {
		if _, ok := doneLocations[hosts[i].Location.StringWithoutDC()]; ok {
			continue
		}
		doneLocations[hosts[i].Location.StringWithoutDC()] = struct{}{}
		w.Logger.Info(ctx, "Locking schema files", "location", hosts[i].Location.StringWithoutDC(), "host", hosts[i].IP)

		cqlPath := backupspec.RemoteSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag)
		paths := []string{path.Base(cqlPath)}
		if _, ok := getAlternatorHost(hosts); ok {
			paths = append(paths, path.Base(backupspec.AlternatorSchemaPath(w.ClusterID, w.TaskID, w.SnapshotTag)))
		}
		remoteSchemaDir := hosts[i].Location.RemotePath(path.Dir(cqlPath))

		switch cfg.mode {
		case RetentionLockUnlocked, RetentionLockLocked:
			if err := w.batchLock(ctx, hosts[i].IP, remoteSchemaDir, paths, cfg, "", ""); err != nil {
				return errors.Wrapf(err, "await retention lock with node %s", hosts[i].IP)
			}
		case RetentionLockEventBasedHold:
			// Initialize holdHandler
			apply := func(ctx context.Context, paths []string, hold bool) error {
				return w.holdAndWait(ctx, hosts[i].IP, remoteSchemaDir, paths, hold, "", "")
			}
			holdHandler := newEventBasedHoldHandler(apply, eventBasedHoldBatchSize)
			// Feed local files - even though schema files have already been uploaded,
			// we still treat them as local for the purposes of applying/removing the hold.
			for _, p := range paths {
				holdHandler.addLocal(p)
			}
			holdHandler.finalizeLocal()
			// Feed remote files - limit to schema files coming from the same task ID,
			// so that behavior is consistent with SM manifest holds.
			opts := &scyllaclient.RcloneListDirOpts{
				FilesOnly:          true,
				ShowEventBasedHold: true,
			}
			listErr := w.Client.RcloneListDirIter(ctx, hosts[i].IP, remoteSchemaDir, opts, func(item *scyllaclient.RcloneListDirItem) {
				taskID, _, err := ParseSchemaFileName(item.Name)
				if err == nil && taskID == w.TaskID {
					holdHandler.addRemote(ctx, item.Name, item.EventBasedHold)
				}
			})
			if err := stdErr.Join(
				errors.Wrap(listErr, "list schema files holds"),
				errors.Wrap(holdHandler.finalize(ctx), "finalize remote schema files holds"),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// lockHostFiles locks sstables, scylla manifests, and SM manifest for given host.
func (w *worker) lockHostFiles(ctx context.Context, h *hostInfo, cfg retentionLockConfig) (err error) {
	w.Logger.Info(ctx, "Locking host files", "host", h.IP)

	manifestPath := backupspec.RemoteManifestFile(w.ClusterID, w.TaskID, w.SnapshotTag, h.DC, h.ID)
	remoteManifestPath := h.Location.RemotePath(manifestPath)
	remoteManifestDir := path.Dir(remoteManifestPath)
	switch cfg.mode {
	case RetentionLockUnlocked, RetentionLockLocked:
		// As we want to make this stage resumable, to discover files that
		// need to be locked, we need to download manifest from backup location.
		r, err := w.Client.RcloneOpen(ctx, h.IP, remoteManifestPath)
		if err != nil {
			return errors.Wrap(err, "download manifest")
		}
		defer func() {
			err = stdErr.Join(err, r.Close())
		}()

		var manifest backupspec.ManifestContentWithIndex
		if err := manifest.Read(r); err != nil {
			return errors.Wrap(err, "read manifest")
		}

		// Lock sstables and scylla manifests
		err = manifest.ForEachIndexIterWithError(nil, func(fm backupspec.FilesMeta) error {
			return errors.Wrapf(w.lockTableFiles(ctx, h, fm, cfg), "lock table %s.%s files", fm.Keyspace, fm.Table)
		})
		if err != nil {
			return errors.Wrap(err, "lock index files")
		}

		// Lock SM manifest
		w.Logger.Info(ctx, "Locking manifest file", "host", h.IP)
		if err := w.lockAndAwait(ctx, h.IP, remoteManifestDir, []string{path.Base(remoteManifestPath)}, cfg, "", ""); err != nil {
			return errors.Wrap(err, "lock manifest")
		}
	case RetentionLockEventBasedHold:
		// Initialize holdHandler
		apply := func(ctx context.Context, paths []string, hold bool) error {
			return w.holdAndWait(ctx, h.IP, remoteManifestDir, paths, hold, "", "")
		}
		holdHandler := newEventBasedHoldHandler(apply, eventBasedHoldBatchSize)
		// Feed local files - even though manifests have already been uploaded,
		// we still treat them as local for the purposes of applying/removing the hold.
		// For consistency, we also treat shadowed temporary manifest as local,
		// so that its hold cycle follows the regular manifest cycle.
		holdHandler.addLocal(path.Base(remoteManifestPath))
		holdHandler.addLocal(backupspec.TempFile(path.Base(remoteManifestPath)))
		holdHandler.finalizeLocal()
		// Feed remote files - limit to manifests coming from the same task ID,
		// so that we reduce interference between multiple backup tasks.
		opts := &scyllaclient.RcloneListDirOpts{
			FilesOnly:          true,
			ShowEventBasedHold: true,
		}
		listErr := w.Client.RcloneListDirIter(ctx, h.IP, remoteManifestDir, opts, func(item *scyllaclient.RcloneListDirItem) {
			var mi backupspec.ManifestInfo
			err := mi.ParsePath(path.Join(path.Dir(manifestPath), item.Name))
			if err == nil && mi.TaskID == w.TaskID {
				holdHandler.addRemote(ctx, item.Name, item.EventBasedHold)
			}
		})
		err := stdErr.Join(
			errors.Wrap(listErr, "list manifests holds"),
			errors.Wrap(holdHandler.finalize(ctx), "finalize remote manifests holds"),
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// lockTableFiles locks sstables and scylla manifests for given table.
func (w *worker) lockTableFiles(ctx context.Context, h *hostInfo, fm backupspec.FilesMeta, cfg retentionLockConfig) error {
	sstDir := backupspec.RemoteSSTableVersionDir(w.ClusterID, h.DC, h.ID, fm.Keyspace, fm.Table, fm.Version)
	remoteSSTDir := h.Location.RemotePath(sstDir)

	// Locking sstables and scylla manifests separately to avoid
	// potential reallocation of huge sstable files slice.
	if len(fm.Files) > 0 {
		w.Logger.Info(ctx, "Locking sstables",
			"host", h.IP,
			"keyspace", fm.Keyspace,
			"table", fm.Table,
			"count", len(fm.Files),
		)
		if err := w.batchLock(ctx, h.IP, remoteSSTDir, fm.Files, cfg, fm.Keyspace, fm.Table); err != nil {
			return errors.Wrap(err, "batch lock sstables")
		}
	}

	if len(fm.ScyllaManifests) > 0 {
		w.Logger.Info(ctx, "Locking scylla manifests",
			"host", h.IP,
			"keyspace", fm.Keyspace,
			"table", fm.Table,
			"count", len(fm.ScyllaManifests),
		)
		if err := w.batchLock(ctx, h.IP, remoteSSTDir, fm.ScyllaManifests, cfg, fm.Keyspace, fm.Table); err != nil {
			return errors.Wrap(err, "batch lock scylla manifests")
		}
	}

	return nil
}

func (w *worker) batchLock(ctx context.Context, host, remoteDir string,
	paths []string, cfg retentionLockConfig, keyspace, table string,
) error {
	for len(paths) > 0 {
		size := min(len(paths), scyllaclient.MaxSStableComponentCountInReqBody)
		batch := paths[:size]
		paths = paths[size:]

		if err := w.lockAndAwait(ctx, host, remoteDir, batch, cfg, keyspace, table); err != nil {
			return errors.Wrap(err, "lock and await")
		}
	}
	return nil
}

func (w *worker) lockAndAwait(ctx context.Context, host, remoteDir string,
	paths []string, cfg retentionLockConfig, keyspace, table string,
) error {
	mode, err := cfg.mode.toRetentionMode()
	if err != nil {
		return err
	}
	jobID, err := w.Client.RcloneBatchRetentionLock(ctx, host, remoteDir, paths, mode, cfg.until, cfg.override)
	if err != nil {
		return errors.Wrap(err, "schedule retention lock job")
	}
	return w.waitRetentionJob(ctx, host, jobID, keyspace, table)
}

func (w *worker) holdAndWait(ctx context.Context, host, remoteDir string, paths []string, hold bool, keyspace, table string) error {
	jobID, err := w.Client.RcloneBatchEventBasedHold(ctx, host, remoteDir, paths, hold)
	if err != nil {
		return errors.Wrap(err, "schedule event based hold job")
	}
	return w.waitRetentionJob(ctx, host, jobID, keyspace, table)
}

// waitRetentionJob polls the async retention lock job until completion,
// updating the retention_locked_files metric on each poll.
func (w *worker) waitRetentionJob(ctx context.Context, host string, jobID int64, keyspace, table string) (err error) {
	defer func() {
		if err != nil {
			w.Logger.Info(ctx, "Stop job", "host", host, "id", jobID)
			if stopErr := w.Client.RcloneJobStop(context.Background(), host, jobID); stopErr != nil {
				w.Logger.Error(ctx, "Failed to stop retention lock job",
					"host", host,
					"id", jobID,
					"error", stopErr,
				)
			}
		}

		if cleanupErr := w.clearJobStats(context.Background(), jobID, host); cleanupErr != nil {
			w.Logger.Error(ctx, "Failed to clear retention lock job stats",
				"host", host,
				"id", jobID,
				"error", cleanupErr,
			)
		}
	}()

	var done int64
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		job, err := w.Client.RcloneJobProgress(ctx, host, jobID, w.Config.LongPollingTimeoutSeconds)
		if err != nil {
			return errors.Wrap(err, "fetch retention lock job progress")
		}

		switch scyllaclient.RcloneJobStatus(job.Status) {
		case scyllaclient.JobError:
			return errors.Errorf("retention lock job error (%d): %s", jobID, job.Error)
		case scyllaclient.JobNotFound:
			return errors.Errorf("retention lock job not found (%d)", jobID)
		case scyllaclient.JobSuccess, scyllaclient.JobRunning:
			diff := job.Uploaded - done
			done = job.Uploaded
			w.Metrics.IncreaseRetentionLockedFiles(w.ClusterID, keyspace, table, host, diff)
			if scyllaclient.RcloneJobStatus(job.Status) == scyllaclient.JobSuccess {
				return nil
			}
		}
	}
}

// eventBasedHoldBatchSize defines how many files are batched together
// before objects are sent to SM agent.
const eventBasedHoldBatchSize = 1000

// eventBasedHoldApplyFunc sets (hold=true) or removes (hold=false) event based hold on the given paths.
type eventBasedHoldApplyFunc func(ctx context.Context, paths []string, hold bool) error

type eventBasedHoldRequest struct {
	ctx   context.Context // nolint: containedctx
	paths []string
	hold  bool
}

// eventBasedHoldHandler is a helper for applying event based holds.
//
// WORM backup created with retention locks can be inefficient in terms
// of per-object request count, as on every backup run, we need to prolong
// retention periods on all objects being a part of current snapshot,
// even if they were deduplicated.
// Backup made with RetentionLockEventBasedHold aims to improve
// this by relying on event based holds which don't need to be reset for
// deduplicated objects, but need to be separately released when we expect
// that the object won't be a part of next snapshots.
// When making backup with RetentionLockEventBasedHold,
// we rely on 2 bucket level configurations:
// - default retention period - ensures WORM backup
// - default event based hold - optional, saves on per-object requests
//
// In many cases (e.g. StageDeduplicate), we operate on two groups of objects:
// - snapshot objects located on nodes (local)
// - already uploaded objects in the backup location (remote)
// Other cases can usually be reduced to this one.
// Remote objects can already have holds applies from default bucket settings
// or from previous backup task executions.
//
// The table below shows what actions needs to be taken in which scenarios:
// +----------+------------------+---------------------+-----------+
// |          | remote with hold | remote without hold | no remote |
// +----------+------------------+---------------------+-----------+
// | local    | nothing          | set hold            | nothing*  |
// | no local | remove hold      | nothing             | nothing   |
// +----------+------------------+---------------------+-----------+
// Hold is explicitly set only when local object has remote counterpart without hold.
// We won't reupload the object, so we need to ensure that it has correct hold.
// This is a rare case, but a possible one (SSTable migration back and forth,
// file based restore, manual hold removal).
// Hold is removed only when remote object with hold is no longer a part of local.
// This is the common case when SSTable is compacted away.
// The only remaining interesting case is when we have local object with no remote.
// We don't need to apply the hold there, as we count on the default hold bucket configuration.
// Even if it isn't set, the object would be still protected by the default retention period,
// and if it will be a part of the next snapshot, it would fall to the case of local object
// with remote without hold, which would apply the hold then.
//
// To minimize the per-object request count, we want to send requests only when needed.
// To do that, we first cache all local files and apply the holds on they fly when reading
// remote files (we assume that there is strictly less local than remote files).
// We do it on the fly, as caching all remote objects results in unnecessary memory pressure.
// Moreover, learning about remote files requires listing, which can be an iterative and time-consuming
// operation, so it makes sense to start setting/removing needed holds while it's still ongoing.
// SM agent endpoint responsible for handling holds has built in parallelism, so it's better to send
// hold jobs to it in batches (aiming for minBatchSize objects per batch).
// One important consideration is that we don't want to stall the listing because of the longer time
// needed to apply the holds on multiple objects. Such behavior could lead to timeouts on SM agent
// side related to a too slow receiver. Implementation takes that into consideration and
// aggregates objects over minBatchSize when previous batch is still being processed.
//
// Usage contract:
//   - feed all local files via addLocal
//   - call finalizeLocal
//   - feed remote files via addRemote (setting/removing holds might happen in the background)
//   - call finalize (flush remaining batches and wait for all jobs)
type eventBasedHoldHandler struct {
	apply        eventBasedHoldApplyFunc
	minBatchSize int

	local     map[string]struct{}
	localDone bool

	setBatch    []string
	removeBatch []string

	reqCh chan eventBasedHoldRequest
	done  chan error
}

func newEventBasedHoldHandler(apply eventBasedHoldApplyFunc, minBatchSize int) *eventBasedHoldHandler {
	h := eventBasedHoldHandler{
		apply:        apply,
		minBatchSize: minBatchSize,
		local:        make(map[string]struct{}),
		setBatch:     make([]string, 0, minBatchSize),
		removeBatch:  make([]string, 0, minBatchSize),
		// Even though we have single worker, buffered channel
		// allows for cheaper check if worker is busy.
		reqCh: make(chan eventBasedHoldRequest, 1),
		done:  make(chan error, 1),
	}
	go h.worker()
	return &h
}

// addLocal records a local object name. It must be called before finalizeLocal.
func (r *eventBasedHoldHandler) addLocal(name string) {
	if r.localDone {
		close(r.reqCh)
		panic("cannot add local file after finalizeLocal")
	}
	r.local[name] = struct{}{}
}

// finalizeLocal marks that all local objects have already been added.
func (r *eventBasedHoldHandler) finalizeLocal() {
	r.localDone = true
}

// addRemote reconciles a single remote file against the local set.
// In case remote needs its hold changed, it's recorded in the batch buffer.
// When the buffer reaches minBatchSize, objects are sent to SM agent,
// so that their holds can be adjusted.
// It must be called after finalizeLocal.
func (r *eventBasedHoldHandler) addRemote(ctx context.Context, name string, hold bool) {
	if !r.localDone {
		close(r.reqCh)
		panic("cannot add remote file before finishLocal")
	}
	_, local := r.local[name]
	switch {
	case local && !hold:
		r.setBatch = append(r.setBatch, name)
		if len(r.setBatch) >= r.minBatchSize {
			r.tryFlush(ctx, true)
		}
	case !local && hold:
		r.removeBatch = append(r.removeBatch, name)
		if len(r.removeBatch) >= r.minBatchSize {
			r.tryFlush(ctx, false)
		}
	}
}

func (r *eventBasedHoldHandler) tryFlush(ctx context.Context, hold bool) bool { // nolint: unparam
	if cap(r.reqCh) != 0 && len(r.reqCh) == cap(r.reqCh) {
		// Quick check cheaper than select
		return false
	}
	batch := r.batch(hold)
	if len(batch) == 0 {
		return false
	}
	select {
	case r.reqCh <- eventBasedHoldRequest{ctx: ctx, paths: batch, hold: hold}:
		r.resetBatch(hold)
		return true
	default:
		return false
	}
}

func (r *eventBasedHoldHandler) flush(ctx context.Context, hold bool) error {
	batch := r.batch(hold)
	if len(batch) == 0 {
		return nil
	}
	select {
	case r.reqCh <- eventBasedHoldRequest{ctx: ctx, paths: batch, hold: hold}:
		r.resetBatch(hold)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *eventBasedHoldHandler) batch(hold bool) []string {
	if hold {
		return r.setBatch
	}
	return r.removeBatch
}

func (r *eventBasedHoldHandler) resetBatch(hold bool) {
	if hold {
		r.setBatch = make([]string, 0, r.minBatchSize)
		return
	}
	r.removeBatch = make([]string, 0, r.minBatchSize)
}

// worker is method executed in a dedicated goroutine responsible
// for applying the batches. It returns the first encountered
// error, but keeps on applying the batches until reqCh is closed.
func (r *eventBasedHoldHandler) worker() {
	var err error
	setErr := func(e error) {
		if err == nil {
			err = e
		}
	}

	for req := range r.reqCh {
		paths := req.paths
		for len(paths) > 0 {
			if err := req.ctx.Err(); err != nil {
				setErr(err)
				break
			}

			size := min(len(paths), r.minBatchSize)
			batch := paths[:size]
			paths = paths[size:]

			setErr(r.apply(req.ctx, batch, req.hold))
		}
	}
	r.done <- err
}

// finalize flushes the remaining batches and waits for SM agent requests to finish.
func (r *eventBasedHoldHandler) finalize(ctx context.Context) error {
	flushSetErr := errors.Wrap(r.flush(ctx, true), "flush set hold batch")
	flushRemoveErr := errors.Wrap(r.flush(ctx, false), "flush remove hold batch")
	close(r.reqCh)
	return stdErr.Join(<-r.done, flushSetErr, flushRemoveErr)
}

func retentionLockUntil(snapshotTag string, retentionDays int) (time.Time, error) {
	snapshotTime, err := backupspec.SnapshotTagTime(snapshotTag)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "parse snapshot tag time")
	}
	return snapshotTime.Add(24 * time.Hour * time.Duration(retentionDays)), nil
}
