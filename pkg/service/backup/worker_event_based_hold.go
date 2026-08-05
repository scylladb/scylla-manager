// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"
	stdErr "errors"
	"net/http"
	"path"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"golang.org/x/sync/errgroup"
)

// eventBasedHoldSnapshot handles StageRetentionLock for the RetentionLockEventBasedHold mode.
// This approach works retroactively - holds placed on the current snapshot files are removed
// only during the next backup task execution (unless they will be deduplicated).
// This is true even for the never deduplicated files like schema and manifests.
// eventBasedHoldSnapshot doesn't touch sstables nor scylla manifests,
// as they already have been handled in StageDeduplicate.
// Schema holds are removed first, then it proceeds with SM manifests, so that the manifests
// have the longest retention period and can be used to reason about snapshot retention protection.
func (w *worker) eventBasedHoldSnapshot(ctx context.Context, hosts []hostInfo, target Target) error {
	if !target.SkipSchema {
		if err := w.holdSchemaFiles(ctx, hosts); err != nil {
			return errors.Wrap(err, "handle schema files holds")
		}
	}
	// We branch parallelism on location level first (instead of jumping to the host level right away),
	// because we need to list/download/parse all currently held manifests - even from not currently
	// operating hosts.
	eg, ctx := errgroup.WithContext(ctx)
	locHosts := groupHostsByLocation(hosts)
	// MaxManifestInMemory is cluster wide limit, so we need divide it across
	// all locations. This is not a hard limit, so we can round it up.
	perLocManifestLimit := (MaxManifestInMemory + len(locHosts) - 1) / len(locHosts)
	perLocManifestLimit = max(perLocManifestLimit, 1)
	for loc, hs := range locHosts {
		eg.Go(func() error {
			return errors.Wrapf(w.holdLocationManifests(ctx, perLocManifestLimit, loc, hs), "location %s", loc)
		})
	}
	return eg.Wait()
}

// holdSchemaFiles handles event based holds on CQL and alternator schema files for all locations.
func (w *worker) holdSchemaFiles(ctx context.Context, hosts []hostInfo) error {
	doneLocations := make(map[string]struct{})
	for i := range hosts {
		if _, ok := doneLocations[hosts[i].Location.StringWithoutDC()]; ok {
			continue
		}
		doneLocations[hosts[i].Location.StringWithoutDC()] = struct{}{}
		w.Logger.Info(ctx, "Handle schema files holds", "location", hosts[i].Location.StringWithoutDC(), "host", hosts[i].IP)

		cqlPath := backupspec.RemoteSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag)
		paths := []string{path.Base(cqlPath)}
		if _, ok := getAlternatorHost(hosts); ok {
			paths = append(paths, path.Base(backupspec.AlternatorSchemaPath(w.ClusterID, w.TaskID, w.SnapshotTag)))
		}
		remoteSchemaDir := hosts[i].Location.RemotePath(path.Dir(cqlPath))

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
	return nil
}

// holdLocationManifests handles holds of SM manifests and vanished sstable dirs (see vanishedSSTableDirs).
// We first remove holds from all vanished sstable dirs and only after that we
// proceed with previous snapshot manifests hold removal. This order is important
// because we identify unprocessed previous snapshot manifests by the fact that
// they still have hold applied.
// Hosts are expected to share the same single loc.
// Manifest limit refers to the amount of manifests which can be stored
// in memory at a single time.
func (w *worker) holdLocationManifests(ctx context.Context, manifestLimit int, loc backupspec.Location, hosts []hostInfo) error {
	manifests, err := listRemoteManifests(ctx, w.Client, hosts[0].IP, loc, w.ClusterID)
	if err != nil {
		return errors.Wrap(err, "list manifests")
	}
	current, oldHeld := groupManifestsByTagAndHold(manifests, w.TaskID, w.SnapshotTag)
	w.Logger.Info(ctx, "Found manifests to process",
		"location", loc,
		"current manifests", len(current),
		"old held manifests", len(oldHeld),
	)

	// Even though not having default event based holds set in bucket configuration
	// is fine for sstables and schema files (it results in one additional per object request),
	// we rely on correct manifest hold handling to reason about protected snapshots
	// and vanished sstable dirs. Because of that, we should apply missing manifests holds.
	var currentNotHeld []string
	for _, m := range current {
		if !m.EventBasedHold {
			currentNotHeld = append(currentNotHeld, m.Path())
		}
	}
	if len(currentNotHeld) > 0 {
		if err := w.holdAndWait(ctx, hosts[0].IP, loc.RemotePath(""), currentNotHeld, true, "", ""); err != nil {
			return errors.Wrap(err, "set missing hold on current manifests")
		}
	}

	// Look for vanished sstable dirs and clean holds from them
	if len(oldHeld) > 0 {
		vanished, err := w.vanishedSSTableDirs(ctx, manifestLimit, hosts, current, oldHeld)
		if err != nil {
			return errors.Wrap(err, "find vanished dirs")
		}
		if err := w.releaseAllHoldsAllDirs(ctx, hosts, vanished); err != nil {
			return errors.Wrap(err, "release vanished dirs holds")
		}
	}

	// Finally, clean holds from old manifests
	if err := w.releaseManifestsHold(ctx, hosts[0].IP, oldHeld); err != nil {
		return errors.Wrap(err, "release old manifest holds")
	}
	return nil
}

// sstableDir describes sstable version dir.
type sstableDir struct {
	Keyspace string
	Table    string
	NodeID   string
	Path     string // Relative to location root.
}

// vanishedSSTableDirs returns sstable version dirs (identified by nodeID, keyspace, table, table version)
// which used to be a part of previous snapshot, but are not a part of the current one.
// This can happen when node is replaced or table is dropped.
// Such dirs are discovered by reading contents of previous snapshot manifests
// and comparing their sstable dirs with the ones from the current snapshot.
// Hosts, current and oldHeld manifests are expected to share the same single location.
// Hosts work in parallel up to specified limit.
func (w *worker) vanishedSSTableDirs(ctx context.Context, limit int, hosts []hostInfo, current, oldHeld []remoteManifestInfo) ([]sstableDir, error) {
	if len(current)+len(oldHeld) == 0 {
		return nil, nil
	}

	type parseManifestJob struct {
		mi      *backupspec.ManifestInfo
		current bool
	}
	jobCh := make(chan parseManifestJob, len(current)+len(oldHeld))
	for _, m := range current {
		jobCh <- parseManifestJob{mi: m.ManifestInfo, current: true}
	}
	for _, m := range oldHeld {
		jobCh <- parseManifestJob{mi: m.ManifestInfo}
	}
	close(jobCh)

	var (
		currentDirs = make(map[sstableDir]struct{})
		oldDirs     = make(map[sstableDir]struct{})
		mu          sync.Mutex
	)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(limit)
	for i := range hosts {
		eg.Go(func() error {
			for job := range jobCh {
				if egCtx.Err() != nil {
					return egCtx.Err()
				}
				dirs, err := w.manifestSSTableDirs(egCtx, hosts[i].IP, job.mi)
				if err != nil {
					return errors.Wrapf(err, "%s: get sstable dirs of manifest %s", hosts[i].IP, job.mi.Path())
				}
				mu.Lock()
				if job.current {
					for _, d := range dirs {
						currentDirs[d] = struct{}{}
					}
				} else {
					for _, d := range dirs {
						oldDirs[d] = struct{}{}
					}
				}
				mu.Unlock()
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	var vanished []sstableDir
	for d := range oldDirs {
		if _, ok := currentDirs[d]; !ok {
			vanished = append(vanished, d)
		}
	}
	return vanished, nil
}

// manifestSSTableDirs downloads manifest and returns its sstable dirs.
func (w *worker) manifestSSTableDirs(ctx context.Context, host string, m *backupspec.ManifestInfo) (dirs []sstableDir, err error) {
	r, err := w.Client.RcloneOpen(ctx, host, m.Location.RemotePath(m.Path()))
	if err != nil {
		return nil, errors.Wrap(err, "download manifest")
	}
	defer func() {
		err = stdErr.Join(err, r.Close())
	}()

	var mc backupspec.ManifestContentWithIndex
	if err := mc.Read(r); err != nil {
		return nil, errors.Wrap(err, "read manifest")
	}

	err = mc.ForEachIndexIter(nil, func(fm backupspec.FilesMeta) {
		dirs = append(dirs, sstableDir{
			Keyspace: fm.Keyspace,
			Table:    fm.Table,
			NodeID:   m.NodeID,
			Path:     m.SSTableVersionDir(fm.Keyspace, fm.Table, fm.Version),
		})
	})
	if err != nil {
		return nil, errors.Wrap(err, "iterate over manifest index")
	}
	return dirs, nil
}

// releaseAllHoldsAllDirs removes event based holds from all
// objects in all given dirs in parallel.
// Hosts and dirs are expected to share the same single location.
func (w *worker) releaseAllHoldsAllDirs(ctx context.Context, hosts []hostInfo, dirs []sstableDir) error {
	if len(dirs) == 0 {
		return nil
	}

	jobCh := make(chan sstableDir, len(dirs))
	for _, dir := range dirs {
		jobCh <- dir
	}
	close(jobCh)

	eg, egCtx := errgroup.WithContext(ctx)
	for i := range hosts {
		eg.Go(func() error {
			for dir := range jobCh {
				if egCtx.Err() != nil {
					return egCtx.Err()
				}
				if err := w.releaseAllDirHolds(egCtx, hosts[i], dir); err != nil {
					return errors.Wrapf(err, "dir %s", dir.Path)
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

// releaseAllDirHolds removes event based holds from all objects in given dir.
func (w *worker) releaseAllDirHolds(ctx context.Context, h hostInfo, dir sstableDir) error {
	w.Logger.Info(ctx, "Removing all holds from dir",
		"host", h.IP,
		"node", dir.NodeID,
		"keyspace", dir.Keyspace,
		"table", dir.Table,
		"dir", dir.Path,
	)

	remoteDir := h.Location.RemotePath(dir.Path)
	apply := func(ctx context.Context, paths []string, hold bool) error {
		return w.holdAndWait(ctx, h.IP, remoteDir, paths, hold, dir.Keyspace, dir.Table)
	}
	holdHandler := newEventBasedHoldHandler(apply, eventBasedHoldBatchSize)
	// No local files - all objects should have holds removed
	holdHandler.finalizeLocal()
	opts := &scyllaclient.RcloneListDirOpts{
		FilesOnly:          true,
		ShowEventBasedHold: true,
	}
	listErr := w.Client.RcloneListDirIter(ctx, h.IP, remoteDir, opts, func(item *scyllaclient.RcloneListDirItem) {
		holdHandler.addRemote(ctx, item.Path, item.EventBasedHold)
	})
	return stdErr.Join(
		errors.Wrap(listErr, "list dir"),
		errors.Wrap(holdHandler.finalize(ctx), "finalize holds removal"),
	)
}

// releaseManifestsHold releases holds for given manifests.
// Since regular manifests can shadow temporary ones which still have hold,
// it tries to release the hold from the shadowed temporary manifests first.
// Host and all manifests are expected to share the same single location.
func (w *worker) releaseManifestsHold(ctx context.Context, host string, manifests []remoteManifestInfo) error {
	if len(manifests) == 0 {
		return nil
	}
	paths := make([]string, 0, len(manifests))
	for _, m := range manifests {
		if !m.EventBasedHold {
			continue
		}
		paths = append(paths, m.Path())
		// Try to release hold from shadowed tmp manifest first.
		// Those calls are not batched with remaining ones, as
		// we want to tolerate errors in case tmp manifest doesn't exist.
		if !m.Temporary {
			tmpM := *m.ManifestInfo
			tmpM.Temporary = true
			remoteTmpPath := tmpM.Location.RemotePath(tmpM.Path())
			err := w.Client.RcloneEventBasedHold(ctx, host, remoteTmpPath, false)
			if err != nil && scyllaclient.StatusCodeOf(err) != http.StatusNotFound {
				return err
			}
		}
	}
	return w.holdAndWait(ctx, host, manifests[0].Location.RemotePath(""), paths, false, "", "")
}

func (w *worker) holdAndWait(ctx context.Context, host, remoteDir string, paths []string, hold bool, keyspace, table string) error {
	jobID, err := w.Client.RcloneBatchEventBasedHold(ctx, host, remoteDir, paths, hold)
	if err != nil {
		return errors.Wrap(err, "schedule event based hold job")
	}
	return w.waitRetentionJob(ctx, host, jobID, w.retentionLockJobCB(host, jobID, keyspace, table))
}

// groupManifestsByTagAndHold returns manifests being a part of the current snapshot
// and manifests from other snapshots still having hold applied.
// Shadowed manifests and manifests coming with different task IDs are ignored.
func groupManifestsByTagAndHold(manifests []remoteManifestInfo, taskID uuid.UUID, snapshotTag string) (current, oldHeld []remoteManifestInfo) {
	all := map[*backupspec.ManifestInfo]remoteManifestInfo{}
	for _, m := range manifests {
		all[m.ManifestInfo] = m
	}
	for _, m := range filterShadowedTemporaryManifests(manifestInfos(manifests)) {
		if m.TaskID != taskID {
			continue
		}
		switch {
		case m.SnapshotTag == snapshotTag:
			current = append(current, all[m])
		case all[m].EventBasedHold:
			oldHeld = append(oldHeld, all[m])
		}
	}
	return current, oldHeld
}

// groupHostsByLocation groups hosts by their location.
// The DC part of the location is ignored, so that the same
// physical location is processed only once.
func groupHostsByLocation(hosts []hostInfo) map[backupspec.Location][]hostInfo {
	m := make(map[backupspec.Location][]hostInfo)
	for i := range hosts {
		loc := hosts[i].Location
		loc.DC = ""
		m[loc] = append(m[loc], hosts[i])
	}
	return m
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
// Remote objects can already have holds applied from default bucket settings
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
// To do that, we first cache all local files and apply the holds on the fly when reading
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

// addRemote handles a single remote file against the local set.
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
		for batch := range slices.Chunk(req.paths, r.minBatchSize) {
			if err := req.ctx.Err(); err != nil {
				setErr(err)
				break
			}

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
