// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"
	stdErr "errors"

	"github.com/pkg/errors"
)

func (w *worker) holdAndWait(ctx context.Context, host, remoteDir string, paths []string, hold bool, keyspace, table string) error {
	jobID, err := w.Client.RcloneBatchEventBasedHold(ctx, host, remoteDir, paths, hold)
	if err != nil {
		return errors.Wrap(err, "schedule event based hold job")
	}
	return w.waitRetentionJob(ctx, host, jobID, keyspace, table)
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
