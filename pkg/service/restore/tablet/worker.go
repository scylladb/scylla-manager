// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"context"
	"maps"
	"net/netip"
	"slices"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	schematable "github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
	"golang.org/x/sync/errgroup"
)

// RestoreWorker is a set of tools responsible for
// restoring tables with tablet aware restore.
type RestoreWorker struct {
	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	logger             log.Logger
	client             *scyllaclient.Client
	smSession          gocqlx.Session
	nodeConfig         map[netip.Addr]configcache.NodeConfig
	hostPicker         *hostPicker
	longPollingSeconds int
}

// NewRestoreWorker is a constructor for RestoreWorker.
func NewRestoreWorker(clusterID, taskID, runID uuid.UUID,
	logger log.Logger, client *scyllaclient.Client, smSession gocqlx.Session,
	nodeConfig map[netip.Addr]configcache.NodeConfig, longPollingSeconds int,
) *RestoreWorker {
	return &RestoreWorker{
		clusterID:          clusterID,
		taskID:             taskID,
		runID:              runID,
		logger:             logger.Named("tablet_restore"),
		client:             client,
		smSession:          smSession,
		nodeConfig:         nodeConfig,
		hostPicker:         newHostPicker(nodeConfig),
		longPollingSeconds: longPollingSeconds,
	}
}

// Restore given tables with tablet aware restore based on provided workload.
func (w *RestoreWorker) Restore(ctx context.Context, tables Workload) error {
	w.logger.Info(ctx, "Started tablet aware restore")
	defer w.logger.Info(ctx, "Finished tablet aware restore")

	// We don't need to limit concurrency because of scylla,
	// because it handles parallel tablet aware restores according
	// to its tablet scheduler. We want to set sane limits to avoid
	// exhausting too many resources on SM and SM agent sides.
	clusterShards := 0
	for _, nc := range w.nodeConfig {
		clusterShards += int(nc.CPUCount)
	}
	limit := max(100, clusterShards)
	limit = min(1000, limit)
	w.logger.Info(ctx, "Calculated concurrency limit", "limit", limit)

	// Returns empty map unless CloneProgress was called.
	prevProgress, err := getProgressMap(w.smSession, w.clusterID, w.taskID, w.runID)
	if err != nil {
		return errors.Wrap(err, "load previous run tablet aware restore progress")
	}

	reset := w.setTaskTTL(ctx)
	defer reset()

	eg := errgroup.Group{}
	eg.SetLimit(limit)
	for _, tm := range tables {
		eg.Go(func() error {
			return errors.Wrapf(w.restoreTableWithResume(ctx, tm, prevProgress[tm.Table]),
				"tablet aware restore table %s.%s", tm.Table.Keyspace, tm.Table.Name)
		})
	}
	return eg.Wait()
}

// setTaskTTL sets scylla user task TTL on all nodes, so that task status
// is preserved on scylla side in between long polling calls.
// It returns reset function resetting TTL to its previous value.
func (w *RestoreWorker) setTaskTTL(ctx context.Context) (reset func()) {
	var (
		mu     sync.Mutex
		resets = make([]func(), 0, len(w.nodeConfig))
	)
	wg := sync.WaitGroup{}
	for ip := range w.nodeConfig {
		wg.Go(func() {
			r, err := w.client.ScyllaControlTaskUserTTL(ctx, ip.String())
			if err != nil {
				w.logger.Error(ctx, "Failed to set Scylla user task TTL", "host", ip.String(), "error", err)
				return
			}
			mu.Lock()
			resets = append(resets, r)
			mu.Unlock()
		})
	}
	wg.Wait()

	return func() {
		wg := sync.WaitGroup{}
		for _, r := range resets {
			wg.Go(r)
		}
		wg.Wait()
	}
}

// restoreTableWithResume skips or restores the table based on the provided RunProgress.
func (w *RestoreWorker) restoreTableWithResume(ctx context.Context, tm TableMeta, prev RunProgress) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if prev.isSuccess() {
		w.logger.Info(ctx, "Table already restored by previous run, skipping",
			"keyspace", tm.Table.Keyspace,
			"table", tm.Table.Name,
		)
		return nil
	}
	// Note that the scylla task scheduled by the previous run might still be running
	// (e.g. after SM crash). SM doesn't need to re-attach to it, because scheduling
	// tablet aware restore of the same table and snapshot again makes scylla join
	// the ongoing restore request instead of starting a new one. Scylla also skips
	// sstables that were already downloaded by the previous attempts so no progress is lost.
	return w.restoreTable(ctx, tm)
}

// restoreTable by scheduling tablet aware restore task and waiting for its completion.
func (w *RestoreWorker) restoreTable(ctx context.Context, tm TableMeta) error {
	locations, err := w.buildTabletRestoreLocations(tm)
	if err != nil {
		return errors.Wrap(err, "build tablet restore locations")
	}

	hostIP, err := w.hostPicker.pick(slices.Collect(maps.Keys(tm.Datacenters))...)
	if err != nil {
		return errors.Wrap(err, "pick host for coordinating tablet aware restore")
	}
	defer w.hostPicker.release(hostIP)
	host := hostIP.String()

	w.logger.Info(ctx, "Started table tablet aware restore",
		"keyspace", tm.Table.Keyspace,
		"table", tm.Table.Name,
		"host", host,
		"locations", locations,
	)
	defer w.logger.Info(ctx, "Finished table tablet aware restore", "keyspace", tm.Table.Keyspace, "table", tm.Table.Name)

	pr := &RunProgress{
		ClusterID: w.clusterID,
		TaskID:    w.taskID,
		RunID:     w.runID,
		Keyspace:  tm.Table.Keyspace,
		Table:     tm.Table.Name,
		StartedAt: timeutc.Now(),
		Size:      tm.Size,
	}
	w.upsertProgress(ctx, pr)

	id, err := w.client.TabletRestore(ctx, host, tm.Table.Keyspace, tm.Table.Name, tm.SnapshotTag, locations)
	if err != nil {
		err = errors.Wrapf(err, "schedule tablet aware restore on node %s", host)
		w.recordFailure(ctx, pr, err)
		return err
	}
	pr.Host = host
	pr.ScyllaTaskID = id
	w.upsertProgress(ctx, pr)

	if err := w.waitTask(ctx, pr); err != nil {
		return errors.Wrapf(err, "wait for tablet aware restore task %s on node %s", id, host)
	}
	return nil
}

func (w *RestoreWorker) buildTabletRestoreLocations(tm TableMeta) ([]scyllaclient.TabletRestoreLocation, error) {
	locations := make([]scyllaclient.TabletRestoreLocation, 0, len(tm.Datacenters))
	for _, dcMeta := range tm.Datacenters {
		nodeCfg, err := w.nodeConfigForDC(dcMeta.DC)
		if err != nil {
			return nil, errors.Wrapf(err, "get node config for dc %s", dcMeta.DC)
		}
		endpoint, err := nodeCfg.ScyllaObjectStorageEndpoint(dcMeta.Provider)
		if err != nil {
			return nil, errors.Wrapf(err, "get object storage endpoint for dc %s", dcMeta.DC)
		}
		locations = append(locations, scyllaclient.TabletRestoreLocation{
			Endpoint: endpoint,
			Bucket:   dcMeta.Bucket,
			// Note that DcMeta contains source DC name.
			// When scheduling tablet aware restore, we should use target DC names.
			// They might be different when --dc-mapping is used.
			// For now, tablet aware restore does not support --dc-mapping changing
			// DC names, so no change needs to be applied, but when it happens,
			// we need to remember to apply it here as well.
			Datacenter: dcMeta.DC,
			Manifests:  dcMeta.RemoteManifests,
		})
	}
	return locations, nil
}

// waitTask executes waitTaskTick until error or terminal scylla task status is encountered.
// Error from scylla task status is included in the returned error.
// Scylla task is aborted on SM task pause.
func (w *RestoreWorker) waitTask(ctx context.Context, pr *RunProgress) error {
	for {
		task, err := w.waitTaskTick(ctx, pr, w.longPollingSeconds)
		if err != nil {
			w.abortTaskOnPause(ctx, pr)
			return err
		}

		switch scyllaclient.ScyllaTaskState(task.State) {
		case scyllaclient.ScyllaTaskStateFailed:
			return errors.New("scylla task failed: " + task.Error)
		case scyllaclient.ScyllaTaskStateDone:
			return nil
		}
	}
}

const taskAbortGracePeriodSeconds = 60

// abortTaskOnPause aborts scylla task and waits for it to reach terminal state
// for up to taskAbortGracePeriodSeconds. Waiting is caused by scylla abort API
// being async, and trying schedule tablet aware restore on a table that is
// currently being aborted results in an error.
func (w *RestoreWorker) abortTaskOnPause(parentCtx context.Context, pr *RunProgress) {
	// Don't want to abort on non-pause related errors
	if parentCtx.Err() == nil {
		return
	}
	// Since parent ctx is canceled, we need to run abort procedure in a new, bounded one
	ctx, cancel := context.WithTimeout(context.Background(), taskAbortGracePeriodSeconds*time.Second)
	defer cancel()

	if err := w.client.ScyllaAbortTask(ctx, pr.Host, pr.ScyllaTaskID); err != nil {
		w.logger.Error(ctx, "Failed to abort task",
			"host", pr.Host,
			"id", pr.ScyllaTaskID,
			"error", err,
		)
		return
	}

	status, err := w.waitTaskTick(ctx, pr, taskAbortGracePeriodSeconds)
	if err != nil {
		w.logger.Error(ctx, "Failed to wait for aborted task to finish",
			"host", pr.Host,
			"id", pr.ScyllaTaskID,
			"error", err,
		)
		return
	}
	if s := scyllaclient.ScyllaTaskState(status.State); s != scyllaclient.ScyllaTaskStateDone && s != scyllaclient.ScyllaTaskStateFailed {
		w.logger.Error(ctx, "Aborted task has not finished within grace period",
			"host", pr.Host,
			"id", pr.ScyllaTaskID,
			"grace_period", taskAbortGracePeriodSeconds*time.Second,
		)
	}
}

// waitTaskTick performs a single long polling iteration on the scylla task
// and updates run progress with the returned task status.
// Error from scylla task status is not included in the returned error.
func (w *RestoreWorker) waitTaskTick(ctx context.Context, pr *RunProgress, longPollingSeconds int) (*models.TaskStatus, error) {
	task, err := w.client.ScyllaWaitTask(ctx, pr.Host, pr.ScyllaTaskID, int64(longPollingSeconds))
	if err != nil {
		w.recordFailure(ctx, pr, err)
		return nil, errors.Wrap(err, "long poll task status")
	}
	w.updateProgress(ctx, pr, task)
	return task, nil
}

// updateProgress updates and saves run progress described by scylla task status.
func (w *RestoreWorker) updateProgress(ctx context.Context, pr *RunProgress, task *models.TaskStatus) {
	pr.RestoredSSTables = int64(task.ProgressCompleted)
	pr.TotalSSTables = int64(task.ProgressTotal)
	if t := time.Time(task.StartTime); !t.IsZero() {
		pr.StartedAt = t
	}

	switch scyllaclient.ScyllaTaskState(task.State) {
	case scyllaclient.ScyllaTaskStateDone:
		pr.CompletedAt = taskEndTime(task)
		pr.Error = ""
	case scyllaclient.ScyllaTaskStateFailed:
		pr.CompletedAt = taskEndTime(task)
		pr.Error = joinError(pr.Error, "scylla task error: "+task.Error)
	}
	w.upsertProgress(ctx, pr)
}

// recordFailure saves SM side error (e.g. failed scylla task schedule or status poll)
// in run progress.
func (w *RestoreWorker) recordFailure(ctx context.Context, pr *RunProgress, err error) {
	pr.Error = joinError(pr.Error, err.Error())
	w.upsertProgress(ctx, pr)
}

// taskEndTime returns task end time falling back to SM side clock when unset.
func taskEndTime(task *models.TaskStatus) time.Time {
	if t := time.Time(task.EndTime); !t.IsZero() {
		return t
	}
	return timeutc.Now()
}

func joinError(recorded, current string) string {
	switch {
	case recorded == "":
		return current
	case current == "":
		return recorded
	default:
		return recorded + "; " + current
	}
}

func (w *RestoreWorker) upsertProgress(ctx context.Context, pr *RunProgress) {
	q := schematable.RestoreRunProgressTablet.InsertQuery(w.smSession)
	defer q.Release()
	if err := q.BindStruct(pr).Exec(); err != nil {
		w.logger.Error(ctx, "Failed to upsert tablet aware restore run progress",
			"keyspace", pr.Keyspace,
			"table", pr.Table,
			"error", err,
		)
	}
}

// nodeConfigForDC returns config of an arbitrary node from the given datacenter.
func (w *RestoreWorker) nodeConfigForDC(dc string) (configcache.NodeConfig, error) {
	for _, nc := range w.nodeConfig {
		if nc.Datacenter == dc {
			return nc, nil
		}
	}
	return configcache.NodeConfig{}, errors.Errorf("no node found in datacenter %s", dc)
}

// hostPicker picks the least utilized host from the eligible datacenters.
// It's safe for concurrent use.
type hostPicker struct {
	mu       sync.Mutex
	hostDC   map[netip.Addr]string
	inflight map[netip.Addr]int
}

func newHostPicker(nodeConfig map[netip.Addr]configcache.NodeConfig) *hostPicker {
	hostDC := make(map[netip.Addr]string, len(nodeConfig))
	for ip, nc := range nodeConfig {
		hostDC[ip] = nc.Datacenter
	}
	return &hostPicker{
		hostDC:   hostDC,
		inflight: make(map[netip.Addr]int, len(hostDC)),
	}
}

// pick returns the host from the given datacenters with the lowest
// number of in-flight requests and increases its count.
// A finished request should be reported with release.
func (p *hostPicker) pick(dcs ...string) (netip.Addr, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var (
		best  netip.Addr
		found bool
	)
	for ip, dc := range p.hostDC {
		if !slices.Contains(dcs, dc) {
			continue
		}
		if !found || p.inflight[ip] < p.inflight[best] {
			best = ip
			found = true
		}
	}
	if !found {
		return netip.Addr{}, errors.Errorf("no node found in datacenters %v", dcs)
	}
	p.inflight[best]++
	return best, nil
}

// release marks a request to the given host as finished.
func (p *hostPicker) release(host netip.Addr) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inflight[host]--
}
