package backup

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// TODO docstrings

type RestoreTarget struct {
	Location         []Location `json:"location"`
	SnapshotTag      string     `json:"snapshot_tag"`
	Keyspace         []string   `json:"keyspace"`
	BatchSize        int        `json:"batch_size"`
	MinFreeDiskSpace int        `json:"min_free_disk_space"`
	Continue         bool       `json:"continue"`
	Parallel         int        `json:"parallel"`
}

type RestoreRunner struct {
	service *Service
}

func (r RestoreRunner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	t, err := r.service.GetRestoreTarget(ctx, clusterID, properties)
	if err != nil {
		return errors.Wrap(err, "get restore target")
	}
	return r.service.Restore(ctx, clusterID, taskID, runID, t)
}

func (s *Service) RestoreRunner() RestoreRunner {
	return RestoreRunner{service: s}
}

func (s *Service) GetRestoreTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (RestoreTarget, error) {
	s.logger.Info(ctx, "GetRestoreTarget", "cluster_id", clusterID)

	var t RestoreTarget

	if err := json.Unmarshal(properties, &t); err != nil {
		return t, err
	}

	if t.Location == nil {
		return t, errors.New("missing location")
	}

	// Set default values
	if t.BatchSize == 0 {
		t.BatchSize = 2
	}
	if t.MinFreeDiskSpace == 0 {
		t.MinFreeDiskSpace = 10
	}
	if t.Parallel == 0 {
		t.Parallel = 1
	}

	return t, nil
}

// tableRunProgress maps Rclone job ID to RestoreProgressRun
// for a specific table.
type tableRunProgress map[int64]*RestoreRunProgress

// jobUnit represents host that can be used for restoring files.
// If set, JobID is the ID of the unfinished rclone job started on the host.
type jobUnit struct {
	Host  string
	JobID int64
}

// bundle represents list of SSTables with the same ID.
type bundle []string

func (s *Service) forEachRestoredManifest(clusterID uuid.UUID, snapshotTag string) func(context.Context, Location, func(ManifestInfoWithContent) error) error {
	return func(ctx context.Context, location Location, f func(content ManifestInfoWithContent) error) error {
		return s.forEachManifest(ctx, clusterID, []Location{location}, ListFilter{SnapshotTag: snapshotTag}, f)
	}
}

func (s *Service) Restore(ctx context.Context, clusterID, taskID, runID uuid.UUID, target RestoreTarget) error {
	s.logger.Info(ctx, "Restore",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
		"target", target,
	)

	run := &RestoreRun{
		ClusterID:   clusterID,
		TaskID:      taskID,
		ID:          runID,
		SnapshotTag: target.SnapshotTag,
		Stage:       StageRestoreInit,
	}

	// Get cluster name
	clusterName, err := s.clusterName(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "invalid cluster")
	}

	// Get the cluster client
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "initialize: get client proxy")
	}

	// Get cluster session
	clusterSession, err := s.clusterSession(ctx, clusterID)
	if err != nil {
		s.logger.Info(ctx, "No CQL cluster session, restore can't proceed", "error", err)
		return err
	}
	defer clusterSession.Close()

	w := &restoreWorker{
		worker: worker{
			ClusterID:   clusterID,
			ClusterName: clusterName,
			TaskID:      taskID,
			RunID:       runID,
			Client:      client,
			Config:      s.config,
			Metrics:     s.metrics,
			Logger:      s.logger,
		},
		managerSession:          s.session,
		clusterSession:          clusterSession,
		forEachRestoredManifest: s.forEachRestoredManifest(clusterID, target.SnapshotTag),
	}

	if target.Continue {
		if err := w.decorateWithPrevRun(ctx, run); err != nil {
			return err
		}
		w.InsertRun(ctx, run)
		// Update run with previous progress.
		if run.PrevID != uuid.Nil {
			if err := w.clonePrevProgress(run); err != nil {
				return errors.Wrap(err, "clone progress")
			}
		}

		w.Logger.Info(ctx, "After decoration", "run", *run)

	} else {
		w.InsertRun(ctx, run)
	}

	if run.Stage.Index() <= StageCalcRestoreSize.Index() {
		run.Stage = StageCalcRestoreSize
		w.InsertRun(ctx, run)

		if err := w.RecordSize(ctx, run, target); err != nil {
			return errors.Wrap(err, "record restore size")
		}
	}

	if run.Stage.Index() <= StageRestoreFiles.Index() {
		run.Stage = StageRestoreFiles
		w.InsertRun(ctx, run)

		if err := w.restoreFiles(ctx, run, target, s.config.LocalDC); err != nil {
			return errors.Wrap(err, "restore files")
		}
	}

	run.Stage = StageRestoreDone
	w.InsertRun(ctx, run)

	return nil
}

// initRestoreHostPool creates host pool consisting of hosts that are currently restoring files
// and live hosts from a single datacenter specified in dcs. Datacenters in dcs are ordered by decreasing priority.
// If none of the nodes living in dcs is alive, pool is initialized with all living nodes.
//
// If resumed is set to false, it also initializes curProgress with information
// about all rclone jobs started on the table specified in run.
func (w *restoreWorker) initRestoreHostPool(ctx context.Context, run *RestoreRun, dcs []string, resumed bool) (chan jobUnit, error) {
	status, err := w.Client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get client status")
	}

	var liveNodes scyllaclient.NodeStatusInfoSlice

	// Find live nodes in dcs
	for _, dc := range dcs {
		if liveNodes, err = w.Client.GetLiveNodes(ctx, status.Datacenter([]string{dc})); err == nil {
			break
		}

		w.Logger.Info(ctx, "No live nodes found in dc",
			"dc", dc,
		)
	}

	if liveNodes == nil {
		// Find any live nodes
		liveNodes, err = w.Client.GetLiveNodes(ctx, status)
		if err != nil {
			return nil, errors.New("no live nodes in any dc")
		}
	}

	w.Logger.Info(ctx, "Live nodes",
		"nodes", liveNodes.Hosts(),
	)

	var (
		pool        = make(chan jobUnit, len(status))
		hostsInPool = strset.New()
	)

	// Collect table progress info from previous run
	if !resumed {
		cb := func(pr *RestoreRunProgress) error {
			// Place hosts with unfinished jobs at the beginning of the pool
			if pr.AgentJobID != 0 && pr.CompletedAt == nil {
				pool <- jobUnit{
					Host:  pr.Host,
					JobID: pr.AgentJobID,
				}
				hostsInPool.Add(pr.Host)
			}

			return nil
		}

		if err := w.ForEachTableProgress(run, cb); err != nil {
			w.Logger.Error(ctx, "Collect table progress",
				"manifest_path", run.ManifestPath,
				"keyspace", run.KeyspaceName,
				"table", run.TableName,
				"error", err,
			)
		}
	}

	w.Logger.Info(ctx, "Hosts with ongoing jobs",
		"hosts", hostsInPool.String(),
	)

	for _, n := range liveNodes {
		// Place free hosts in the pool
		if !hostsInPool.Has(n.Addr) {
			pool <- jobUnit{Host: n.Addr}
			hostsInPool.Add(n.Addr)
		}
	}

	w.Logger.Info(ctx, "Host pool",
		"pool", hostsInPool.String(),
	)

	return pool, nil
}

func (w *restoreWorker) getTableRunProgress(ctx context.Context, run *RestoreRun) (tableRunProgress, error) {
	tablePr := make(tableRunProgress)

	cb := func(pr *RestoreRunProgress) error {
		// Don't include progress created in RecordSize
		if pr.AgentJobID != 0 {
			tablePr[pr.AgentJobID] = pr
		}

		return nil
	}

	if err := w.ForEachTableProgress(run, cb); err != nil {
		return nil, err
	}

	w.Logger.Info(ctx, "Previous Table progress",
		"keyspace", run.KeyspaceName,
		"table", run.TableName,
		"table_progress", tablePr,
	)

	return tablePr, nil
}

// chooseIDsForBatch validates host's restore capabilities and
// returns slice of IDs of SSTables that the batch consists of.
func (w *restoreWorker) chooseIDsForBatch(ctx context.Context, host string, target RestoreTarget, bundleIDs chan string) ([]string, error) {
	if err := w.validateHostDiskSpace(ctx, host, target.MinFreeDiskSpace); err != nil {
		return nil, errors.Wrap(err, "validate host free disk space")
	}

	shards, err := w.Client.ShardCount(ctx, host)
	if err != nil {
		return nil, errors.Wrap(err, "get host shard count")
	}

	var (
		batchSize = target.BatchSize * int(shards)
		takenIDs  []string
		done      bool
	)

	// Create batch
	for i := 0; i < batchSize; i++ {
		select {
		case id := <-bundleIDs:
			takenIDs = append(takenIDs, id)
		default:
			done = true
		}

		if done {
			break
		}
	}

	return takenIDs, nil
}

// validateHostDiskSpace checks if host has at least minDiskSpace percent of free disk space.
func (w *restoreWorker) validateHostDiskSpace(ctx context.Context, host string, minDiskSpace int) error {
	disk, err := w.diskFreePercent(ctx, hostInfo{IP: host})
	if err != nil {
		return err
	}
	if disk < minDiskSpace {
		return errors.Errorf("Host %s has %d%% free disk space and requires %d%%", host, disk, minDiskSpace)
	}

	return nil
}

// initBundlePool creates pool of SSTable IDs that have yet to be restored.
// (It does not include ones that are currently being restored).
func initBundlePool(curProgress tableRunProgress, bundles map[string]bundle) chan string {
	var (
		pool      = make(chan string, len(bundles))
		processed = strset.New()
	)

	for _, pr := range curProgress {
		processed.Add(pr.SstableID...)
	}

	for id := range bundles {
		if !processed.Has(id) {
			pool <- id
		}
	}

	return pool
}

func getChanContent(c chan string) []string {
	content := make([]string, len(c))

	for s := range c {
		content = append(content, s)
	}

	return content
}

func isSystemKeyspace(keyspace string) bool {
	return strings.HasPrefix(keyspace, "system")
}

func sstableID(file string) string {
	return strings.SplitN(file, "-", 3)[1]
}

// groupSSTablesToBundles maps SSTable ID to its bundle.
func groupSSTablesToBundles(sstables []string) map[string]bundle {
	var bundles = make(map[string]bundle)

	for _, f := range sstables {
		id := sstableID(f)
		bundles[id] = append(bundles[id], f)
	}

	return bundles
}

func returnBatchToPool(pool chan string, ids []string) {
	for _, i := range ids {
		pool <- i
	}
}

// batchFromIDs creates batch of SSTables with IDs present in ids.
func batchFromIDs(bundles map[string]bundle, ids []string) []string {
	var batch []string

	for _, id := range ids {
		batch = append(batch, bundles[id]...)
	}

	return batch
}

// GetRestoreProgress aggregates progress for the restore run of the task
// and breaks it down by keyspace and table.json.
// If nothing was found scylla-manager.ErrNotFound is returned.
func (s *Service) GetRestoreProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (RestoreProgress, error) {
	w := &restoreWorker{
		worker: worker{
			ClusterID: clusterID,
			TaskID:    taskID,
			RunID:     runID,
		},
		managerSession: s.session,
	}

	return w.GetProgress(ctx)
}
