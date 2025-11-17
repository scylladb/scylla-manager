// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/multierr"
)

type tablesWorker struct {
	worker

	hosts        []string
	hostShardCnt map[string]uint
	tableVersion map[TableName]string
	repairSvc    *repair.Service
	progress     *TotalRestoreProgress
}

// TotalRestoreProgress is a struct that holds information about the total progress of the restore job.
type TotalRestoreProgress struct {
	restoredBytes       int64
	totalBytesToRestore int64
	mu                  sync.RWMutex
}

func NewTotalRestoreProgress(totalBytesToRestore int64) *TotalRestoreProgress {
	return &TotalRestoreProgress{
		restoredBytes:       0,
		totalBytesToRestore: totalBytesToRestore,
	}
}

// CurrentProgress returns current progress of the restore job in percentage.
func (p *TotalRestoreProgress) CurrentProgress() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.totalBytesToRestore == 0 {
		return 100
	}

	if p.restoredBytes == 0 {
		return 0
	}

	progress := float64(p.restoredBytes) / float64(p.totalBytesToRestore) * 100
	return progress
}

// Update updates the progress of the restore job, caller should provide number of bytes restored by its job.
func (p *TotalRestoreProgress) Update(bytesRestored int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.restoredBytes += bytesRestored
}

func newTablesWorker(ctx context.Context, w worker, repairSvc *repair.Service, totalBytes int64) (*tablesWorker, error) {
	versions := make(map[TableName]string)
	for _, u := range w.run.Units {
		for _, t := range u.Tables {
			v, err := query.GetTableVersion(w.clusterSession, u.Keyspace, t.Table)
			if err != nil {
				return nil, errors.Wrapf(err, "get %s.%s version", u.Keyspace, t.Table)
			}
			versions[TableName{
				Keyspace: u.Keyspace,
				Table:    t.Table,
			}] = v
		}
	}

	var hosts []string
	for _, l := range w.target.locationInfo {
		hosts = append(hosts, l.AllHosts()...)
	}

	hostToShard, err := w.client.HostsShardCount(ctx, hosts)
	if err != nil {
		return nil, errors.Wrap(err, "get hosts shard count")
	}
	for h, sh := range hostToShard {
		w.logger.Info(ctx, "Host shard count", "host", h, "shards", sh)
	}

	return &tablesWorker{
		worker:       w,
		hosts:        hosts,
		hostShardCnt: hostToShard,
		tableVersion: versions,
		repairSvc:    repairSvc,
		progress:     NewTotalRestoreProgress(totalBytes),
	}, nil
}

// restore files from every location specified in restore target.
func (w *tablesWorker) restore(ctx context.Context) error {
	stageFunc := map[Stage]func() error{
		StageDropViews: func() error {
			return w.stageDropViews(ctx)
		},
		StageDisableTGC: func() error {
			w.AwaitSchemaAgreement(ctx, w.clusterSession)
			for _, u := range w.run.Units {
				for _, t := range u.Tables {
					if err := w.AlterTableTombstoneGC(ctx, u.Keyspace, t.Table, modeDisabled); err != nil {
						return errors.Wrapf(err, "disable %s.%s tombstone_gc", u.Keyspace, t.Table)
					}
				}
			}
			return nil
		},
		StageData: func() error {
			return w.stageRestoreData(ctx)
		},
		StageRepair: func() error {
			return w.stageRepair(ctx)
		},
		StageEnableTGC: func() error {
			w.AwaitSchemaAgreement(ctx, w.clusterSession)
			for _, u := range w.run.Units {
				for _, t := range u.Tables {
					if err := w.AlterTableTombstoneGC(ctx, u.Keyspace, t.Table, t.TombstoneGC); err != nil {
						return errors.Wrapf(err, "enable %s.%s tombstone_gc", u.Keyspace, t.Table)
					}
				}
			}
			return nil
		},
		StageRecreateViews: func() error {
			return w.stageRecreateViews(ctx)
		},
	}

	for i, s := range StageOrder() {
		if i < w.run.Stage.Index() {
			continue
		}
		w.run.Stage = s
		w.insertRun(ctx)
		w.logger.Info(ctx, "Executing stage", "name", s)

		if f, ok := stageFunc[s]; ok {
			if err := f(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *tablesWorker) stageRestoreData(ctx context.Context) error {
	w.AwaitSchemaAgreement(ctx, w.clusterSession)
	w.logger.Info(ctx, "Started restoring tables")
	defer w.logger.Info(ctx, "Restoring tables finished")

	workload, err := w.IndexWorkload(ctx, w.target.locationInfo)
	if err != nil {
		return err
	}
	w.initMetrics(workload)

	if w.target.Method == MethodNative {
		if err := workload.NativeRestoreSupport(); err != nil {
			return errors.Wrap(err, "ensure native restore support")
		}
	}

	// This defer is outside of target field check for improved safety.
	// We always want to enable auto compaction outside the restore.
	defer func() {
		if err := w.setAutoCompaction(context.Background(), w.hosts, true); err != nil {
			w.logger.Error(ctx, "Couldn't enable auto compaction", "error", err)
		}
	}()
	if !w.target.AllowCompaction {
		if err := w.setAutoCompaction(ctx, w.hosts, false); err != nil {
			return errors.Wrapf(err, "disable auto compaction")
		}
	}

	// Same as above.
	// We always want to pin agent to CPUs outside the restore.
	defer func() {
		if err := w.pinAgentCPU(context.Background(), w.hosts, true); err != nil {
			w.logger.Error(ctx, "Couldn't re-pin agent to CPUs", "error", err)
		}
	}()
	if w.target.UnpinAgentCPU {
		if err := w.pinAgentCPU(ctx, w.hosts, false); err != nil {
			return errors.Wrapf(err, "unpin agent from CPUs")
		}
	}

	bd := newBatchDispatcher(workload, w.target.BatchSize, w.hostShardCnt, w.target.locationInfo)

	f := func(n int) error {
		host := w.hosts[n]
		dc, err := w.client.HostDatacenter(ctx, host)
		if err != nil {
			return errors.Wrapf(err, "get host %s data center", host)
		}
		hi := w.hostInfo(host, dc, w.hostShardCnt[host])
		w.logger.Info(ctx, "Host info", "host", hi.Host, "transfers", hi.Transfers, "rate limit", hi.RateLimit)

		ip, err := netip.ParseAddr(host)
		if err != nil {
			return errors.Wrap(err, "parse host IP address")
		}
		nc, ok := w.nodeConfig[ip]
		if !ok {
			return errors.Errorf("unknown node IP %s, known node IPs %v", ip, slices.Collect(maps.Keys(w.nodeConfig)))
		}

		// Ensure that there are not leftovers from previous SM or manual restores in the upload dirs
		if err := w.cleanHostUploadDirs(ctx, host); err != nil {
			return errors.Wrapf(err, "clean host %s upload dirs", host)
		}

		if err := w.hostNativeRestoreSupport(ctx, hi.Host, nc.NodeInfo, w.target.Location); err == nil {
			reset, err := w.client.ScyllaControlTaskUserTTL(ctx, host)
			if err != nil {
				return err
			}
			defer reset()
		}

		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := w.checkAvailableDiskSpace(ctx, hi.Host); err != nil {
				return errors.Wrap(err, "validate free disk space")
			}
			b, ok := bd.DispatchBatch(ctx, hi.Host)
			if !ok {
				w.logger.Info(ctx, "No more batches to restore", "host", hi.Host)
				return nil
			}
			w.onBatchDispatch(ctx, b, host)

			if err := w.restoreBatch(ctx, hi, nc, b); err != nil {
				err = multierr.Append(errors.Wrap(err, "restore batch"), bd.ReportFailure(hi.Host, b))
				w.logger.Error(ctx, "Failed to restore batch",
					"host", hi.Host,
					"keyspace", b.Keyspace,
					"table", b.Table,
					"error", err)
				continue
			}
			bd.ReportSuccess(b)
		}
	}

	notify := func(n int, err error) {
		w.logger.Error(ctx, "Failed to restore files on host",
			"host", w.hosts[n],
			"error", err,
		)
	}

	err = parallel.Run(len(w.hosts), w.target.Parallel, f, notify)
	if err == nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return bd.ValidateAllDispatched()
	}
	return err
}

func (w *tablesWorker) restoreBatch(ctx context.Context, hi HostInfo, nc configcache.NodeConfig, b batch) error {
	if w.target.Method == MethodRclone {
		return w.rcloneBatchRestore(ctx, hi, b)
	}

	// Handle lack of native restore support on the host level
	if err := hostNativeRestoreSupport(nc.NodeInfo, w.target.Location, w.target.Method); err != nil {
		if w.target.Method == MethodNative {
			return errors.Wrap(err, "ensure native restore support")
		}
		return w.rcloneBatchRestore(ctx, hi, b)
	}

	if w.target.Method == MethodNative {
		if err := b.NativeRestoreSupport(); err != nil {
			return errors.Wrap(err, "ensure native restore support")
		}
		return w.nativeBatchRestore(ctx, hi.Host, nc, b)
	}

	if w.target.Method == MethodAuto {
		if err := w.batchNativeRestoreSupport(ctx, hi.Host, b); err != nil {
			return w.rcloneBatchRestore(ctx, hi, b)
		}
		return w.nativeBatchRestore(ctx, hi.Host, nc, b)
	}

	return errors.New("unknown method: " + string(w.target.Method))
}

func (w *tablesWorker) stageRepair(ctx context.Context) error {
	var keyspace []string
	for _, u := range w.run.Units {
		for _, t := range u.Tables {
			keyspace = append(keyspace, fmt.Sprintf("%s.%s", u.Keyspace, t.Table))
		}
	}
	repairProps, err := json.Marshal(map[string]any{
		"keyspace":  keyspace,
		"intensity": 0,
		"parallel":  0,
	})
	if err != nil {
		return errors.Wrap(err, "parse repair properties")
	}

	repairTarget, err := w.repairSvc.GetTarget(ctx, w.run.ClusterID, repairProps)
	if err != nil {
		if errors.Is(err, repair.ErrEmptyRepair) {
			return nil
		}
		return errors.Wrap(err, "get repair target")
	}

	if w.run.RepairTaskID == uuid.Nil {
		w.run.RepairTaskID = uuid.NewTime()
	}
	w.insertRun(ctx)
	repairRunID := uuid.NewTime()

	return w.repairSvc.Repair(ctx, w.run.ClusterID, w.run.RepairTaskID, repairRunID, repairTarget)
}

// Disables auto compaction on all provided hosts and units.
func (w *tablesWorker) setAutoCompaction(ctx context.Context, hosts []string, enabled bool) error {
	f := w.client.EnableAutoCompaction
	if !enabled {
		f = w.client.DisableAutoCompaction
	}
	for _, h := range hosts {
		for _, u := range w.run.Units {
			for _, t := range u.Tables {
				if err := f(ctx, h, u.Keyspace, t.Table); err != nil {
					return errors.Wrapf(err, "set autocompaction on %s to %v", h, enabled)
				}
			}
		}
	}
	return nil
}

// Pins/unpins all provided hosts to/from CPUs.
func (w *tablesWorker) pinAgentCPU(ctx context.Context, hosts []string, pin bool) error {
	err := parallel.Run(len(hosts), parallel.NoLimit,
		func(i int) error {
			if pin {
				return w.client.PinCPU(ctx, hosts[i])
			}
			return w.client.UnpinFromCPU(ctx, hosts[i])
		}, func(i int, err error) {
			w.logger.Error(ctx, "Failed to change agent CPU pinning",
				"host", hosts[i],
				"pinned", pin,
				"error", err)
		})
	return errors.Wrapf(err, "set agent CPU pinning")
}

func (w *tablesWorker) hostInfo(host, dc string, shards uint) HostInfo {
	return HostInfo{
		Host:      host,
		Transfers: hostTransfers(w.target.Transfers, shards),
		RateLimit: dcRateLimit(w.target.RateLimit, dc),
	}
}

func hostTransfers(transfers int, shards uint) int {
	if transfers == maxTransfers {
		transfers = 2 * int(shards)
	}
	return transfers
}

func dcRateLimit(limits []backup.DCLimit, dc string) int {
	defaultLimit := maxRateLimit
	for _, limit := range limits {
		if limit.DC == dc {
			return limit.Limit
		}
		if limit.DC == "" {
			defaultLimit = limit.Limit
		}
	}
	return defaultLimit
}
