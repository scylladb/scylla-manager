// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"golang.org/x/sync/errgroup"
)

type tablesWorker struct {
	worker

	repairSvc *repair.Service
	progress  *TotalRestoreProgress
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

func newTablesWorker(w worker, repairSvc *repair.Service, totalBytes int64) *tablesWorker {
	return &tablesWorker{
		worker:    w,
		repairSvc: repairSvc,
		progress:  NewTotalRestoreProgress(totalBytes),
	}
}

// restore files from every location specified in restore target.
func (w *tablesWorker) restore(ctx context.Context) error {
	hosts := strset.New(w.client.Config().Hosts...).List()

	stageFunc := map[Stage]func() error{
		StageDropViews: func() error {
			return w.stageDropViews(ctx)
		},
		StageDisableCompaction: func() error {
			return w.stageDisableCompaction(ctx, hosts)
		},
		StageDisableTGC: func() error {
			return w.stageDisableTGC(ctx)
		},
		StageData: func() error {
			return w.stageRestoreData(ctx)
		},
		StageRepair: func() error {
			return w.stageRepair(ctx)
		},
		StageEnableTGC: func() error {
			return w.stageEnableTGC(ctx)
		},
		StageEnableCompaction: func() error {
			return w.stageEnableCompaction(ctx, hosts)
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

func (w *tablesWorker) stageDropViews(ctx context.Context) error {
	for _, v := range w.run.Views {
		if err := w.DropView(ctx, v); err != nil {
			return errors.Wrapf(err, "drop %s.%s", v.Keyspace, v.View)
		}
	}
	return nil
}

func (w *tablesWorker) stageRecreateViews(ctx context.Context) error {
	for _, v := range w.run.Views {
		if err := w.CreateView(ctx, v); err != nil {
			return errors.Wrapf(err, "recreate %s.%s with statement %s", v.Keyspace, v.View, v.CreateStmt)
		}
		if err := w.WaitForViewBuilding(ctx, v); err != nil {
			return errors.Wrapf(err, "wait for %s.%s", v.Keyspace, v.View)
		}
	}
	return nil
}

func (w *tablesWorker) stageDisableCompaction(ctx context.Context, hosts []string) error {
	return parallel.Run(len(hosts), parallel.NoLimit, func(i int) error {
		host := hosts[i]
		for _, u := range w.run.Units {
			for _, t := range u.Tables {
				if err := w.client.DisableAutoCompaction(ctx, host, u.Keyspace, t.Table); err != nil {
					return errors.Wrapf(err, "disable autocompaction on %s", host)
				}
			}
		}
		return nil
	}, parallel.NopNotify)
}

func (w *tablesWorker) stageEnableCompaction(ctx context.Context, hosts []string) error {
	return parallel.Run(len(hosts), parallel.NoLimit, func(i int) error {
		host := hosts[i]
		for _, u := range w.run.Units {
			for _, t := range u.Tables {
				if err := w.client.EnableAutoCompaction(ctx, host, u.Keyspace, t.Table); err != nil {
					return errors.Wrapf(err, "enable autocompaction on %s", host)
				}
			}
		}
		return nil
	}, parallel.NopNotify)
}

func (w *tablesWorker) stageDisableTGC(ctx context.Context) error {
	w.AwaitSchemaAgreement(ctx, w.clusterSession)
	for _, u := range w.run.Units {
		for _, t := range u.Tables {
			if err := w.AlterTableTombstoneGC(ctx, u.Keyspace, t.Table, modeDisabled); err != nil {
				return errors.Wrapf(err, "disable %s.%s tombstone_gc", u.Keyspace, t.Table)
			}
		}
	}
	return nil
}

func (w *tablesWorker) stageEnableTGC(ctx context.Context) error {
	w.AwaitSchemaAgreement(ctx, w.clusterSession)
	for _, u := range w.run.Units {
		for _, t := range u.Tables {
			if err := w.AlterTableTombstoneGC(ctx, u.Keyspace, t.Table, t.TombstoneGC); err != nil {
				return errors.Wrapf(err, "enable %s.%s tombstone_gc", u.Keyspace, t.Table)
			}
		}
	}
	return nil
}

func (w *tablesWorker) stageRestoreData(ctx context.Context) error {
	w.AwaitSchemaAgreement(ctx, w.clusterSession)
	w.logger.Info(ctx, "Started restoring tables")
	defer func() {
		w.logger.Info(ctx, "Restoring tables finished")
		w.LogRestoreStats(context.Background())
	}()

	workload, err := w.IndexWorkload(ctx)
	if err != nil {
		return errors.Wrap(err, "index workload")
	}

	hostsS := strset.New()
	for _, h := range w.target.locationHosts {
		hostsS.Add(h...)
	}
	hosts := hostsS.List()
	hostToShard, err := w.client.HostsShardCount(ctx, hosts)
	if err != nil {
		return errors.Wrap(err, "get hosts shard count")
	}

	bd := newBatchDispatcher(workload, hostToShard)
	w.LogFileStats(ctx, bd.workload)

	f := func(n int) (err error) {
		h := hosts[n]
		eg := errgroup.Group{}

		for i := 0; i < w.target.TableParallel; i++ {
			id := uint(i)
			w.SetBatchSizeMetric(h, id, 0)
			eg.Go(func() error {
				for {
					// Download and stream in parallel.
					b, ok := bd.DispatchBatch(h, id)
					if !ok {
						w.logger.Info(ctx, "No more batches to restore", "host", h, "worker ID", id)
						return nil
					}
					w.IncreaseBatchSizeMetric(h, id, b.Size)
					w.logger.Info(ctx, "Got batch to restore",
						"host", h,
						"worker ID", id,
						"keyspace", b.Keyspace,
						"table", b.Table,
						"size", b.Size,
						"sstable count", b.SSTables,
					)

					pr, err := w.newRunProgress(ctx, h, id, b)
					if err != nil {
						return errors.Wrap(err, "create new run progress")
					}
					if err := w.restoreBatch(ctx, h, id, b, pr); err != nil {
						return errors.Wrap(err, "restore batch")
					}
				}
			})
		}

		return eg.Wait()
	}

	notify := func(n int, err error) {
		w.logger.Error(ctx, "Failed to restore files on host",
			"host", hosts[n],
			"error", err,
		)
	}

	err = parallel.Run(len(hosts), w.target.Parallel, f, notify)
	if err == nil {
		for _, tw := range bd.workload {
			if tw.Size != 0 {
				panic(fmt.Sprintf("expected all data to be restored (%s.%s - %d)", tw.Keyspace, tw.Table, tw.Size))
			}
		}
	}
	return err
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

func (w *tablesWorker) SetProgressMetric(progress float64) {
	l := metrics.RestoreProgressLabels{
		ClusterID:   w.run.ClusterID.String(),
		SnapshotTag: w.target.SnapshotTag,
	}
	w.metrics.SetProgress(l, progress)
}

func (w *tablesWorker) SetRemainingBytesMetric(workload RemoteDirWorkload) {
	labels := metrics.RestoreBytesLabels{
		ClusterID:   workload.ClusterID.String(),
		SnapshotTag: w.run.SnapshotTag,
		Location:    workload.Location.String(),
		DC:          workload.DC,
		Node:        workload.NodeID,
		Keyspace:    workload.Keyspace,
		Table:       workload.Table,
	}
	w.metrics.SetRemainingBytes(labels, workload.Size)
}

func (w *tablesWorker) DecreaseRemainingBytesMetric(b batch) {
	labels := metrics.RestoreBytesLabels{
		ClusterID:   b.ClusterID.String(),
		SnapshotTag: w.run.SnapshotTag,
		Location:    b.Location.String(),
		DC:          b.DC,
		Node:        b.NodeID,
		Keyspace:    b.Keyspace,
		Table:       b.Table,
	}
	w.metrics.DecreaseRemainingBytes(labels, b.Size)
	w.progress.Update(b.Size)

	progressLabels := metrics.RestoreProgressLabels{
		ClusterID:   w.run.ClusterID.String(),
		SnapshotTag: w.run.SnapshotTag,
	}
	w.metrics.SetProgress(progressLabels, w.progress.CurrentProgress())
}

func (w *tablesWorker) SetRestoreStateMetric(host string, workerID uint, state metrics.RestoreState) {
	w.metrics.SetRestoreState(w.run.ClusterID, w.target.SnapshotTag, host, workerID, state)
}

func (w *tablesWorker) SetBatchSizeMetric(host string, workerID uint, size int64) {
	w.metrics.SetBatchSize(w.run.ClusterID, host, workerID, size)
}

func (w *tablesWorker) IncreaseBatchSizeMetric(host string, workerID uint, size int64) {
	w.metrics.IncreaseBatchSize(w.run.ClusterID, host, workerID, size)
}
