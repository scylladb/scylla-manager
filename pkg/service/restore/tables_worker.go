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
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type tablesWorker struct {
	worker

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

func newTablesWorker(w worker, repairSvc *repair.Service, totalBytes int64) (*tablesWorker, error) {
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

	return &tablesWorker{
		worker:       w,
		tableVersion: versions,
		repairSvc:    repairSvc,
		progress:     NewTotalRestoreProgress(totalBytes),
	}, nil
}

// restore files from every location specified in restore target.
func (w *tablesWorker) restore(ctx context.Context) error {
	// Init metrics only on fresh start
	if w.run.PrevID == uuid.Nil {
		w.initRestoreMetrics(ctx)
	}

	stageFunc := map[Stage]func() error{
		StageDropViews: func() error {
			for _, v := range w.run.Views {
				if err := w.DropView(ctx, v); err != nil {
					return errors.Wrapf(err, "drop %s.%s", v.Keyspace, v.View)
				}
			}
			return nil
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
			for _, v := range w.run.Views {
				if err := w.CreateView(ctx, v); err != nil {
					return errors.Wrapf(err, "recreate %s.%s with statement %s", v.Keyspace, v.View, v.CreateStmt)
				}
				if err := w.WaitForViewBuilding(ctx, v); err != nil {
					return errors.Wrapf(err, "wait for %s.%s", v.Keyspace, v.View)
				}
			}
			return nil
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

	workload, err := w.IndexWorkload(ctx, w.target.Location)
	if err != nil {
		return err
	}
	w.initMetrics(workload)

	hostsS := strset.New()
	for _, h := range w.target.locationHosts {
		hostsS.Add(h...)
	}
	hosts := hostsS.List()

	hostToShard, err := w.client.HostsShardCount(ctx, hosts)
	if err != nil {
		return errors.Wrap(err, "get hosts shard count")
	}
	for h, sh := range hostToShard {
		w.logger.Info(ctx, "Host shard count", "host", h, "shards", sh)
	}

	bd := newBatchDispatcher(workload, w.target.BatchSize, hostToShard, w.target.locationHosts)

	f := func(n int) (err error) {
		h := hosts[n]
		for {
			// Download and stream in parallel
			b, ok := bd.DispatchBatch(h)
			if !ok {
				w.logger.Info(ctx, "No more batches to restore", "host", h)
				return nil
			}
			w.metrics.IncreaseBatchSize(w.run.ClusterID, h, b.Size)
			w.logger.Info(ctx, "Got batch to restore",
				"host", h,
				"keyspace", b.Keyspace,
				"table", b.Table,
				"size", b.Size,
				"sstable count", len(b.SSTables),
			)

			pr, err := w.newRunProgress(ctx, h, b)
			if err != nil {
				return errors.Wrap(err, "create new run progress")
			}
			if err := w.restoreBatch(ctx, b, pr); err != nil {
				return errors.Wrap(err, "restore batch")
			}
			w.decreaseRemainingBytesMetric(b)
		}
	}

	notify := func(n int, err error) {
		w.logger.Error(ctx, "Failed to restore files on host",
			"host", hosts[n],
			"error", err,
		)
	}

	err = parallel.Run(len(hosts), w.target.Parallel, f, notify)
	if err == nil {
		return bd.ValidateAllDispatched()
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

func (w *tablesWorker) initRestoreMetrics(ctx context.Context) {
	for _, location := range w.target.Location {
		err := w.forEachManifest(
			ctx,
			location,
			func(miwc ManifestInfoWithContent) error {
				sizePerTableAndKeyspace := make(map[string]map[string]int64)
				err := miwc.ForEachIndexIterWithError(
					nil,
					func(fm FilesMeta) error {
						if !unitsContainTable(w.run.Units, fm.Keyspace, fm.Table) {
							return nil
						}

						if sizePerTableAndKeyspace[fm.Keyspace] == nil {
							sizePerTableAndKeyspace[fm.Keyspace] = make(map[string]int64)
						}
						sizePerTableAndKeyspace[fm.Keyspace][fm.Table] += fm.Size
						return nil
					})
				for kspace, sizePerTable := range sizePerTableAndKeyspace {
					for table, size := range sizePerTable {
						labels := metrics.RestoreBytesLabels{
							ClusterID:   w.run.ClusterID.String(),
							SnapshotTag: w.target.SnapshotTag,
							Location:    location.String(),
							DC:          miwc.DC,
							Node:        miwc.NodeID,
							Keyspace:    kspace,
							Table:       table,
						}
						w.metrics.SetRemainingBytes(labels, size)
					}
				}
				return err
			})
		progressLabels := metrics.RestoreProgressLabels{
			ClusterID:   w.run.ClusterID.String(),
			SnapshotTag: w.target.SnapshotTag,
		}
		w.metrics.SetProgress(progressLabels, 0)
		if err != nil {
			w.logger.Info(ctx, "Couldn't count restore data size")
			continue
		}
	}
}
