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
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/multierr"
)

type tablesWorker struct {
	worker

	repairSvc *repair.Service
	progress  *TotalRestoreProgress
	// When set to false, tablesWorker will skip restoration of location/manifest/table
	// until it encounters the one present in run.
	alreadyResumed bool
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
		worker:         w,
		repairSvc:      repairSvc,
		alreadyResumed: true,
		progress:       NewTotalRestoreProgress(totalBytes),
	}
}

// restore files from every location specified in restore target.
func (w *tablesWorker) restore(ctx context.Context) error {
	if w.target.Continue && w.run.PrevID != uuid.Nil && w.run.Table != "" {
		w.alreadyResumed = false
	}
	// Init metrics only on fresh start
	if w.alreadyResumed {
		w.initRestoreMetrics(ctx)
	}
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
	defer w.logger.Info(ctx, "Restoring tables finished")

	// Restore locations in deterministic order
	for _, l := range w.target.Location {
		if !w.alreadyResumed && w.run.Location != l.String() {
			w.logger.Info(ctx, "Skipping location", "location", l)
			continue
		}
		if err := w.restoreLocation(ctx, l); err != nil {
			return err
		}
	}
	return nil
}

func (w *tablesWorker) restoreLocation(ctx context.Context, location Location) error {
	w.logger.Info(ctx, "Restoring location", "location", location)
	defer w.logger.Info(ctx, "Restoring location finished", "location", location)

	restoreManifest := func(miwc ManifestInfoWithContent) error {
		if !w.alreadyResumed && w.run.ManifestPath != miwc.Path() {
			w.logger.Info(ctx, "Skipping manifest", "manifest", miwc.ManifestInfo)
			return nil
		}

		w.logger.Info(ctx, "Restoring manifest", "manifest", miwc.ManifestInfo)
		defer w.logger.Info(ctx, "Restoring manifest finished", "manifest", miwc.ManifestInfo)

		return miwc.ForEachIndexIterWithError(w.target.Keyspace, w.restoreDir(ctx, miwc))
	}

	return w.forEachManifest(ctx, location, restoreManifest)
}

func (w *tablesWorker) restoreDir(ctx context.Context, miwc ManifestInfoWithContent) func(fm FilesMeta) error {
	return func(fm FilesMeta) error {
		if !w.alreadyResumed {
			if w.run.Keyspace != fm.Keyspace || w.run.Table != fm.Table {
				w.logger.Info(ctx, "Skipping table", "keyspace", fm.Keyspace, "table", fm.Table)
				return nil
			}
		}

		w.logger.Info(ctx, "Restoring table", "keyspace", fm.Keyspace, "table", fm.Table)
		defer w.logger.Info(ctx, "Restoring table finished", "keyspace", fm.Keyspace, "table", fm.Table)

		w.run.Location = miwc.Location.String()
		w.run.ManifestPath = miwc.Path()
		w.run.Table = fm.Table
		w.run.Keyspace = fm.Keyspace
		w.insertRun(ctx)

		dw, err := newTablesDirWorker(ctx, w.worker, miwc, fm, w.progress)
		if err != nil {
			return errors.Wrap(err, "create dir worker")
		}
		if !w.alreadyResumed {
			if err := dw.resumePrevProgress(); err != nil {
				return errors.Wrap(err, "resume prev run progress")
			}
		}
		w.alreadyResumed = true

		if err := dw.restore(ctx); err != nil {
			return multierr.Append(err, ctx.Err())
		}
		return nil
	}
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
					w.target.Keyspace,
					func(fm FilesMeta) error {
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
