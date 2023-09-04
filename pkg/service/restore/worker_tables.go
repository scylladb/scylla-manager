// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
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
func (w *tablesWorker) restore(ctx context.Context, target Target) error {
	if target.Continue && w.run.PrevID != uuid.Nil && w.run.Table != "" {
		w.alreadyResumed = false
	}
	// Init metrics only on fresh start
	if w.alreadyResumed {
		w.initRestoreMetrics(ctx, target)
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
			return w.stageRestoreData(ctx, target)
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

func (w *tablesWorker) stageRestoreData(ctx context.Context, target Target) error {
	w.AwaitSchemaAgreement(ctx, w.clusterSession)
	w.logger.Info(ctx, "Started restoring tables")
	defer w.logger.Info(ctx, "Restoring tables finished")

	// Restore locations in deterministic order
	for _, l := range target.Location {
		if !w.alreadyResumed && w.run.Location != l.String() {
			w.logger.Info(ctx, "Skipping location", "location", l)
			continue
		}
		if err := w.restoreLocation(ctx, target, l); err != nil {
			return err
		}
	}
	return nil
}

func (w *tablesWorker) restoreLocation(ctx context.Context, target Target, location Location) error {
	w.logger.Info(ctx, "Restoring location", "location", location)
	defer w.logger.Info(ctx, "Restoring location finished", "location", location)

	hosts, err := w.hostsForLocation(ctx, location)
	if err != nil {
		return errors.Wrapf(err, "get hosts for location %s", location)
	}

	restoreManifest := func(miwc ManifestInfoWithContent) error {
		if !w.alreadyResumed && w.run.ManifestPath != miwc.Path() {
			w.logger.Info(ctx, "Skipping manifest", "manifest", miwc.ManifestInfo)
			return nil
		}

		w.logger.Info(ctx, "Restoring manifest", "manifest", miwc.ManifestInfo)
		defer w.logger.Info(ctx, "Restoring manifest finished", "manifest", miwc.ManifestInfo)

		return miwc.ForEachIndexIterWithError(target.Keyspace, w.restoreDir(ctx, hosts, miwc))
	}

	return w.forEachRestoredManifest(ctx, location, restoreManifest)
}

func (w *tablesWorker) restoreDir(ctx context.Context, hosts []string, miwc ManifestInfoWithContent) func(fm FilesMeta) error {
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

		dw, err := newTablesDirWorker(ctx, w.worker, hosts, miwc, fm, w.progress)
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
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// In case all SSTables have been restored, restore can proceed even
			// with errors from some hosts.
			if len(dw.bundleIDPool) > 0 {
				return errors.Wrapf(err, "not restored bundles %v", dw.bundleIDPool.drain())
			}

			w.logger.Error(ctx, "Restore table failed on some hosts but restore will proceed",
				"keyspace", w.run.Keyspace,
				"table", w.run.Table,
				"error", err,
			)
		}

		return nil
	}
}

// hostsForLocation returns hosts living in currently restored location dc and with access to it.
func (w *tablesWorker) hostsForLocation(ctx context.Context, location Location) ([]string, error) {
	status, err := w.client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get client status")
	}

	var (
		remotePath     = location.RemotePath("")
		locationStatus = status
	)
	// In case location does not have specified dc, use nodes from all dcs.
	if location.DC != "" {
		locationStatus = status.Datacenter([]string{location.DC})
		if len(locationStatus) == 0 {
			return nil, errors.Errorf("no nodes in location's datacenter: %s", location)
		}
	}

	nodes, err := w.client.GetNodesWithLocationAccess(ctx, locationStatus, remotePath)
	if err != nil {
		return nil, errors.Wrap(err, "no live nodes in location's dc")
	}

	var hosts []string
	for _, n := range nodes {
		hosts = append(hosts, n.Addr)
	}

	w.logger.Info(ctx, "Found hosts with location access", "location", location, "hosts", hosts)
	return hosts, nil
}

func (w *tablesWorker) stageRepair(ctx context.Context) error {
	var keyspace []string
	for _, u := range w.run.Units {
		for _, t := range u.Tables {
			keyspace = append(keyspace, fmt.Sprintf("%s.%s", u.Keyspace, t.Table))
		}
	}
	repairProps, err := json.Marshal(map[string]any{
		"keyspace": keyspace,
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

func (w *tablesWorker) initRestoreMetrics(ctx context.Context, target Target) {
	for _, location := range target.Location {
		err := w.forEachRestoredManifest(
			ctx,
			location,
			func(miwc ManifestInfoWithContent) error {
				sizePerTableAndKeyspace := make(map[string]map[string]int64)
				err := miwc.ForEachIndexIterWithError(
					target.Keyspace,
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
							SnapshotTag: target.SnapshotTag,
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
			SnapshotTag: target.SnapshotTag,
		}
		w.metrics.SetProgress(progressLabels, 0)
		if err != nil {
			w.logger.Info(ctx, "Couldn't count restore data size")
			continue
		}
	}
}
