// Copyright (C) 2023 ScyllaDB

package backup

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
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type tablesWorker struct {
	restoreWorkerTools

	hosts        []restoreHost // Restore units created for currently restored location
	continuation bool          // Defines if the worker is part of resume task that is the continuation of previous run
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

// restoreData restores files from every location specified in restore target.
func (w *tablesWorker) restore(ctx context.Context, run *RestoreRun, target RestoreTarget) error {
	if target.Continue && run.PrevID != uuid.Nil && run.Table != "" {
		w.continuation = true
	}
	if !w.continuation {
		w.initRestoreMetrics(ctx, run, target)
	}

	stageFunc := map[RestoreStage]func() error{
		StageRestoreDropViews: func() error {
			for _, v := range run.Views {
				if err := w.DropView(ctx, v); err != nil {
					return errors.Wrapf(err, "drop %s.%s", v.Keyspace, v.View)
				}
			}
			return nil
		},
		StageRestoreDisableTGC: func() error {
			w.AwaitSchemaAgreement(ctx, w.clusterSession)
			for _, u := range run.Units {
				for _, t := range u.Tables {
					if err := w.AlterTableTombstoneGC(ctx, u.Keyspace, t.Table, modeDisabled); err != nil {
						return errors.Wrapf(err, "disable %s.%s tombstone_gc", u.Keyspace, t.Table)
					}
				}
			}
			return nil
		},
		StageRestoreData: func() error {
			return w.stageRestoreData(ctx, run, target)
		},
		StageRestoreRepair: func() error {
			return w.stageRepair(ctx, run, target)
		},
		StageRestoreEnableTGC: func() error {
			w.AwaitSchemaAgreement(ctx, w.clusterSession)
			for _, u := range run.Units {
				for _, t := range u.Tables {
					if err := w.AlterTableTombstoneGC(ctx, u.Keyspace, t.Table, t.TombstoneGC); err != nil {
						return errors.Wrapf(err, "enable %s.%s tombstone_gc", u.Keyspace, t.Table)
					}
				}
			}
			return nil
		},
		StageRestoreRecreateViews: func() error {
			for _, v := range run.Views {
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

	for i, s := range RestoreStageOrder() {
		if i < run.Stage.Index() {
			continue
		}
		run.Stage = s
		w.insertRun(ctx, run)
		w.Logger.Info(ctx, "Executing stage", "name", s)

		if f, ok := stageFunc[s]; ok {
			if err := f(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *tablesWorker) stageRestoreData(ctx context.Context, run *RestoreRun, target RestoreTarget) error {
	w.AwaitSchemaAgreement(ctx, w.clusterSession)
	w.Logger.Info(ctx, "Started restoring tables")
	defer w.Logger.Info(ctx, "Restoring tables finished")

	// Restore locations in deterministic order
	for _, l := range target.Location {
		if w.continuation && run.Location != l.String() {
			w.Logger.Info(ctx, "Skipping location", "location", l)
			continue
		}
		if err := w.locationRestoreHandler(ctx, run, target, l); err != nil {
			return err
		}
	}
	return nil
}

func (w *tablesWorker) locationRestoreHandler(ctx context.Context, run *RestoreRun, target RestoreTarget, location Location) error {
	w.Logger.Info(ctx, "Restoring location", "location", location)
	defer w.Logger.Info(ctx, "Restoring location finished", "location", location)

	w.location = location
	run.Location = location.String()

	if err := w.initHosts(ctx, run); err != nil {
		return errors.Wrap(err, "initialize hosts")
	}

	manifestHandler := func(miwc ManifestInfoWithContent) error {
		// Check if manifest has already been processed in previous run
		if w.continuation && run.ManifestPath != miwc.Path() {
			w.Logger.Info(ctx, "Skipping manifest", "manifest", miwc.ManifestInfo)
			return nil
		}

		w.Logger.Info(ctx, "Restoring manifest", "manifest", miwc.ManifestInfo)
		defer w.Logger.Info(ctx, "Restoring manifest finished", "manifest", miwc.ManifestInfo)

		run.ManifestPath = miwc.Path()

		iw := indexWorker{
			restoreWorkerTools: w.restoreWorkerTools,
			continuation:       w.continuation,
			hosts:              w.hosts,
			miwc:               miwc,
			progress:           w.progress,
		}

		err := miwc.ForEachIndexIterWithError(target.Keyspace, iw.filesMetaRestoreHandler(ctx, run, target))
		// We have to keep track of continuation from filesMetaHandler,
		// so that can stop skipping not restored manifests and locations.
		w.continuation = iw.continuation
		return err
	}

	return w.forEachRestoredManifest(ctx, w.location, manifestHandler)
}

// initHosts creates hosts living in currently restored location's dc and with access to it.
// All running hosts are located at the beginning of the result slice.
func (w *tablesWorker) initHosts(ctx context.Context, run *RestoreRun) error {
	status, err := w.Client.Status(ctx)
	if err != nil {
		return errors.Wrap(err, "get client status")
	}

	var (
		remotePath     = w.location.RemotePath("")
		locationStatus = status
	)
	// In case location does not have specified dc, use nodes from all dcs.
	if w.location.DC != "" {
		locationStatus = status.Datacenter([]string{w.location.DC})
		if len(locationStatus) == 0 {
			return errors.Errorf("no nodes in location's datacenter: %s", w.location)
		}
	}

	nodes, err := w.Client.GetNodesWithLocationAccess(ctx, locationStatus, remotePath)
	if err != nil {
		return errors.Wrap(err, "no live nodes in location's dc")
	}

	w.hosts = make([]restoreHost, 0)
	hostsInPool := strset.New()

	if w.continuation {
		// Place hosts with unfinished jobs at the beginning
		cb := func(pr *RestoreRunProgress) {
			if !validateTimeIsSet(pr.RestoreCompletedAt) {
				// Pointer cannot be stored directly because it is overwritten in each
				// iteration of ForEachTableProgress.
				ongoing := *pr
				// Reset rclone stats for unfinished rclone jobs - they will be recreated from rclone job progress.
				if !validateTimeIsSet(pr.DownloadCompletedAt) {
					pr.Downloaded = 0
					pr.Skipped = 0
					pr.Failed = 0
				}
				w.hosts = append(w.hosts, restoreHost{
					Host:               pr.Host,
					OngoingRunProgress: &ongoing,
				})

				hostsInPool.Add(pr.Host)
			}
		}

		w.ForEachTableProgress(ctx, run, cb)
	}

	// Place free hosts in the pool
	for _, n := range nodes {
		if !hostsInPool.Has(n.Addr) {
			w.hosts = append(w.hosts, restoreHost{
				Host: n.Addr,
			})

			hostsInPool.Add(n.Addr)
		}
	}

	w.Logger.Info(ctx, "Initialized restore hosts", "hosts", w.hosts)
	return nil
}

func (w *tablesWorker) stageRepair(ctx context.Context, run *RestoreRun, _ RestoreTarget) error {
	var keyspace []string
	for _, u := range run.Units {
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

	repairTarget, err := w.repairSvc.GetTarget(ctx, run.ClusterID, repairProps)
	if err != nil {
		if errors.Is(err, repair.ErrEmptyRepair) {
			return nil
		}
		return errors.Wrap(err, "get repair target")
	}

	if run.RepairTaskID == uuid.Nil {
		run.RepairTaskID = uuid.NewTime()
	}
	w.insertRun(ctx, run)
	repairRunID := uuid.NewTime()

	return w.repairSvc.Repair(ctx, run.ClusterID, run.RepairTaskID, repairRunID, repairTarget)
}

func (w *tablesWorker) initRestoreMetrics(ctx context.Context, run *RestoreRun, target RestoreTarget) {
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
							ClusterID:   run.ClusterID.String(),
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
			ClusterID:   run.ClusterID.String(),
			SnapshotTag: target.SnapshotTag,
		}
		w.metrics.SetProgress(progressLabels, 0)
		if err != nil {
			w.Logger.Info(ctx, "Couldn't count restore data size", "location", w.location)
			continue
		}
	}
}
