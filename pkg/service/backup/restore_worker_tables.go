// Copyright (C) 2023 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type tablesWorker struct {
	restoreWorkerTools

	hosts        []restoreHost // Restore units created for currently restored location
	continuation bool          // Defines if the worker is part of resume task that is the continuation of previous run
}

// restoreData restores files from every location specified in restore target.
func (w *tablesWorker) restore(ctx context.Context, run *RestoreRun, target RestoreTarget) error {
	if target.Continue && run.PrevID != uuid.Nil && run.Table != "" {
		w.continuation = true
	}
	if !w.continuation {
		w.initRemainingBytesMetric(ctx, run, target)
	}

	stageFunc := map[RestoreStage]func() error{
		StageRestoreDisableTGC: func() error {
			w.AwaitSchemaAgreement(ctx, w.clusterSession)
			for _, u := range run.Units {
				for _, t := range u.Tables {
					if err := w.AlterTableTombstoneGC(ctx, u.Keyspace, t.Table, modeDisabled); err != nil {
						return errors.Wrap(err, "disable table's tombstone_gc")
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
						return errors.Wrap(err, "enable table's tombstone_gc")
					}
				}
			}
			return nil
		},
	}

	for i, s := range RestoreStageOrder {
		if i < run.Stage.Index() {
			continue
		}
		run.Stage = s
		w.insertRun(ctx, run)

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
		}

		return miwc.ForEachIndexIterWithError(target.Keyspace, iw.filesMetaRestoreHandler(ctx, run, target))
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
	if run.RepairTaskID == uuid.Nil {
		run.RepairTaskID = uuid.NewTime()
	}
	w.insertRun(ctx, run)

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
		return errors.Wrap(err, "get repair target")
	}

	repairRunID := uuid.NewTime()
	return w.repairSvc.Repair(ctx, run.ClusterID, run.RepairTaskID, repairRunID, repairTarget)
}

func (w *tablesWorker) initRemainingBytesMetric(ctx context.Context, run *RestoreRun, target RestoreTarget) {
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
						w.metrics.SetRemainingBytes(run.ClusterID, target.SnapshotTag, location, miwc.DC, miwc.NodeID,
							kspace, table, size)
					}
				}
				return err
			})
		if err != nil {
			w.Logger.Info(ctx, "Couldn't count restore data size", "location", w.location)
			continue
		}
	}
}
