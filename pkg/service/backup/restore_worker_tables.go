package backup

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

type tablesWorker struct {
	restoreWorkerTools

	hosts   []restoreHost // Restore units created for currently restored location
	resumed bool          // Set to true if current run has already skipped all tables restored in previous run
}

// restoreData restores files from every location specified in restore target.
func (w *tablesWorker) restore(ctx context.Context, run *RestoreRun, target RestoreTarget) error {
	w.AwaitSchemaAgreement(ctx, w.clusterSession)

	w.Logger.Info(ctx, "Started restoring tables")
	defer w.Logger.Info(ctx, "Restoring tables finished")
	// Disable gc_grace_seconds
	for _, u := range run.Units {
		for _, t := range u.Tables {
			if err := w.DisableTableGGS(ctx, u.Keyspace, t.Table); err != nil {
				return err
			}
		}
	}
	// Restore files
	for _, l := range target.Location {
		if !w.resumed && run.Location != l.String() {
			w.Logger.Info(ctx, "Skipping location", "location", l)
			continue
		}
		if err := w.locationRestoreHandler(ctx, run, target, l); err != nil {
			return err
		}
	}

	return nil
}

func (w *tablesWorker) locationRestoreHandler(ctx context.Context, run *RestoreRun, target RestoreTarget, location backupspec.Location) error {
	if !w.resumed && run.Location != location.String() {
		w.Logger.Info(ctx, "Skipping location", "location", location)
		return nil
	}

	w.Logger.Info(ctx, "Restoring location", "location", location)
	defer w.Logger.Info(ctx, "Restoring location finished", "location", location)

	w.location = location
	run.Location = location.String()

	if err := w.initHosts(ctx, run); err != nil {
		return errors.Wrap(err, "initialize hosts")
	}

	manifestHandler := func(miwc backupspec.ManifestInfoWithContent) error {
		// Check if manifest has already been processed in previous run
		if !w.resumed && run.ManifestPath != miwc.Path() {
			w.Logger.Info(ctx, "Skipping manifest", "manifest", miwc.ManifestInfo)
			return nil
		}

		w.Logger.Info(ctx, "Restoring manifest", "manifest", miwc.ManifestInfo)
		defer w.Logger.Info(ctx, "Restoring manifest finished", "manifest", miwc.ManifestInfo)

		w.miwc = miwc
		run.ManifestPath = miwc.Path()

		iw := indexWorker{
			restoreWorkerTools: w.restoreWorkerTools,
			resumed:            w.resumed,
			hosts:              w.hosts,
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

	checkedNodes, err := w.Client.GetLiveNodesWithLocationAccess(ctx, locationStatus, remotePath)
	if err != nil {
		return errors.Wrap(err, "no live nodes in location's dc")
	}

	w.hosts = make([]restoreHost, 0)
	hostsInPool := strset.New()

	if !w.resumed {
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
	for _, n := range checkedNodes {
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

func (w *tablesWorker) startFromScratch() {
	w.resumed = true
}
