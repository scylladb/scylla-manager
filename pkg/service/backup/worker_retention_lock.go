// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"
	stdErr "errors"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"golang.org/x/sync/errgroup"
)

// retentionLockConfig holds retention lock parameters.
type retentionLockConfig struct {
	until    time.Time
	locked   bool
	override bool
}

// RetentionLock sets object retention locks on all backup files.
// Schema files are locked first, then sstables and scylla manifests,
// finalizing with SM manifests.
func (w *worker) RetentionLock(egCtx context.Context, hosts []hostInfo, target Target) error {
	lockUntil, err := retentionLockUntil(w.SnapshotTag, target.RetentionDays)
	if err != nil {
		return err
	}
	cfg := retentionLockConfig{
		until:    lockUntil,
		locked:   target.RetentionLockMode == RetentionLockLocked,
		override: target.OverrideRetentionLock,
	}

	// Lock schema files separately, as they are per location, not per host
	if !target.SkipSchema {
		if err := w.lockSchemaFiles(egCtx, hosts, cfg); err != nil {
			return errors.Wrap(err, "lock schema files")
		}
	}

	// Lock per-host files in parallel.
	// Since it requires storing manifest contents in memory,
	// we need to limit parallelism to the same level,
	// as when uploading manifests.
	eg, egCtx := errgroup.WithContext(egCtx)
	eg.SetLimit(MaxManifestInMemory)

	for i := range hosts {
		eg.Go(func() error {
			if err := w.lockHostFiles(egCtx, &hosts[i], cfg); err != nil {
				return errors.Wrapf(err, "lock host %s files", hosts[i].IP)
			}
			return nil
		})
	}

	return eg.Wait()
}

// lockSchemaFiles locks CQL and alternator schema files for all locations.
func (w *worker) lockSchemaFiles(ctx context.Context, hosts []hostInfo, cfg retentionLockConfig) error {
	doneLocations := make(map[string]struct{})
	for i := range hosts {
		if _, ok := doneLocations[hosts[i].Location.StringWithoutDC()]; ok {
			continue
		}
		doneLocations[hosts[i].Location.StringWithoutDC()] = struct{}{}
		w.Logger.Info(ctx, "Locking schema files", "location", hosts[i].Location.StringWithoutDC(), "host", hosts[i].IP)

		cqlPath := backupspec.RemoteSchemaFile(w.ClusterID, w.TaskID, w.SnapshotTag)
		paths := []string{path.Base(cqlPath)}
		if _, ok := getAlternatorHost(hosts); ok {
			paths = append(paths, path.Base(backupspec.AlternatorSchemaPath(w.ClusterID, w.TaskID, w.SnapshotTag)))
		}
		remoteSchemaDir := hosts[i].Location.RemotePath(path.Dir(cqlPath))

		if err := w.lockAndAwait(ctx, hosts[i].IP, remoteSchemaDir, paths, cfg, "", ""); err != nil {
			return errors.Wrapf(err, "await retention lock with node %s", hosts[i].IP)
		}
	}
	return nil
}

// lockHostFiles locks sstables, scylla manifests, and SM manifest for given host.
func (w *worker) lockHostFiles(ctx context.Context, h *hostInfo, cfg retentionLockConfig) (err error) {
	w.Logger.Info(ctx, "Locking host files", "host", h.IP)

	// As we want to make this stage resumable, to discover files that
	// need to be locked, we need to download manifest from backup location.
	manifestPath := backupspec.RemoteManifestFile(w.ClusterID, w.TaskID, w.SnapshotTag, h.DC, h.ID)
	remoteManifestPath := h.Location.RemotePath(manifestPath)

	r, err := w.Client.RcloneOpen(ctx, h.IP, remoteManifestPath)
	if err != nil {
		return errors.Wrap(err, "download manifest")
	}
	defer func() {
		err = stdErr.Join(err, r.Close())
	}()

	var manifest backupspec.ManifestContentWithIndex
	if err := manifest.Read(r); err != nil {
		return errors.Wrap(err, "read manifest")
	}

	// Lock sstables and scylla manifests
	err = manifest.ForEachIndexIterWithError(nil, func(fm backupspec.FilesMeta) error {
		return errors.Wrapf(w.lockTableFiles(ctx, h, fm, cfg), "lock table %s.%s files", fm.Keyspace, fm.Table)
	})
	if err != nil {
		return errors.Wrap(err, "lock index files")
	}

	// Lock SM manifest
	w.Logger.Info(ctx, "Locking manifest file", "host", h.IP)
	if err := w.lockAndAwait(ctx, h.IP, path.Dir(remoteManifestPath), []string{path.Base(remoteManifestPath)}, cfg, "", ""); err != nil {
		return errors.Wrap(err, "lock manifest")
	}

	return nil
}

// lockTableFiles locks sstables and scylla manifests for given table.
func (w *worker) lockTableFiles(ctx context.Context, h *hostInfo, fm backupspec.FilesMeta, cfg retentionLockConfig) error {
	sstDir := backupspec.RemoteSSTableVersionDir(w.ClusterID, h.DC, h.ID, fm.Keyspace, fm.Table, fm.Version)
	remoteSSTDir := h.Location.RemotePath(sstDir)

	// Locking sstables and scylla manifests separately to avoid
	// potential reallocation of huge sstable files slice.
	if len(fm.Files) > 0 {
		w.Logger.Info(ctx, "Locking sstables",
			"host", h.IP,
			"keyspace", fm.Keyspace,
			"table", fm.Table,
			"count", len(fm.Files),
		)
		if err := w.batchLock(ctx, h.IP, remoteSSTDir, fm.Files, cfg, fm.Keyspace, fm.Table); err != nil {
			return errors.Wrap(err, "batch lock sstables")
		}
	}

	if len(fm.ScyllaManifests) > 0 {
		w.Logger.Info(ctx, "Locking scylla manifests",
			"host", h.IP,
			"keyspace", fm.Keyspace,
			"table", fm.Table,
			"count", len(fm.ScyllaManifests),
		)
		if err := w.batchLock(ctx, h.IP, remoteSSTDir, fm.ScyllaManifests, cfg, fm.Keyspace, fm.Table); err != nil {
			return errors.Wrap(err, "batch lock scylla manifests")
		}
	}

	return nil
}

func (w *worker) batchLock(ctx context.Context, host, remoteDir string,
	paths []string, cfg retentionLockConfig, keyspace, table string,
) error {
	for len(paths) > 0 {
		size := min(len(paths), scyllaclient.MaxSStableComponentCountInReqBody)
		batch := paths[:size]
		paths = paths[size:]

		if err := w.lockAndAwait(ctx, host, remoteDir, batch, cfg, keyspace, table); err != nil {
			return errors.Wrap(err, "lock and await")
		}
	}
	return nil
}

func (w *worker) lockAndAwait(ctx context.Context, host, remoteDir string,
	paths []string, cfg retentionLockConfig, keyspace, table string,
) error {
	jobID, err := w.Client.RcloneRetentionLock(ctx, host, remoteDir, paths, cfg.locked, cfg.until, cfg.override)
	if err != nil {
		return errors.Wrap(err, "schedule retention lock job")
	}
	return w.waitRetentionLockJob(ctx, host, jobID, keyspace, table)
}

// waitRetentionLockJob polls the async retention lock job until completion,
// updating the retention_locked_files metric on each poll.
func (w *worker) waitRetentionLockJob(ctx context.Context, host string, jobID int64, keyspace, table string) (err error) {
	defer func() {
		if err != nil {
			w.Logger.Info(ctx, "Stop job", "host", host, "id", jobID)
			if stopErr := w.Client.RcloneJobStop(context.Background(), host, jobID); stopErr != nil {
				w.Logger.Error(ctx, "Failed to stop retention lock job",
					"host", host,
					"id", jobID,
					"error", stopErr,
				)
			}
		}

		if cleanupErr := w.clearJobStats(context.Background(), jobID, host); cleanupErr != nil {
			w.Logger.Error(ctx, "Failed to clear retention lock job stats",
				"host", host,
				"id", jobID,
				"error", cleanupErr,
			)
		}
	}()

	var done int64
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		job, err := w.Client.RcloneJobProgress(ctx, host, jobID, w.Config.LongPollingTimeoutSeconds)
		if err != nil {
			return errors.Wrap(err, "fetch retention lock job progress")
		}

		switch scyllaclient.RcloneJobStatus(job.Status) {
		case scyllaclient.JobError:
			return errors.Errorf("retention lock job error (%d): %s", jobID, job.Error)
		case scyllaclient.JobNotFound:
			return errors.Errorf("retention lock job not found (%d)", jobID)
		case scyllaclient.JobSuccess, scyllaclient.JobRunning:
			diff := job.Uploaded - done
			done = job.Uploaded
			w.Metrics.IncreaseRetentionLockedFiles(w.ClusterID, keyspace, table, host, diff)
			if scyllaclient.RcloneJobStatus(job.Status) == scyllaclient.JobSuccess {
				return nil
			}
		}
	}
}

func retentionLockUntil(snapshotTag string, retentionDays int) (time.Time, error) {
	snapshotTime, err := backupspec.SnapshotTagTime(snapshotTag)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "parse snapshot tag time")
	}
	return snapshotTime.Add(24 * time.Hour * time.Duration(retentionDays)), nil
}
