// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
)

func (w *worker) restoreTables(ctx context.Context, workload []hostWorkload, keyspaces []string) error {
	logError := func(i int, err error) {
		w.logger.Error(ctx, "Restore data", "err", err, "host", workload[i].host)
	}
	return parallel.Run(len(workload), len(workload), func(i int) error {
		hostTask := workload[i]
		const (
			repeatInterval  = 10 * time.Second
			pollIntervalSec = 10
		)
		return hostTask.manifestContent.ForEachIndexIterWithError(keyspaces, func(table backupspec.FilesMeta) error {
			w.logger.Info(ctx, "Restoring data", "ks", table.Keyspace, "table", table.Table, "size", table.Size)

			jobID, err := w.createDownloadJob(ctx, table, hostTask.manifestInfo, hostTask.host)
			if err != nil {
				return errors.Wrapf(err, "create download job: %s.%s", table.Keyspace, table.Table)
			}
			pr := w.downloadProgress(ctx, hostTask.host.Addr, table)

			if err := w.waitJob(ctx, jobID, hostTask.host, pr, pollIntervalSec); err != nil {
				return errors.Wrapf(err, "wait job: %s.%s", table.Keyspace, table.Table)
			}

			if err := w.refreshNode(ctx, table, hostTask.host, pr); err != nil {
				return errors.Wrapf(err, "refresh node: %s.%s", table.Keyspace, table.Table)
			}
			return nil
		})
	}, logError)
}

func (w *worker) createDownloadJob(ctx context.Context, table backupspec.FilesMeta, m *backupspec.ManifestInfo, h Host) (int64, error) {
	uploadDir := backupspec.UploadTableDir(table.Keyspace, table.Table, table.Version)
	remoteDir := m.LocationSSTableVersionDir(table.Keyspace, table.Table, table.Version)
	jobID, err := w.client.RcloneCopyPaths(ctx, h.Addr, scyllaclient.TransfersFromConfig, scyllaclient.NoRateLimit, uploadDir, remoteDir, table.Files)
	if err != nil {
		return 0, errors.Wrapf(err, "copy dir: %s", m.LocationSSTableVersionDir(table.Keyspace, table.Table, table.Version))
	}
	return jobID, nil
}

func (w *worker) refreshNode(ctx context.Context, table backupspec.FilesMeta, h Host, pr *RunTableProgress) error {
	err := w.client.AwaitLoadSSTables(ctx, h.Addr, table.Keyspace, table.Table, false, false)
	w.finishDownloadProgress(ctx, pr, err)
	return err
}

func (w *worker) waitJob(ctx context.Context, jobID int64, h Host, pr *RunTableProgress, pollIntervalSec int) (err error) {
	defer func() {
		cleanCtx := context.Background()
		// On error stop job
		if err != nil {
			w.stopJob(cleanCtx, jobID, h.Addr)
		}
		// On exit clear stats
		w.clearJobStats(cleanCtx, jobID, h.Addr)
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		job, err := w.client.RcloneJobProgress(ctx, h.Addr, jobID, pollIntervalSec)
		if err != nil {
			return errors.Wrap(err, "fetch job info")
		}
		w.updateDownloadProgress(ctx, pr, job)

		switch scyllaclient.RcloneJobStatus(job.Status) {
		case scyllaclient.JobError:
			return errors.Errorf("job error (%d): %s: host %s", jobID, job.Error, h.Addr)
		case scyllaclient.JobSuccess:
			w.logger.Info(ctx, "Job done",
				"job_id", jobID,
				"host", h,
				"took", time.Time(job.CompletedAt).Sub(time.Time(job.StartedAt)),
			)
			return nil
		case scyllaclient.JobRunning:
			continue
		case scyllaclient.JobNotFound:
			return errors.New("job not found")
		}
	}
}

func (w *worker) clearJobStats(ctx context.Context, jobID int64, host string) {
	if err := w.client.RcloneDeleteJobStats(ctx, host, jobID); err != nil {
		w.logger.Error(ctx, "Failed to clear job stats",
			"host", host,
			"id", jobID,
			"error", err,
		)
	}
}

func (w *worker) stopJob(ctx context.Context, jobID int64, host string) {
	if err := w.client.RcloneJobStop(ctx, host, jobID); err != nil {
		w.logger.Error(ctx, "Failed to stop job",
			"host", host,
			"id", jobID,
			"error", err,
		)
	}
}
