// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"golang.org/x/sync/errgroup"
)

const (
	downloadWorkerName = "download"
	refreshWorkerName  = "refresh"
)

func (w *worker) restoreTables(ctx context.Context, workload []hostWorkload, keyspaces []string) error {
	logError := func(i int, err error) {
		w.logger.Error(ctx, "Restore data", "err", err, "host", workload[i].host)
	}
	stats := newRestoreStats(workload)
	defer func() {
		w.logger.Info(ctx, "Restore stats", "bandwidth_per_shard", stats.averageBandwidthPerShard())
	}()
	return parallel.Run(len(workload), len(workload), func(i int) error {
		hostTask := workload[i]
		manifestInfo, host := hostTask.manifestInfo, hostTask.host
		var (
			refreshQueueSize = min(100, len(hostTask.manifestContent.Index))
			refreshQueue     = make(chan refreshNodeInput, refreshQueueSize)
		)
		workerNames := []string{downloadWorkerName, refreshWorkerName}
		w.setRestoreStateForWorkers(manifestInfo, host, workerNames, metrics.One2OneRestoreStateIdle)
		downloadAndRefreshGroup, gCtx := errgroup.WithContext(ctx)
		downloadAndRefreshGroup.Go(w.downloadTablesWorker(gCtx, hostTask, keyspaces, stats, refreshQueue))
		downloadAndRefreshGroup.Go(w.refreshNodeWorker(gCtx, hostTask, refreshQueue))
		if err := downloadAndRefreshGroup.Wait(); err != nil {
			return err
		}
		return nil
	}, logError)
}

func (w *worker) downloadTablesWorker(ctx context.Context, hostTask hostWorkload, keyspaces []string, stats *restoreStats, refreshQueue chan<- refreshNodeInput) func() error {
	manifestInfo, host := hostTask.manifestInfo, hostTask.host
	return func() (err error) {
		defer func() {
			close(refreshQueue)
			w.finishWorker(manifestInfo, host, downloadWorkerName, err)
		}()
		return hostTask.manifestContent.ForEachIndexIterWithError(keyspaces, func(table backupspec.FilesMeta) error {
			w.logger.Info(ctx, "Restoring data", "ks", table.Keyspace, "table", table.Table, "size", table.Size)

			jobID, err := w.createDownloadJob(ctx, table, manifestInfo, host)
			if err != nil {
				return errors.Wrapf(err, "create download job: %s.%s", table.Keyspace, table.Table)
			}
			pr := w.restoreTableProgress(ctx, hostTask.host.Addr, table)

			const pollIntervalSec = 10
			if err := w.waitJob(ctx, jobID, manifestInfo, host, pr, stats, pollIntervalSec); err != nil {
				return errors.Wrapf(err, "wait job: %s.%s", table.Keyspace, table.Table)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case refreshQueue <- refreshNodeInput{
				Table:    table,
				Progress: pr,
			}:
			}
			return nil
		})
	}
}

func (w *worker) createDownloadJob(ctx context.Context, table backupspec.FilesMeta, m *backupspec.ManifestInfo, h Host) (int64, error) {
	uploadDir := backupspec.UploadTableDir(table.Keyspace, table.Table, table.Version)
	remoteDir := m.LocationSSTableVersionDir(table.Keyspace, table.Table, table.Version)
	transfers := 2 * h.ShardCount // 2 * shard count showed to be the most optimal value in tests
	jobID, err := w.client.RcloneCopyPaths(ctx, h.Addr, transfers, scyllaclient.NoRateLimit, uploadDir, remoteDir, table.Files)
	if err != nil {
		return 0, errors.Wrapf(err, "copy dir: %s", m.LocationSSTableVersionDir(table.Keyspace, table.Table, table.Version))
	}
	w.metrics.SetOne2OneRestoreState(w.runInfo.ClusterID, m.Location, m.SnapshotTag, h.Addr, downloadWorkerName, metrics.One2OneRestoreStateDownloading)
	return jobID, nil
}

type refreshNodeInput struct {
	Table    backupspec.FilesMeta
	Progress *RunTableProgress
}

func (w *worker) refreshNodeWorker(ctx context.Context, hostTask hostWorkload, queue <-chan refreshNodeInput) func() error {
	manifestInfo, host := hostTask.manifestInfo, hostTask.host
	return func() (err error) {
		defer func() {
			w.finishWorker(manifestInfo, host, refreshWorkerName, err)
		}()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case input, ok := <-queue:
				if !ok {
					return nil
				}
				if err := w.refreshNode(ctx, input.Table, manifestInfo, host, input.Progress); err != nil {
					return errors.Wrapf(err, "refresh node: %s.%s", input.Table.Keyspace, input.Table.Table)
				}
			}
		}
	}
}

func (w *worker) refreshNode(ctx context.Context, table backupspec.FilesMeta, m *backupspec.ManifestInfo, h Host, pr *RunTableProgress) error {
	start := timeutc.Now()
	w.metrics.SetOne2OneRestoreState(w.runInfo.ClusterID, m.Location, m.SnapshotTag, h.Addr, refreshWorkerName, metrics.One2OneRestoreStateLoading)
	err := w.client.AwaitLoadSSTables(ctx, h.Addr, table.Keyspace, table.Table, false, false)
	w.finishRestoreTableProgress(ctx, pr, err)
	if err == nil {
		w.logger.Info(ctx, "Refresh node done",
			"host", h.Addr,
			"keyspace", table.Keyspace,
			"table", table.Table,
			"size", table.Size,
			"took", timeutc.Since(start),
		)
		w.metrics.SetOne2OneRestoreState(w.runInfo.ClusterID, m.Location, m.SnapshotTag, h.Addr, refreshWorkerName, metrics.One2OneRestoreStateIdle)
	}
	return err
}

func (w *worker) waitJob(ctx context.Context, jobID int64, m *backupspec.ManifestInfo, h Host, pr *RunTableProgress, stats *restoreStats, pollIntervalSec int) (err error) {
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
		w.updateRestoreTableProgress(ctx, pr, job)
		w.metrics.SetDownloadRemainingBytes(metrics.One2OneRestoreBytesLabels{
			ClusterID:   w.runInfo.ClusterID.String(),
			SnapshotTag: m.SnapshotTag,
			Location:    m.Location.String(),
			DC:          h.DC,
			Node:        h.Addr,
			Keyspace:    pr.Keyspace,
			Table:       pr.Table,
		}, float64(pr.TableSize-job.Uploaded))

		switch scyllaclient.RcloneJobStatus(job.Status) {
		case scyllaclient.JobError:
			return errors.Errorf("job error (%d): %s: host %s", jobID, job.Error, h.Addr)
		case scyllaclient.JobSuccess:
			took := time.Time(job.CompletedAt).Sub(time.Time(job.StartedAt))
			var bandwidth int64
			if ms := took.Milliseconds(); ms > 0 {
				bandwidth = job.Uploaded / ms
			}
			w.logger.Info(ctx, "Job done",
				"job_id", jobID,
				"host", h,
				"bandwidth", bandwidth,
				"took", took,
			)
			stats.incrementDownloadStats(job.Uploaded, took.Milliseconds())
			w.metrics.SetOne2OneRestoreState(w.runInfo.ClusterID, m.Location, m.SnapshotTag, h.Addr, downloadWorkerName, metrics.One2OneRestoreStateIdle)
			return nil
		case scyllaclient.JobRunning:
			continue
		case scyllaclient.JobNotFound:
			return errors.New("job not found")
		default:
			return errors.Errorf("unknown job status (%d): %s: host %s", jobID, job.Status, h.Addr)
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

func (w *worker) setRestoreStateForWorkers(manifestInfo *backupspec.ManifestInfo, host Host, workerNames []string, state metrics.One2OneRestoreState) {
	for _, worker := range workerNames {
		w.metrics.SetOne2OneRestoreState(w.runInfo.ClusterID, manifestInfo.Location, manifestInfo.SnapshotTag, host.Addr, worker, state)
	}
}

func (w *worker) finishWorker(manifestInfo *backupspec.ManifestInfo, host Host, worker string, err error) {
	state := metrics.One2OneRestoreStateDone
	if err != nil {
		state = metrics.One2OneRestoreStateError
	}
	w.metrics.SetOne2OneRestoreState(w.runInfo.ClusterID, manifestInfo.Location, manifestInfo.SnapshotTag, host.Addr, worker, metrics.One2OneRestoreState(state))
}
