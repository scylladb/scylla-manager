// Copyright (C) 2024 ScyllaDB

package restore

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// hostScyllaRestoreSupport checks if native restore API is supported for given host.
func (w *tablesWorker) hostScyllaRestoreSupport(ctx context.Context, host string, nc configcache.NodeConfig) bool {
	ok, err := nc.SupportsScyllaBackupRestoreAPI()
	if err != nil || !ok {
		w.logger.Info(ctx, "Can't use Scylla restore API with given Scylla version",
			"host", host,
			"version", nc.ScyllaVersion,
			"error", err)
		return false
	}
	return true
}

// batchScyllaRestoreSupport checks if we should use native restore API
// for restoring given batch.
func (w *tablesWorker) batchScyllaRestoreSupport(ctx context.Context, b batch, nc configcache.NodeConfig) bool {
	scyllaSupportedProviders := []Provider{
		S3,
	}
	if !slices.Contains(scyllaSupportedProviders, b.Location.Provider) {
		// As this check is done per batch, we shouldn't flood the logs
		// with semi-interesting information.
		return false
	}

	_, err := nc.ScyllaBackupRestoreEndpoint(b.Location.Provider)
	if err != nil {
		w.logger.Info(ctx, "Can't use Scylla restore API without object storage endpoint configuration",
			"error", err)
		return false
	}

	if b.batchType.Versioned {
		w.logger.Info(ctx, "Can't use Scylla restore API with versioned sstables",
			"backup node ID", b.NodeID,
			"keyspace", b.Keyspace,
			"table", b.Table)
		return false
	}

	if b.batchType.IDType == sstable.IntegerID {
		w.logger.Info(ctx, "Can't use Scylla restore API with integer based sstable ID",
			"backup node ID", b.NodeID,
			"keyspace", b.Keyspace,
			"table", b.Table)
		return false
	}
	return true
}

// tryScyllaRestore returns true and runs scyllaRestore if hostScyllaRestoreSupport and batchScyllaRestoreSupport
// checks have passed. Otherwise, it returns false, nil.
// Note that hostScyllaRestoreSupport needs to be evaluated before running this method.
func (w *tablesWorker) tryScyllaRestore(ctx context.Context, hostScyllaRestoreSupport bool, host string, b batch, nc configcache.NodeConfig) (bool, error) {
	if !hostScyllaRestoreSupport {
		return false, nil
	}
	if !w.batchScyllaRestoreSupport(ctx, b, nc) {
		return false, nil
	}
	w.logger.Info(ctx, "Use Scylla restore API",
		"host", host,
		"keyspace", b.Keyspace,
		"table", b.Table)
	return true, w.scyllaRestore(ctx, host, b, nc)
}

func (w *tablesWorker) scyllaRestore(ctx context.Context, host string, b batch, nc configcache.NodeConfig) (err error) {
	w.logger.Info(ctx, "Use native Scylla restore API", "host", host, "keyspace", b.Keyspace, "table", b.Table)
	w.metrics.SetRestoreState(w.run.ClusterID, b.Location, w.run.SnapshotTag, host, metrics.RestoreStateNativeRestore)
	defer func() {
		if err != nil && scheduler.IsTaskInterrupted(ctx) {
			w.metrics.SetRestoreState(w.run.ClusterID, b.Location, w.run.SnapshotTag, host, metrics.RestoreStateError)
		} else {
			w.metrics.SetRestoreState(w.run.ClusterID, b.Location, w.run.SnapshotTag, host, metrics.RestoreStateIdle)
		}
	}()

	// RemoteSSTableDir has "<provider>:<bucket>/<path>" format
	prefix, ok := strings.CutPrefix(b.RemoteSSTableDir, b.Location.StringWithoutDC())
	if !ok {
		return errors.Errorf("remote sstable dir (%s) should contain location path prefix (%s)", b.RemoteSSTableDir, b.Location.Path)
	}
	endpoint, err := nc.ScyllaBackupRestoreEndpoint(b.Location.Provider)
	if err != nil {
		return errors.Wrap(err, "get Scylla object storage endpoint")
	}

	id, err := w.client.ScyllaRestore(ctx, host, endpoint, b.Location.Path, prefix, b.Keyspace, b.Table, b.TOC())
	if err != nil {
		return errors.Wrap(err, "restore")
	}

	pr := &RunProgress{
		ClusterID:        w.run.ClusterID,
		TaskID:           w.run.TaskID,
		RunID:            w.run.ID,
		RemoteSSTableDir: b.RemoteSSTableDir,
		Keyspace:         b.Keyspace,
		Table:            b.Table,
		Host:             host,
		ShardCnt:         int64(w.hostShardCnt[host]),
		ScyllaTaskID:     id,
		SSTableID:        b.IDs(),
	}
	w.insertRunProgress(ctx, pr)

	w.logger.Info(ctx, "Wait for restore task to finish", "host", host, "task id", id)
	err = w.scyllaWaitTask(ctx, pr, b)
	if err != nil {
		w.cleanupRunProgress(context.Background(), pr)
	}
	return err
}

func (w *tablesWorker) scyllaWaitTask(ctx context.Context, pr *RunProgress, b batch) (err error) {
	for {
		if ctx.Err() != nil {
			w.scyllaAbortTask(pr.Host, pr.ScyllaTaskID)
			return ctx.Err()
		}

		task, err := w.client.ScyllaWaitTask(ctx, pr.Host, pr.ScyllaTaskID, int64(w.config.LongPollingTimeoutSeconds))
		if err != nil {
			w.scyllaAbortTask(pr.Host, pr.ScyllaTaskID)
			return errors.Wrap(err, "wait for task")
		}

		w.scyllaUpdateProgress(ctx, pr, b, task)
		switch scyllaclient.ScyllaTaskState(task.State) {
		case scyllaclient.ScyllaTaskStateFailed:
			return errors.Errorf("task error (%s): %s", pr.ScyllaTaskID, task.Error)
		case scyllaclient.ScyllaTaskStateDone:
			return nil
		}
	}
}

func (w *tablesWorker) scyllaAbortTask(host, id string) {
	if err := w.client.ScyllaAbortTask(context.Background(), host, id); err != nil {
		w.logger.Error(context.Background(), "Failed to abort task",
			"host", host,
			"id", id,
			"error", err,
		)
	}
}

func (w *tablesWorker) scyllaUpdateProgress(ctx context.Context, pr *RunProgress, b batch, task *models.TaskStatus) {
	now := timeutc.Now()
	restored := b.Size * int64(task.ProgressCompleted/task.ProgressTotal)
	restoredDiff := restored - pr.Restored
	var startedAt, completedAt *time.Time
	if t := time.Time(task.StartTime); !t.IsZero() {
		startedAt = &t
	}
	if t := time.Time(task.EndTime); !t.IsZero() {
		completedAt = &t
	}

	// Update metrics boilerplate
	w.metrics.IncreaseRestoredBytes(w.run.ClusterID, pr.Host, restoredDiff)
	w.metrics.IncreaseRestoreDuration(w.run.ClusterID, pr.Host, timeSub(startedAt, completedAt, now))
	w.metrics.DecreaseRemainingBytes(metrics.RestoreBytesLabels{
		ClusterID:   b.ClusterID.String(),
		SnapshotTag: b.SnapshotTag,
		Location:    b.Location.String(),
		DC:          b.DC,
		Node:        b.NodeID,
		Keyspace:    b.Keyspace,
		Table:       b.Table,
	}, restoredDiff)
	w.progress.Update(restoredDiff)
	w.metrics.SetProgress(metrics.RestoreProgressLabels{
		ClusterID:   w.run.ClusterID.String(),
		SnapshotTag: w.run.SnapshotTag,
	}, w.progress.CurrentProgress())

	// Update run progress
	pr.RestoreStartedAt = startedAt
	pr.RestoreCompletedAt = completedAt
	pr.Error = task.Error
	pr.Restored = restored
	w.insertRunProgress(ctx, pr)
}
