// Copyright (C) 2024 ScyllaDB

package restore

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/maps"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/slices"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// hostNativeRestoreSupport validates that native restore API can be used for given host.
// Scylla version check is not performed for explicit --method=native.
func hostNativeRestoreSupport(ni *scyllaclient.NodeInfo, loc []Location, method Method) error {
	if method != MethodNative {
		ok, err := ni.SupportsNativeRestoreAPI()
		if err != nil {
			return errors.Wrap(err, "check native restore api support")
		}
		if !ok {
			return errors.New("native restore api is not supported for this scylla version")
		}
	}
	// Simplification - native restore is supported by host if host supports
	// it for all providers from the backup. All sane scenarios have a single
	// provider backup. Others can still use rclone method.
	// This allows us not to check provider support per batch.
	locProviders := maps.SetFromSlice(slices.Map(loc, func(l Location) Provider {
		return l.Provider
	}))
	if len(locProviders) == 0 {
		return errors.New("no providers in locations") // Should never happen
	}
	for p := range locProviders {
		_, err := ni.ScyllaObjectStorageEndpoint(p)
		if err != nil {
			return errors.Wrap(err, "check scylla object storage endpoint")
		}
	}
	return nil
}

// hostNativeRestoreSupport is the regular hostNativeRestoreSupport with logging on error.
func (w *worker) hostNativeRestoreSupport(ctx context.Context, host string, ni *scyllaclient.NodeInfo, loc []Location) error {
	err := hostNativeRestoreSupport(ni, loc, w.target.Method)
	if err != nil {
		w.logger.Info(ctx, "Can't use native restore api", "host", host, "error", err)
	}
	return err
}

// batchNativeRestoreSupport is the regular batch.NativeRestoreSupport with logging on error.
func (w *worker) batchNativeRestoreSupport(ctx context.Context, host string, b batch) error {
	err := b.NativeRestoreSupport()
	if err != nil {
		w.logger.Info(ctx, "Can't use native restore api",
			"host", host,
			"keyspace", b.Keyspace,
			"table", b.Table,
			"error", err)
	}
	return err
}

func (w *tablesWorker) nativeBatchRestore(ctx context.Context, host string, nc configcache.NodeConfig, b batch) (err error) {
	w.logger.Info(ctx, "Use native restore API", "host", host, "keyspace", b.Keyspace, "table", b.Table)
	w.metrics.SetRestoreState(w.run.ClusterID, b.Location, w.run.SnapshotTag, host, metrics.RestoreStateNativeRestore)
	defer func() {
		if err != nil && scheduler.IsTaskInterrupted(ctx) {
			w.metrics.SetRestoreState(w.run.ClusterID, b.Location, w.run.SnapshotTag, host, metrics.RestoreStateError)
		} else {
			w.metrics.SetRestoreState(w.run.ClusterID, b.Location, w.run.SnapshotTag, host, metrics.RestoreStateIdle)
		}
	}()

	// RemoteSSTableDir has "<provider>:<bucket>/<path>" format
	prefix, ok := strings.CutPrefix(b.RemoteSSTableDir, b.Location.StringWithoutDC()+"/")
	if !ok {
		return errors.Errorf("remote sstable dir (%s) should contain location path prefix (%s)", b.RemoteSSTableDir, b.Location.Path)
	}
	endpoint, err := nc.ScyllaObjectStorageEndpoint(b.Location.Provider)
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
