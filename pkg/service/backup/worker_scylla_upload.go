// Copyright (C) 2024 ScyllaDB

package backup

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// hostNativeBackupSupport validates that native backup API can be used for given host.
// Scylla version check is not performed for explicit --method=native.
func hostNativeBackupSupport(ni *scyllaclient.NodeInfo, loc backupspec.Location, method Method) error {
	if method != MethodNative {
		ok, err := ni.SupportsNativeBackupAPI()
		if err != nil {
			return errors.Wrap(err, "check native backup api support")
		}
		if !ok {
			return errors.New("native backup api is not supported")
		}
	}
	_, err := ni.ScyllaObjectStorageEndpoint(loc.Provider)
	if err != nil {
		return errors.Wrap(err, "check scylla object storage endpoint")
	}
	return nil
}

// hostNativeBackupSupport is the regular hostNativeBackupSupport with logging on error.
func (w *worker) hostNativeBackupSupport(ctx context.Context, host string, ni *scyllaclient.NodeInfo, loc backupspec.Location) error {
	err := hostNativeBackupSupport(ni, loc, w.Method)
	if err != nil {
		w.Logger.Info(ctx, "Can't use native backup api", "host", host, "error", err)
	}
	return err
}

// snapshotDirNativeBackupSupport validates that native backup API can be used for given snapshot dir.
func snapshotDirNativeBackupSupport(d snapshotDir) error {
	if d.willCreateVersioned {
		return errors.New("native backup api does not support creation of versioned files")
	}
	return nil
}

// snapshotDirNativeBackupSupport is the regular snapshotDirNativeBackupSupport with logging on error.
func (w *worker) snapshotDirNativeBackupSupport(ctx context.Context, host string, d snapshotDir) error {
	err := snapshotDirNativeBackupSupport(d)
	if err != nil {
		w.Logger.Info(ctx, "Can't use native backup api",
			"host", host,
			"keyspace", d.Keyspace,
			"table", d.Table,
			"error", err)
	}
	return err
}

func (w *worker) nativeBackup(ctx context.Context, hi hostInfo, d snapshotDir) error {
	w.Logger.Info(ctx, "Use native backup api",
		"host", d.Host,
		"keyspace", d.Keyspace,
		"table", d.Table)
	if d.Progress.ScyllaTaskID == "" || !w.scyllaCanAttachToTask(ctx, hi.IP, d.Progress.ScyllaTaskID) {
		prefix := w.remoteSSTableDir(hi, d)
		endpoint, err := hi.NodeConfig.ScyllaObjectStorageEndpoint(hi.Location.Provider)
		if err != nil {
			return errors.Wrap(err, "get Scylla object storage endpoint")
		}

		id, err := w.Client.ScyllaBackup(ctx, hi.IP, endpoint, hi.Location.Path, prefix, d.Keyspace, d.Table, w.SnapshotTag)
		if err != nil {
			return errors.Wrap(err, "backup")
		}

		w.Logger.Info(ctx, "Backing up dir", "host", d.Host, "keyspace", d.Keyspace, "table", d.Table, "prefix", prefix, "task id", id)
		d.Progress.ScyllaTaskID = id
		w.onRunProgress(ctx, d.Progress)
	}

	if err := w.scyllaWaitTask(ctx, d.Progress.ScyllaTaskID, d); err != nil {
		w.Logger.Error(ctx, "Backing up dir failed", "host", d.Host, "task id", d.Progress.TaskID, "error", err)
		return err
	}
	return nil
}

func (w *worker) scyllaCanAttachToTask(ctx context.Context, host, taskID string) bool {
	task, err := w.Client.ScyllaTaskProgress(ctx, host, taskID)
	if err != nil {
		w.Logger.Error(ctx, "Failed to fetch task info",
			"host", host,
			"task id", taskID,
			"error", err,
		)
		return false
	}

	state := scyllaclient.ScyllaTaskState(task.State)
	return state == scyllaclient.ScyllaTaskStateDone ||
		state == scyllaclient.ScyllaTaskStateRunning ||
		state == scyllaclient.ScyllaTaskStateCreated
}

func (w *worker) scyllaWaitTask(ctx context.Context, id string, d snapshotDir) (err error) {
	defer func() {
		// On error abort task
		if err != nil {
			w.Logger.Info(ctx, "Stop task", "host", d.Host, "id", id)
			// Watch out for already cancelled context
			if e := w.Client.ScyllaAbortTask(context.Background(), d.Host, id); e != nil {
				w.Logger.Error(ctx, "Failed to abort task",
					"host", d.Host,
					"id", id,
					"error", e,
				)
			}
		}
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		task, err := w.Client.ScyllaWaitTask(ctx, d.Host, id, int64(w.Config.LongPollingTimeoutSeconds))
		if err != nil {
			return errors.Wrap(err, "wait for scylla task")
		}
		w.scyllaUpdateProgress(ctx, d, task)
		switch scyllaclient.ScyllaTaskState(task.State) {
		case scyllaclient.ScyllaTaskStateFailed:
			return errors.Errorf("task error (%s): %s", id, task.Error)
		case scyllaclient.ScyllaTaskStateDone:
			return nil
		}
	}
}

func (w *worker) scyllaUpdateProgress(ctx context.Context, d snapshotDir, task *models.TaskStatus) {
	p := d.Progress
	p.StartedAt = nil
	if t := time.Time(task.StartTime); !t.IsZero() {
		p.StartedAt = &t
	}
	p.CompletedAt = nil
	if t := time.Time(task.EndTime); !t.IsZero() {
		p.CompletedAt = &t
	}
	p.Error = task.Error
	p.Uploaded = int64(task.ProgressCompleted)
	p.Skipped = d.SkippedBytesOffset
	w.onRunProgress(ctx, p)
}
