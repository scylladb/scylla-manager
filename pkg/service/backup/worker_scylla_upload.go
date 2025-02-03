// Copyright (C) 2024 ScyllaDB

package backup

import (
	"context"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// supportsScyllaBackupAPI checks if native backup API for given provider
// is supported by node's Scylla version.
func (w *worker) supportsScyllaBackupAPI(ctx context.Context, host string, provider Provider) (bool, error) {
	// List of object storage providers supported by Scylla backup API.
	scyllaSupportedProviders := []Provider{
		S3,
	}
	if !slices.Contains(scyllaSupportedProviders, provider) {
		w.Logger.Info(ctx, "Can't use Scylla backup API",
			"reason", "unsupported cloud provider")
		return false, nil
	}

	nc, err := w.nodeInfo(ctx, host)
	if err != nil {
		return false, errors.Wrapf(err, "get node %s info", host)
	}

	ok, err := nc.SupportsScyllaBackupRestoreAPI()
	if err != nil {
		return false, err
	}
	if !ok {
		w.Logger.Info(ctx, "Can't use Scylla backup API",
			"reason", "no native Scylla backup API exposed")
	}
	return ok, nil
}

// useScyllaBackupAPI checks if we should use native backup API
// for uploading snapshot-ed dir.
// It assumes that supportsScyllaBackupAPI was already checked and passed.
func (w *worker) useScyllaBackupAPI(ctx context.Context, d snapshotDir) bool {
	if d.willCreateVersioned {
		w.Logger.Info(ctx, "Can't use Scylla backup API",
			"keyspace", d.Keyspace,
			"table", d.Table,
			"reason", "backup needs to create versioned files")
		return false
	}
	return true
}

func (w *worker) scyllaBackup(ctx context.Context, hi hostInfo, d snapshotDir) error {
	if d.Progress.ScyllaTaskID == "" || !w.scyllaCanAttachToTask(ctx, hi.IP, d.Progress.ScyllaTaskID) {
		prefix := w.remoteSSTableDir(hi, d)
		// Agent's proxy can resolve provider into the endpoint.
		id, err := w.Client.ScyllaBackup(ctx, hi.IP, string(hi.Location.Provider), hi.Location.Path, prefix, d.Keyspace, d.Table, w.SnapshotTag)
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
