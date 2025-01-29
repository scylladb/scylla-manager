// Copyright (C) 2024 ScyllaDB

package restore

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// Decides whether we should use Scylla restore API for restoring the batch.
func (w *worker) useScyllaRestoreAPI(ctx context.Context, b batch, host string) (bool, error) {
	// Scylla restore API does not handle restoration of versioned files
	if b.batchType.Versioned {
		return false, nil
	}
	// Scylla restore API does not handle SSTables with sstable.IntegerID
	if b.batchType.IDType == sstable.IntegerID {
		return false, nil
	}
	// List of object storage providers supported by Scylla restore API
	scyllaSupportedProviders := []Provider{
		S3,
	}
	if !slices.Contains(scyllaSupportedProviders, b.Location.Provider) {
		return false, nil
	}
	// Check if node exposes Scylla restore API
	nc, err := w.nodeInfo(ctx, host)
	if err != nil {
		return false, errors.Wrapf(err, "get node %s info", host)
	}
	return nc.SupportsScyllaBackupRestoreAPI()
}

func (w *tablesWorker) scyllaRestore(ctx context.Context, host string, b batch) error {
	w.logger.Info(ctx, "Use native Scylla restore API", "host", host, "keyspace", b.Keyspace, "table", b.Table)

	// RemoteSSTableDir has "<provider>:<bucket>/<path>" format
	prefix, ok := strings.CutPrefix(b.RemoteSSTableDir, b.Location.StringWithoutDC())
	if !ok {
		return errors.Errorf("remote sstable dir (%s) should contain location path prefix (%s)", b.RemoteSSTableDir, b.Location.Path)
	}
	id, err := w.client.ScyllaRestore(ctx, host, string(b.Location.Provider), b.Location.Path, prefix, b.Keyspace, b.Table, b.TOC())
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
	defer func() {
		// On error abort task
		if err != nil {
			if e := w.client.ScyllaAbortTask(context.Background(), pr.Host, pr.ScyllaTaskID); e != nil {
				w.logger.Error(ctx, "Failed to abort task",
					"host", pr.Host,
					"id", pr.ScyllaTaskID,
					"error", e,
				)
			}
		}
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		task, err := w.client.ScyllaWaitTask(ctx, pr.Host, pr.ScyllaTaskID, int64(w.config.LongPollingTimeoutSeconds))
		if err != nil {
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

func (w *worker) scyllaUpdateProgress(ctx context.Context, pr *RunProgress, b batch, task *models.TaskStatus) {
	if t := time.Time(task.StartTime); !t.IsZero() {
		pr.RestoreStartedAt = &t
	}
	if t := time.Time(task.EndTime); !t.IsZero() {
		pr.RestoreCompletedAt = &t
	}
	pr.Error = task.Error
	pr.Restored = b.Size * int64(task.ProgressCompleted/task.ProgressTotal)
	w.insertRunProgress(ctx, pr)
}
