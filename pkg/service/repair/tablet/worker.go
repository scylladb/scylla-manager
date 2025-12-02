// Copyright (C) 2025 ScyllaDB

package tablet

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type worker struct {
	clusterID uuid.UUID
	taskID    uuid.UUID
	runID     uuid.UUID

	logger    log.Logger
	metrics   metrics.TabletRepairMetrics
	smSession gocqlx.Session
	client    *scyllaclient.Client

	incrementalRepairSupport bool
}

func (s *Service) newWorker(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*worker, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "get scylla client")
	}
	ni, err := client.AnyNodeInfo(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get any node info")
	}
	incrementalSupport, err := ni.SupportsIncrementalRepair()
	if err != nil {
		return nil, errors.Wrap(err, "check incremental tablet repair support")
	}
	return &worker{
		clusterID:                clusterID,
		taskID:                   taskID,
		runID:                    runID,
		logger:                   s.logger.Named("worker"),
		metrics:                  s.metrics,
		smSession:                s.smSession,
		client:                   client,
		incrementalRepairSupport: incrementalSupport,
	}, nil
}

func (w *worker) repairAll(ctx context.Context, target Target) error {
	w.init(ctx, target)
	// We need to make sure that leftover scylla tablet repair tasks are not running,
	// as scheduling new scylla tablet repair tasks on a table with an ongoing tablet repair
	// ends with an error. On the other hand, this is just best effort, because we can't
	// ensure that new scylla tablet repair tasks won't be created in the meantime or that
	// the task that we were trying to abort has just finished on its own.
	w.abortAllRepairTasks(ctx)

	for ks, tabs := range target.KsTabs {
		for _, tab := range tabs {
			if err := w.repairTable(ctx, w.client, ks, tab); err != nil {
				return errors.Wrapf(err, "%s.%s: run repair", ks, tab)
			}
		}
	}
	return nil
}

func (w *worker) init(ctx context.Context, target Target) {
	w.metrics.ResetClusterMetrics(w.clusterID)
	for ks, tabs := range target.KsTabs {
		for _, tab := range tabs {
			w.logger.Info(ctx, "Plan to repair table", "keyspace", ks, "table", tab)
			pr := newRunProgress(w.clusterID, w.taskID, w.runID, ks, tab)
			w.upsertTableProgress(ctx, pr)
		}
	}
}

func (w *worker) abortAllRepairTasks(ctx context.Context) {
	tasks, err := w.client.ScyllaListTasks(ctx, "", scyllaclient.ScyllaTaskModuleTablets)
	if err != nil {
		w.logger.Error(ctx, "Failed to list scylla tablet tasks", "error", err)
		return
	}
	for _, t := range tasks {
		if t == nil {
			continue
		}
		if scyllaclient.ScyllaTaskType(t.Type) == scyllaclient.ScyllaTaskTypeUserRepair {
			w.abortRepairTask(ctx, t.TaskID)
		}
	}
}

func (w *worker) repairTable(ctx context.Context, client *scyllaclient.Client, ks, tab string) (err error) {
	pr := newRunProgress(w.clusterID, w.taskID, w.runID, ks, tab)

	start := timeutc.Now()
	w.logger.Info(ctx, "Started tablet repair", "keyspace", ks, "table", tab)
	pr.StartedAt = &start
	w.upsertTableProgress(ctx, pr)
	w.metrics.SetTableProgress(w.clusterID, w.taskID, ks, tab, 0)

	defer func(start time.Time) {
		end := timeutc.Now()
		w.logger.Info(ctx, "Ended tablet repair", "keyspace", ks, "table", tab, "duration", end.Sub(start), "error", err)
		pr.CompletedAt = &end
		if err != nil {
			pr.Error = err.Error()
		} else {
			w.metrics.SetTableProgress(w.clusterID, w.clusterID, ks, tab, 100)
		}
		w.upsertTableProgress(ctx, pr)
	}(start)

	id, err := client.TabletRepair(ctx, ks, tab, "", nil, nil, w.incrementalRepairMode())
	if err != nil {
		if _, _, ok := scyllaclient.IsColocatedTableErr(err); ok {
			// Since we always repair all tablet tables,
			// we can always skip colocated tablet repair error.
			w.logger.Info(ctx, "Skipping repair of colocated tablet table", "keyspace", ks, "table", tab, "error", err)
			return nil
		}
		return errors.Wrapf(err, "schedule tablet repair")
	}

	w.logger.Info(ctx, "Scheduled tablet repair", "keyspace", ks, "table", tab, "task ID", id)
	w.upsertTableProgress(ctx, pr)

	status, err := client.ScyllaWaitTask(ctx, "", id, 0)
	if err != nil {
		w.abortRepairTask(context.Background(), id)
		return errors.Wrap(err, "get tablet repair task status")
	}
	switch scyllaclient.ScyllaTaskState(status.State) {
	case scyllaclient.ScyllaTaskStateDone:
		return nil
	case scyllaclient.ScyllaTaskStateFailed:
		return errors.Errorf("tablet repair task finished with status %q", scyllaclient.ScyllaTaskStateFailed)
	default:
		return errors.Errorf("unexpected tablet repair task status %q", status.State)
	}
}

func (w *worker) incrementalRepairMode() scyllaclient.IncrementalMode {
	if w.incrementalRepairSupport {
		return scyllaclient.IncrementalModeIncremental
	}
	return ""
}

func (w *worker) abortRepairTask(ctx context.Context, id string) {
	if err := w.client.ScyllaAbortTask(ctx, "", id); err != nil {
		w.logger.Error(ctx, "Failed to abort scylla repair task",
			"id", id,
			"error", err,
		)
	} else {
		w.logger.Info(ctx, "Aborted scylla tablet repair task", "id", id)
	}
}

func (w *worker) upsertTableProgress(ctx context.Context, pr RunProgress) {
	q := table.TabletRepairRunProgress.InsertQuery(w.smSession)
	defer q.Release()
	if err := q.BindStruct(pr).Exec(); err != nil {
		w.logger.Error(ctx, "Failed to upsert table progress", "error", err)
	}
}
