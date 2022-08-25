package backup

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type restoreWorker struct {
	worker

	managerSession          gocqlx.Session
	clusterSession          gocqlx.Session
	forEachRestoredManifest func(ctx context.Context, location Location, f func(ManifestInfoWithContent) error) error
}

func (w *restoreWorker) InsertRun(ctx context.Context, run *RestoreRun) {
	if err := table.RestoreRun.InsertQuery(w.managerSession).BindStruct(run).ExecRelease(); err != nil {
		w.Logger.Error(ctx, "Insert run",
			"run", run,
			"error", err,
		)
	}
}

func (w *restoreWorker) InsertRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if err := table.RestoreRunProgress.InsertQuery(w.managerSession).BindStruct(pr).ExecRelease(); err != nil {
		w.Logger.Error(ctx, "Insert run progress",
			"progress", pr,
			"error", err,
		)
	}
}

func (w *restoreWorker) DeleteRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if err := table.RestoreRunProgress.DeleteQuery(w.managerSession).BindStruct(pr).ExecRelease(); err != nil {
		w.Logger.Error(ctx, "Delete run progress",
			"progress", pr,
			"error", err,
		)
	}
}

// decorateWithPrevRun gets restore task previous run and if it can be continued
// sets PrevID on the given run.
// TODO: do we have to validate the time of previous run?
func (w *restoreWorker) decorateWithPrevRun(ctx context.Context, run *RestoreRun) error {
	prev, err := w.GetLastResumableRun(ctx, run.ClusterID, run.TaskID)
	if errors.Is(err, service.ErrNotFound) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "get previous restore run")
	}

	w.Logger.Info(ctx, "Resuming previous run", "prev_run_id", prev.ID)

	run.PrevID = prev.ID
	run.ManifestPath = prev.ManifestPath
	run.KeyspaceName = prev.KeyspaceName
	run.TableName = prev.TableName
	run.Stage = prev.Stage

	return nil
}

func (w *restoreWorker) clonePrevProgress(run *RestoreRun) {
	q := table.RestoreRunProgress.InsertQuery(w.managerSession)
	defer q.Release()

	prevRun := &RestoreRun{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        run.PrevID,
	}

	w.ForEachProgress(prevRun, func(pr *RestoreRunProgress) {
		pr.RunID = run.ID
		_ = q.BindStruct(pr).Exec()
	})
}

// GetLastResumableRun returns the most recent started but not done run of
// the restore task, if there is a recent run that is completely done ErrNotFound is reported.
func (w *restoreWorker) GetLastResumableRun(ctx context.Context, clusterID, taskID uuid.UUID) (*RestoreRun, error) {
	w.Logger.Debug(ctx, "GetLastResumableRun",
		"cluster_id", clusterID,
		"task_id", taskID,
	)

	q := qb.Select(table.RestoreRun.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
	).Limit(1).Query(w.managerSession).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
	})

	var runs []*RestoreRun
	if err := q.SelectRelease(&runs); err != nil {
		return nil, err
	}

	if len(runs) == 0 || runs[0].Stage == StageRestoreDone {
		return nil, service.ErrNotFound
	}

	return runs[0], nil
}

// GetRun returns a run based on ID.
// If nothing was found scylla-manager.ErrNotFound is returned.
func (w *restoreWorker) GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*RestoreRun, error) {
	w.Logger.Error(ctx, "GetRun",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	q := table.RestoreRun.GetQuery(w.managerSession).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
		"id":         runID,
	})

	var r RestoreRun
	return &r, q.GetRelease(&r)
}

// GetProgress aggregates progress for the restore run of the task
// and breaks it down by keyspace and table.json.
// If nothing was found scylla-manager.ErrNotFound is returned.
func (w *restoreWorker) GetProgress(ctx context.Context) (RestoreProgress, error) {
	w.Logger.Debug(ctx, "GetProgress",
		"cluster_id", w.ClusterID,
		"task_id", w.TaskID,
		"run_id", w.RunID,
	)

	run, err := w.GetRun(ctx, w.ClusterID, w.TaskID, w.RunID)
	if err != nil {
		return RestoreProgress{}, err
	}

	return w.aggregateProgress(run), nil
}
