// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// restoreHost represents host that can be used for restoring files.
// If set, OngoingRunProgress represents unfinished RestoreRunProgress created in previous run.
type restoreHost struct {
	Host               string
	Shards             uint
	OngoingRunProgress *RestoreRunProgress
}

// bundle represents SSTables with the same ID.
type bundle []string

// restoreWorker is responsible for coordinating restore procedure.
type restoreWorker struct {
	workerTools

	metrics metrics.RestoreM
	// Fields below are constant among all restore runs of the same restore task.
	managerSession gocqlx.Session
	clusterSession gocqlx.Session
	// Iterates over all manifests in given location with
	// cluster ID and snapshot tag specified in restore target.
	forEachRestoredManifest func(ctx context.Context, location Location, f func(ManifestInfoWithContent) error) error

	// Fields below are mutable for each restore run
	location     Location                // Currently restored location
	miwc         ManifestInfoWithContent // Currently restored manifest
	hosts        []restoreHost           // Restore units created for currently restored location
	bundles      map[string]bundle       // Maps bundle to it's ID
	bundleIDPool chan string             // IDs of the bundles that are yet to be restored
	resumed      bool                    // Set to true if current run has already skipped all tables restored in previous run
}

func (w *restoreWorker) insertRun(ctx context.Context, run *RestoreRun) {
	if err := table.RestoreRun.InsertQuery(w.managerSession).BindStruct(run).ExecRelease(); err != nil {
		w.Logger.Error(ctx, "Insert run",
			"run", *run,
			"error", err,
		)
	}
}

func (w *restoreWorker) insertRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if err := table.RestoreRunProgress.InsertQuery(w.managerSession).BindStruct(pr).ExecRelease(); err != nil {
		w.Logger.Error(ctx, "Insert run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

func (w *restoreWorker) deleteRunProgress(ctx context.Context, pr *RestoreRunProgress) {
	if err := table.RestoreRunProgress.DeleteQuery(w.managerSession).BindStruct(pr).ExecRelease(); err != nil {
		w.Logger.Error(ctx, "Delete run progress",
			"progress", *pr,
			"error", err,
		)
	}
}

// decorateWithPrevRun gets restore task previous run and if it is not done
// sets prev ID on the given run.
func (w *restoreWorker) decorateWithPrevRun(ctx context.Context, run *RestoreRun) error {
	prev, err := w.GetRun(ctx, run.ClusterID, run.TaskID, uuid.Nil)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "get run")
	}
	if prev.Stage == StageRestoreDone {
		return nil
	}

	w.Logger.Info(ctx, "Resuming previous run", "prev_run_id", prev.ID)

	run.PrevID = prev.ID
	run.ManifestPath = prev.ManifestPath
	run.Keyspace = prev.Keyspace
	run.Table = prev.Table
	run.Stage = prev.Stage
	run.Units = prev.Units

	return nil
}

// clonePrevProgress copies all the previous run progress into
// current run progress.
func (w *restoreWorker) clonePrevProgress(ctx context.Context, run *RestoreRun) {
	q := table.RestoreRunProgress.InsertQuery(w.managerSession)
	defer q.Release()

	prevRun := &RestoreRun{
		ClusterID: run.ClusterID,
		TaskID:    run.TaskID,
		ID:        run.PrevID,
	}

	w.ForEachProgress(ctx, prevRun, func(pr *RestoreRunProgress) {
		pr.RunID = run.ID
		if err := q.BindStruct(pr).Exec(); err != nil {
			w.Logger.Error(ctx, "Couldn't clone run progress",
				"run_progress", *pr,
				"error", err,
			)
		}
	})
}

// GetRun returns run with specified cluster, task and run ID.
// If run ID is not specified, it returns latest run with specified cluster and task ID.
func (w *restoreWorker) GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*RestoreRun, error) {
	w.Logger.Debug(ctx, "Get run",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	var q *gocqlx.Queryx
	if runID != uuid.Nil {
		q = table.RestoreRun.GetQuery(w.managerSession).BindMap(qb.M{
			"cluster_id": clusterID,
			"task_id":    taskID,
			"id":         runID,
		})
	} else {
		q = table.RestoreRun.SelectQuery(w.managerSession).BindMap(qb.M{
			"cluster_id": clusterID,
			"task_id":    taskID,
		})
	}

	var r RestoreRun
	return &r, q.GetRelease(&r)
}

func (w *restoreWorker) newUnits(ctx context.Context, target RestoreTarget) ([]RestoreUnit, error) {
	var (
		units   []RestoreUnit
		unitMap = make(map[string]RestoreUnit)
	)

	var foundManifest bool
	for _, w.location = range target.Location {
		manifestHandler := func(miwc ManifestInfoWithContent) error {
			foundManifest = true

			filesHandler := func(fm FilesMeta) {
				ru := unitMap[fm.Keyspace]
				ru.Keyspace = fm.Keyspace
				ru.Size += fm.Size

				for i, t := range ru.Tables {
					if t.Table == fm.Table {
						ru.Tables[i].Size += fm.Size
						unitMap[fm.Keyspace] = ru

						return
					}
				}

				ru.Tables = append(ru.Tables, RestoreTable{
					Table: fm.Table,
					Size:  fm.Size,
				})
				unitMap[fm.Keyspace] = ru
			}

			return miwc.ForEachIndexIter(target.Keyspace, filesHandler)
		}

		if err := w.forEachRestoredManifest(ctx, w.location, manifestHandler); err != nil {
			return nil, err
		}
	}

	if !foundManifest {
		return nil, errors.Errorf("no snapshot with given tag: %s", target.SnapshotTag)
	}

	for _, u := range unitMap {
		units = append(units, u)
	}

	if units == nil {
		return nil, errors.New("no data in backup locations match given keyspace pattern")
	}

	for _, u := range units {
		for _, t := range u.Tables {
			if err := w.ValidateTableExists(ctx, u.Keyspace, t.Table); err != nil {
				return nil, err
			}
		}
	}

	w.Logger.Info(ctx, "Created restore units", "units", units)

	return units, nil
}
