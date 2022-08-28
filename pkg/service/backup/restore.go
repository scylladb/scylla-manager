package backup

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// TODO docstrings

type RestoreTarget struct {
	Location         []Location `json:"location"`
	SnapshotTag      string     `json:"snapshot_tag"`
	Keyspace         []string   `json:"keyspace"`
	BatchSize        int        `json:"batch_size"`
	MinFreeDiskSpace int        `json:"min_free_disk_space"`
	Continue         bool       `json:"continue"`
	Parallel         int        `json:"parallel"`
}

type RestoreRunner struct {
	service *Service
}

func (r RestoreRunner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	t, err := r.service.GetRestoreTarget(ctx, clusterID, properties)
	if err != nil {
		return errors.Wrap(err, "get restore target")
	}
	return r.service.Restore(ctx, clusterID, taskID, runID, t)
}

func (s *Service) RestoreRunner() RestoreRunner {
	return RestoreRunner{service: s}
}

func (s *Service) GetRestoreTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (RestoreTarget, error) {
	s.logger.Info(ctx, "GetRestoreTarget", "cluster_id", clusterID)

	var t RestoreTarget

	if err := json.Unmarshal(properties, &t); err != nil {
		return t, err
	}

	if t.Location == nil {
		return t, errors.New("missing location")
	}

	// Set default values
	if t.BatchSize == 0 {
		t.BatchSize = 2
	}
	if t.MinFreeDiskSpace == 0 {
		t.MinFreeDiskSpace = 10
	}
	if t.Parallel == 0 {
		t.Parallel = 1
	}

	return t, nil
}

func (s *Service) forEachRestoredManifest(clusterID uuid.UUID, snapshotTag string) func(context.Context, Location, func(ManifestInfoWithContent) error) error {
	return func(ctx context.Context, location Location, f func(content ManifestInfoWithContent) error) error {
		return s.forEachManifest(ctx, clusterID, []Location{location}, ListFilter{SnapshotTag: snapshotTag}, f)
	}
}

func (s *Service) Restore(ctx context.Context, clusterID, taskID, runID uuid.UUID, target RestoreTarget) error {
	s.logger.Info(ctx, "Restore",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
		"target", target,
	)

	run := &RestoreRun{
		ClusterID:   clusterID,
		TaskID:      taskID,
		ID:          runID,
		SnapshotTag: target.SnapshotTag,
		Stage:       StageRestoreInit,
	}

	// Get cluster name
	clusterName, err := s.clusterName(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "invalid cluster")
	}

	// Get the cluster client
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "initialize: get client proxy")
	}

	// Get cluster session
	clusterSession, err := s.clusterSession(ctx, clusterID)
	if err != nil {
		s.logger.Info(ctx, "No CQL cluster session, restore can't proceed", "error", err)
		return err
	}
	defer clusterSession.Close()

	w := &restoreWorker{
		worker: worker{
			ClusterID:   clusterID,
			ClusterName: clusterName,
			TaskID:      taskID,
			RunID:       runID,
			Client:      client,
			Config:      s.config,
			Metrics:     s.metrics,
			Logger:      s.logger,
		},
		managerSession:          s.session,
		clusterSession:          clusterSession,
		forEachRestoredManifest: s.forEachRestoredManifest(clusterID, target.SnapshotTag),
	}

	if target.Continue {
		if err := w.decorateWithPrevRun(ctx, run); err != nil {
			return err
		}
		w.InsertRun(ctx, run)
		// Update run with previous progress.
		if run.PrevID != uuid.Nil {
			w.clonePrevProgress(run)
		}

		w.Logger.Info(ctx, "After decoration", "run", *run)

	} else {
		w.InsertRun(ctx, run)
	}

	if run.Stage.Index() <= StageRestoreSchema.Index() {
		run.Stage = StageRestoreSchema
		w.InsertRun(ctx, run)

		if err := w.RestoreSchema(ctx, target); err != nil {
			return errors.Wrap(err, "restore schema")
		}
	}

	if run.Stage.Index() <= StageCalcRestoreSize.Index() {
		run.Stage = StageCalcRestoreSize
		w.InsertRun(ctx, run)

		if err := w.RecordSize(ctx, run, target); err != nil {
			return errors.Wrap(err, "record restore size")
		}
	}

	if run.Stage.Index() <= StageRestoreFiles.Index() {
		run.Stage = StageRestoreFiles
		w.InsertRun(ctx, run)

		if err := w.restoreFiles(ctx, run, target, s.config.LocalDC); err != nil {
			return errors.Wrap(err, "restore files")
		}
	}

	run.Stage = StageRestoreDone
	w.InsertRun(ctx, run)

	return nil
}
