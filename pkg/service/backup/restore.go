// Copyright (C) 2022 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// GetRestoreTarget converts runner properties into RestoreTarget.
func (s *Service) GetRestoreTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (RestoreTarget, error) {
	s.logger.Info(ctx, "GetRestoreTarget", "cluster_id", clusterID)

	t := defaultRestoreTarget()

	if err := json.Unmarshal(properties, &t); err != nil {
		return t, err
	}

	if err := t.validateProperties(); err != nil {
		return t, err
	}

	locations := make(map[string]struct{})
	for _, l := range t.Location {
		rp := l.RemotePath("")
		if _, ok := locations[rp]; ok {
			return t, errors.Errorf("location '%s' is specified multiple times", l)
		}
		locations[rp] = struct{}{}

		if l.DC == "" {
			s.logger.Info(ctx, "No datacenter specified for location - using all nodes for this location", "location", l)
		}
	}

	if t.RestoreSchema {
		t.Keyspace = []string{"system_schema"}
	}
	// Restore data shouldn't restore any system tables
	if t.RestoreTables {
		if t.Keyspace == nil {
			t.Keyspace = []string{"*"}
		}
		t.Keyspace = append(t.Keyspace, "!system_schema")
	}

	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return t, errors.Wrapf(err, "get client")
	}

	status, err := client.Status(ctx)
	if err != nil {
		return t, errors.Wrap(err, "get status")
	}
	if len(status) == 0 {
		return t, errors.New("empty status")
	}
	// Check if for each location there is at least one host
	// living in location's dc with access to it.
	for _, l := range t.Location {
		var (
			remotePath     = l.RemotePath("")
			locationStatus = status
		)
		// In case location does not have specified dc, use nodes from all dcs.
		if l.DC != "" {
			locationStatus = status.Datacenter([]string{l.DC})
			if len(locationStatus) == 0 {
				return t, errors.Errorf("no nodes in location's datacenter: %s", l)
			}
		}

		if _, err = client.GetLiveNodesWithLocationAccess(ctx, locationStatus, remotePath); err != nil {
			if strings.Contains(err.Error(), "NoSuchBucket") {
				return t, errors.Errorf("specified bucket does not exist: %s", l)
			}
			return t, errors.Wrap(err, "location is not accessible")
		}
	}

	return t, nil
}

// Restore executes restore on a given target.
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
		return errors.Wrap(err, "get client proxy")
	}
	// Get cluster session
	clusterSession, err := s.clusterSession(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "get CQL cluster session")
	}
	defer clusterSession.Close()

	tools := restoreWorkerTools{
		workerTools: workerTools{
			ClusterID:   clusterID,
			ClusterName: clusterName,
			TaskID:      taskID,
			RunID:       runID,
			SnapshotTag: target.SnapshotTag,
			Client:      client,
			Config:      s.config,
			Logger:      s.logger.Named("restore"),
		},
		metrics:                 s.metrics.Restore,
		managerSession:          s.session,
		clusterSession:          clusterSession,
		forEachRestoredManifest: s.forEachRestoredManifest(clusterID, target),
	}

	var w restoreWorker
	if target.RestoreTables {
		w = &tablesWorker{restoreWorkerTools: tools}
	} else {
		w = &schemaWorker{restoreWorkerTools: tools}
	}

	if target.Continue {
		if err := w.decorateWithPrevRun(ctx, run); err != nil {
			return err
		}

		w.insertRun(ctx, run)
		// Update run with previous progress.
		if run.PrevID != uuid.Nil {
			w.clonePrevProgress(ctx, run)
		}
	} else {
		w.insertRun(ctx, run)
	}
	// Check if restore should start from scratch.
	// Empty location means that previous run hasn't started restoring any data.
	if !target.Continue || run.PrevID == uuid.Nil || run.Location == "" {
		w.startFromScratch()
		s.metrics.Restore.ResetClusterMetrics(clusterID)
	}
	// As manifests are immutable, units can be initialized only once per task
	if run.Units == nil {
		run.Units, err = w.newUnits(ctx, target)
		if err != nil {
			return errors.Wrap(err, "initialize units")
		}
	}

	run.Stage = StageRestoreData
	w.insertRun(ctx, run)

	if err = w.restore(ctx, run, target); err != nil {
		return errors.Wrapf(err, "restore data")
	}

	run.Stage = StageRestoreDone
	w.insertRun(ctx, run)

	return nil
}

// GetRestoreUnits restored units specified in restore target.
func (s *Service) GetRestoreUnits(ctx context.Context, clusterID uuid.UUID, target RestoreTarget) ([]RestoreUnit, error) {
	clusterSession, err := s.clusterSession(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "get CQL cluster session")
	}
	defer clusterSession.Close()

	w := &restoreWorkerTools{
		clusterSession:          clusterSession,
		forEachRestoredManifest: s.forEachRestoredManifest(clusterID, target),
	}

	return w.newUnits(ctx, target)
}

// GetRestoreProgress aggregates progress for the run of the task and breaks it down
// by keyspace and table.json.
func (s *Service) GetRestoreProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (RestoreProgress, error) {
	w := &restoreWorkerTools{
		workerTools: workerTools{
			ClusterID: clusterID,
			TaskID:    taskID,
			RunID:     runID,
		},
		managerSession: s.session,
	}

	return w.getProgress(ctx)
}

// forEachRestoredManifest returns a wrapper for forEachManifest that iterates over
// manifests with specified in restore target.
func (s *Service) forEachRestoredManifest(clusterID uuid.UUID, target RestoreTarget) func(context.Context, Location, func(ManifestInfoWithContent) error) error {
	return func(ctx context.Context, location Location, f func(content ManifestInfoWithContent) error) error {
		filter := ListFilter{
			SnapshotTag: target.SnapshotTag,
			Keyspace:    target.Keyspace,
		}
		return s.forEachManifest(ctx, clusterID, []Location{location}, filter, f)
	}
}
