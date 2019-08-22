// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/inexlist/dcfilter"
	"github.com/scylladb/mermaid/internal/inexlist/ksfilter"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
)

// ClusterNameFunc returns name for a given ID.
type ClusterNameFunc func(ctx context.Context, clusterID uuid.UUID) (string, error)

// Service orchestrates clusterName backups.
type Service struct {
	session *gocql.Session
	config  Config

	clusterName  ClusterNameFunc
	scyllaClient scyllaclient.ProviderFunc
	logger       log.Logger
}

func NewService(session *gocql.Session, config Config, clusterName ClusterNameFunc, scyllaClient scyllaclient.ProviderFunc, logger log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if clusterName == nil {
		return nil, errors.New("invalid cluster name provider")
	}

	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		session:      session,
		config:       config,
		clusterName:  clusterName,
		scyllaClient: scyllaClient,
		logger:       logger,
	}, nil
}

// Runner creates a Runner that handles repairs.
func (s *Service) Runner() Runner {
	return Runner{service: s}
}

// GetTarget converts runner properties into repair Target.
func (s *Service) GetTarget(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage, force bool) (Target, error) {
	p := defaultTaskProperties()
	t := Target{}

	if err := json.Unmarshal(properties, &p); err != nil {
		return t, mermaid.ErrValidate(errors.Wrapf(err, "failed to parse runner properties: %s", properties), "")
	}

	// Copy simple properties
	t.Location = p.Location
	t.Retention = p.Retention
	t.RateLimit = p.RateLimit
	t.SnapshotParallel = p.SnapshotParallel
	t.UploadParallel = p.UploadParallel
	t.Continue = p.Continue

	if p.Location == nil {
		return t, errors.Errorf("missing location")
	}

	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return t, errors.Wrapf(err, "failed to get client")
	}

	// Get hosts in DCs
	dcMap, err := client.Datacenters(ctx)
	if err != nil {
		return t, errors.Wrap(err, "failed to read datacenters")
	}

	// Filter DCs
	if t.DC, err = dcfilter.Apply(dcMap, p.DC); err != nil {
		return t, err
	}

	// Validate location DCs
	if err := checkDCs(func(i int) (string, string) { return t.Location[i].DC, t.Location[i].String() }, len(t.Location), dcMap); err != nil {
		return t, errors.Wrap(err, "invalid location")
	}
	if err := checkAllDCsCovered(func(i int) string { return t.Location[i].DC }, len(t.Location), t.DC); err != nil {
		return t, errors.Wrap(err, "invalid location")
	}

	// Validate rate limit DCs
	if err := checkDCs(dcLimitDCAtPos(t.RateLimit), len(t.RateLimit), dcMap); err != nil {
		return t, errors.Wrap(err, "invalid rate-limit")
	}

	// Validate upload parallel DCs
	if err := checkDCs(dcLimitDCAtPos(t.SnapshotParallel), len(t.SnapshotParallel), dcMap); err != nil {
		return t, errors.Wrap(err, "invalid snapshot-parallel")
	}

	// Validate snapshot parallel DCs
	if err := checkDCs(dcLimitDCAtPos(t.UploadParallel), len(t.UploadParallel), dcMap); err != nil {
		return t, errors.Wrap(err, "invalid upload-parallel")
	}

	// Filter keyspaces
	f, err := ksfilter.NewFilter(p.Keyspace)
	if err != nil {
		return t, err
	}

	keyspaces, err := client.Keyspaces(ctx)
	if err != nil {
		return t, errors.Wrapf(err, "failed to read keyspaces")
	}
	for _, keyspace := range keyspaces {
		tables, err := client.Tables(ctx, keyspace)
		if err != nil {
			return t, errors.Wrapf(err, "keyspace %s: failed to get tables", keyspace)
		}

		// Get the ring description and skip local data
		ring, err := client.DescribeRing(ctx, keyspace)
		if err != nil {
			return t, errors.Wrapf(err, "keyspace %s: failed to get ring description", keyspace)
		}
		if ring.Replication == scyllaclient.LocalStrategy {
			continue
		}

		// Add to the filter
		f.Add(keyspace, tables)
	}

	// Get the filtered units
	v, err := f.Apply(force)
	if err != nil {
		return t, err
	}

	// Copy units
	for _, u := range v {
		uu := Unit{
			Keyspace:  u.Keyspace,
			Tables:    u.Tables,
			AllTables: u.AllTables,
		}
		t.Units = append(t.Units, uu)
	}

	return t, nil
}

// Backup executes a backup on a given target.
func (s *Service) Backup(ctx context.Context, clusterID uuid.UUID, taskID uuid.UUID, runID uuid.UUID, target Target) error {
	s.logger.Debug(ctx, "Backup",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
		"target", target,
	)

	run := &Run{
		ClusterID: clusterID,
		TaskID:    taskID,
		ID:        runID,
		Units:     target.Units,
		DC:        target.DC,
		Location:  target.Location,
		StartTime: timeutc.Now().UTC(),
	}

	s.logger.Info(ctx, "Initializing backup",
		"cluster_id", run.ClusterID,
		"task_id", run.TaskID,
		"run_id", run.ID,
		"target", target,
	)

	// Register the run
	if err := s.putRun(run); err != nil {
		return errors.Wrap(err, "failed to register the run")
	}

	// Get the cluster client
	client, err := s.scyllaClient(ctx, run.ClusterID)
	if err != nil {
		return errors.Wrap(err, "failed to get client proxy")
	}

	// Get hosts in all DCs
	dcMap, err := client.Datacenters(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to read datacenters")
	}

	// Get hosts in the given DCs
	hosts := dcHosts(dcMap, run.DC)
	if len(hosts) == 0 {
		return errors.Wrap(err, "no matching hosts found")
	}

	// Get host IDs
	hostIDs, err := client.HostIDs(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get host IDs")
	}

	// Create hostInfo for hosts
	hi, err := hostInfoFromHosts(hosts, dcMap, hostIDs, target.Location, target.RateLimit)
	if err != nil {
		return err
	}

	if target.Continue {
		if err := s.decorateWithPrevRun(ctx, run); err != nil {
			return err
		}
		// Update run with previous progress.
		if run.PrevID != uuid.Nil {
			s.putRunLogError(ctx, run)
		}
	}

	// Create a worker
	w := worker{
		clusterID: clusterID,
		taskID:    taskID,
		runID:     runID,

		config: s.config,
		units:  run.Units,
		client: client,
		logger: s.logger.Named("worker"),

		OnRunProgress: s.putRunProgressLogError,
	}

	if run.PrevID == uuid.Nil {
		if err := w.Snapshot(ctx, hi, target.SnapshotParallel); err != nil {
			return err
		}
	}
	prog, err := s.getProgress(run)
	if err != nil {
		return errors.Wrap(err, "failed to load run progress")
	}

	return w.Upload(ctx, hi, target.UploadParallel, prog)
}

// decorateWithPrevRun gets task previous run and if it can be continued
// sets PrevID on the given run.
func (s *Service) decorateWithPrevRun(ctx context.Context, run *Run) error {
	prev, err := s.GetLastResumableRun(ctx, run.ClusterID, run.TaskID)
	if err == mermaid.ErrNotFound {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "failed to get previous run")
	}

	// Check if can continue from prev
	s.logger.Info(ctx, "Found previous run", "run_id", prev.ID, "prev_run_id", prev.PrevID)
	if timeutc.Since(prev.StartTime) > s.config.AgeMax {
		s.logger.Info(ctx, "Starting from scratch: previous run is too old")
		return nil
	}

	run.PrevID = prev.ID
	run.Units = prev.Units
	run.DC = prev.DC

	return nil
}

// GetLastResumableRun returns the the most recent started but not done run of
// the task, if there is a recent run that is completely done ErrNotFound is
// reported.
func (s *Service) GetLastResumableRun(ctx context.Context, clusterID, taskID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetLastResumableRun",
		"cluster_id", clusterID,
		"task_id", taskID,
	)

	stmt, names := qb.Select(schema.BackupRun.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("task_id"),
	).Limit(20).ToCql()

	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
	})

	var runs []*Run
	if err := q.SelectRelease(&runs); err != nil {
		return nil, err
	}

	for _, r := range runs {
		prog, err := s.getProgress(r)
		if err != nil {
			return nil, err
		}
		size, uploaded := runProgress(r, prog)
		if size > 0 {
			if size == uploaded {
				break
			}
			return r, nil
		}
	}

	return nil, mermaid.ErrNotFound
}

func (s *Service) getProgress(run *Run) ([]*RunProgress, error) {
	stmt, names := schema.BackupRunProgress.Select()

	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.TaskID,
		"run_id":     run.ID,
	})

	var p []*RunProgress
	return p, q.SelectRelease(&p)
}

// putRun upserts a backup run.
func (s *Service) putRun(r *Run) error {
	stmt, names := schema.BackupRun.Insert()
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(r)
	return q.ExecRelease()
}

// putRunLogError executes putRun and consumes the error.
func (s *Service) putRunLogError(ctx context.Context, r *Run) {
	if err := s.putRun(r); err != nil {
		s.logger.Error(ctx, "failed to update the run",
			"run", r,
			"error", err,
		)
	}
}

// putRunProgress upserts a backup run progress.
func (s *Service) putRunProgress(ctx context.Context, p *RunProgress) error {
	s.logger.Debug(ctx, "PutRunProgress", "run_progress", p)

	stmt, names := schema.BackupRunProgress.Insert()
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(p)

	return q.ExecRelease()
}

// putRunProgressLogError executes putRunProgress and consumes the error.
func (s *Service) putRunProgressLogError(ctx context.Context, p *RunProgress) {
	if err := s.putRunProgress(ctx, p); err != nil {
		s.logger.Error(ctx, "Failed to update file progress",
			"progress", p,
			"error", err,
		)
	}
}

// GetRun returns a run based on ID. If nothing was found mermaid.ErrNotFound
// is returned.
func (s *Service) GetRun(ctx context.Context, clusterID, taskID, runID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetRun",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	stmt, names := schema.BackupRun.Get()

	q := gocqlx.Query(s.session.Query(stmt), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"task_id":    taskID,
		"id":         runID,
	})

	var r Run
	return &r, q.GetRelease(&r)
}

// GetProgress aggregates progress for the run of the task and breaks it down
// by keyspace and table.json
// If nothing was found mermaid.ErrNotFound is returned.
func (s *Service) GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (Progress, error) {
	s.logger.Debug(ctx, "GetProgress",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	run, err := s.GetRun(ctx, clusterID, taskID, runID)
	if err != nil {
		return Progress{}, err
	}
	prog, err := s.getProgress(run)
	if err != nil {
		return Progress{}, err
	}

	p := aggregateProgress(run, prog)

	return p, nil
}
