// Copyright (C) 2023 ScyllaDB

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/maps"
)

// Service orchestrates clusterName backups.
type Service struct {
	repairSvc *repair.Service // Used for running post-restore repair

	session gocqlx.Session
	config  Config
	metrics metrics.RestoreMetrics
	fg      ScyllaFeatureGate

	scyllaClient   scyllaclient.ProviderFunc
	clusterSession cluster.SessionFunc
	configCache    configcache.ConfigCacher
	logger         log.Logger
}

// ScyllaFeatureGate is a helper for checking scylla feature availability based on scylla version.
type ScyllaFeatureGate interface {
	NativeRestore(version string) (bool, error)
}

func NewService(repairSvc *repair.Service, session gocqlx.Session, config Config, metrics metrics.RestoreMetrics,
	fg ScyllaFeatureGate, scyllaClient scyllaclient.ProviderFunc, clusterSession cluster.SessionFunc,
	configCache configcache.ConfigCacher, logger log.Logger,
) (*Service, error) {
	if session.Session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}
	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		repairSvc:      repairSvc,
		session:        session,
		config:         config,
		metrics:        metrics,
		fg:             fg,
		scyllaClient:   scyllaClient,
		clusterSession: clusterSession,
		configCache:    configCache,
		logger:         logger,
	}, nil
}

// Restore creates and initializes worker performing restore with given properties.
func (s *Service) Restore(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	s.logger.Info(ctx, "Restore",
		"cluster_id", clusterID,
		"task_id", taskID,
		"run_id", runID,
	)

	w, err := s.newWorker(ctx, clusterID)
	if err != nil {
		return errors.Wrap(err, "create worker")
	}
	defer w.clusterSession.Close()
	w.setRunInfo(taskID, runID)

	if err := w.init(ctx, properties); err != nil {
		return err
	}

	if w.run.PrevID == uuid.Nil {
		// Reset metrics on fresh start
		w.metrics.ResetClusterMetrics(w.run.ClusterID)
	} else {
		w.clonePrevProgress(ctx)
		// Check that all units are still present after resume
		for _, u := range w.run.Units {
			for _, t := range u.Tables {
				ok, err := w.client.TableExists(ctx, "", u.Keyspace, t.Table)
				if err != nil {
					return errors.Wrapf(err, "query table %s.%s existence", u.Keyspace, t.Table)
				}
				if !ok {
					return fmt.Errorf("table %s.%s not found", u.Keyspace, t.Table)
				}
			}
		}
	}
	w.insertRun(ctx)

	if w.target.RestoreTables {
		var totalBytesToRestore int64
		for _, unit := range w.run.Units {
			totalBytesToRestore += unit.Size
		}
		tw, workerErr := newTablesWorker(ctx, w, s.repairSvc, totalBytesToRestore)
		if workerErr != nil {
			err = workerErr
		} else {
			err = tw.restore(ctx)
		}
	} else {
		sw := &schemaWorker{worker: w}
		err = sw.restore(ctx)
	}

	if err == nil {
		w.run.Stage = StageDone
		w.insertRun(ctx)
	}
	return err
}

// GetTargetUnitsViews returns all information necessary for task validation and --dry-run.
func (s *Service) GetTargetUnitsViews(ctx context.Context, clusterID uuid.UUID, properties json.RawMessage) (Target, []Unit, []View, error) {
	w, err := s.newWorker(ctx, clusterID)
	if err != nil {
		return Target{}, nil, nil, errors.Wrap(err, "create worker")
	}
	defer w.clusterSession.Close()

	if err := w.init(ctx, properties); err != nil {
		return Target{}, nil, nil, err
	}
	return w.target, w.run.Units, w.run.Views, nil
}

// GetProgress aggregates progress for the run of the task and breaks it down by keyspace and table.
func (s *Service) GetProgress(ctx context.Context, clusterID, taskID, runID uuid.UUID) (Progress, error) {
	run, err := GetRun(s.session, clusterID, taskID, runID)
	if err != nil {
		return Progress{}, errors.Wrap(err, "get run")
	}

	pr, err := getProgress(run, s.session)
	if err != nil {
		return Progress{}, err
	}

	// Check if repair progress needs to be filled
	if run.RepairTaskID == uuid.Nil {
		return pr, nil
	}

	q := table.RepairRun.SelectQuery(s.session).BindMap(qb.M{
		"cluster_id": run.ClusterID,
		"task_id":    run.RepairTaskID,
	})

	var repairRun repair.Run
	if err = q.GetRelease(&repairRun); err != nil {
		return pr, errors.Wrap(err, "get repair run")
	}

	repairPr, err := s.repairSvc.GetProgress(ctx, repairRun.ClusterID, repairRun.TaskID, repairRun.ID)
	if err != nil {
		return Progress{}, errors.Wrap(err, "get repair progress")
	}

	pr.RepairProgress = &repairPr
	return pr, nil
}

func (s *Service) newWorker(ctx context.Context, clusterID uuid.UUID) (worker, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return worker{}, errors.Wrap(err, "get client")
	}
	clusterSession, err := s.clusterSession(ctx, clusterID)
	if err != nil {
		return worker{}, errors.Wrap(err, "get CQL cluster session")
	}
	rawNodeConfig, err := s.configCache.ReadAll(clusterID)
	if err != nil {
		return worker{}, errors.Wrap(err, "read all nodes config")
	}
	nodeConfig, err := maps.MapKeyWithError(rawNodeConfig, netip.ParseAddr)
	if err != nil {
		return worker{}, errors.Wrap(err, "parse node config IP address")
	}

	return worker{
		run: &Run{
			ClusterID: clusterID,
			Stage:     StageInit,
		},
		config:         s.config,
		logger:         s.logger,
		metrics:        s.metrics,
		fg:             s.fg,
		client:         client,
		session:        s.session,
		clusterSession: clusterSession,
		nodeConfig:     nodeConfig,
	}, nil
}

func (w *worker) setRunInfo(taskID, runID uuid.UUID) {
	w.run.TaskID = taskID
	w.run.ID = runID
}

// GetRun returns run with specified cluster, task and run ID.
// If run ID is not specified, it returns the latest run with specified cluster and task ID.
func GetRun(s gocqlx.Session, clusterID, taskID, runID uuid.UUID) (*Run, error) {
	var q *gocqlx.Queryx
	if runID != uuid.Nil {
		q = table.RestoreRun.GetQuery(s).BindMap(qb.M{
			"cluster_id": clusterID,
			"task_id":    taskID,
			"id":         runID,
		})
	} else {
		q = table.RestoreRun.SelectQuery(s).BindMap(qb.M{
			"cluster_id": clusterID,
			"task_id":    taskID,
		})
	}

	var r Run
	return &r, q.GetRelease(&r)
}
