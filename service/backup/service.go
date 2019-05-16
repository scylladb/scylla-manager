// Copyright (C) 2017 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/cespare/xxhash"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx"
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

// Service orchestrates clusterName repairs.
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

	if err := json.Unmarshal(properties, &p); err != nil {
		return Target{}, mermaid.ErrValidate(errors.Wrapf(err, "failed to parse runner properties: %s", properties), "")
	}

	t := Target{
		Location:  p.Location,
		Retention: p.Retention,
		RateLimit: p.RateLimit,
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
			Keyspace: u.Keyspace,
			Tables:   u.Tables,
		}
		if u.AllTables {
			uu.Tables = nil
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
		TTL:       timeutc.Now().Add(target.Retention.Duration()),
	}

	// TODO: run single task at a time #980
	// TODO: continue previous run

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

	// Get target hosts
	var hosts []string
	dcHosts, err := client.Datacenters(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get host datacenters")
	}
	for _, dc := range run.DC {
		dh := dcHosts[dc]

		sort.Slice(dh, func(i, j int) bool {
			return xxhash.Sum64String(hosts[i]) < xxhash.Sum64String(hosts[j])
		})

		hosts = append(hosts, dh...)
	}
	if len(hosts) == 0 {
		s.logger.Info(ctx, "no matching hosts found")
		return nil
	}

	// TODO: implement
	s.logger.Debug(ctx, "todo", "config", s.config)

	return nil
}

// putRun upserts a backup run.
func (s *Service) putRun(r *Run) error {
	stmt, names := schema.BackupRun.Insert()
	q := gocqlx.Query(s.session.Query(stmt), names).BindStruct(r)
	return q.ExecRelease()
}

// putRunLogError executes putRun and consumes the error.
//func (s *Service) putRunLogError(ctx context.Context, r *Run) {
//	if err := s.putRun(r); err != nil {
//		s.logger.Error(ctx, "Cannot update the run",
//			"run", &r,
//			"error", err,
//		)
//	}
//}
