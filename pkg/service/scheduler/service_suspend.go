// Copyright (C) 2022 ScyllaDB

package scheduler

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/b16set"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/pkg/scheduler"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
	"github.com/scylladb/scylla-manager/pkg/service"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func (s *Service) initSuspended() error {
	var clusters []uuid.UUID
	if err := qb.Select(table.SchedulerTask.Name()).Distinct("cluster_id").Query(s.session).SelectRelease(&clusters); err != nil {
		return errors.Wrap(err, "list clusters")
	}

	for _, c := range clusters {
		si := &suspendInfo{ClusterID: c}
		if err := s.drawer.Get(si); err != nil {
			if !errors.Is(err, service.ErrNotFound) {
				return err
			}
		} else {
			s.suspended.Add(c.Bytes16())
		}
	}

	return nil
}

// IsSuspended returns true iff cluster is suspended.
func (s *Service) IsSuspended(ctx context.Context, clusterID uuid.UUID) bool {
	s.logger.Debug(ctx, "IsSuspended", "clusterID", clusterID)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isSuspendedLocked(clusterID)
}

func (s *Service) isSuspendedLocked(clusterID uuid.UUID) bool {
	return s.suspended.Has(clusterID.Bytes16())
}

// Suspend stops scheduler for a given cluster.
// Running tasks will be stopped.
// Scheduled task executions will be canceled.
// Scheduler can be later resumed, see `Resume` function.
func (s *Service) Suspend(ctx context.Context, clusterID uuid.UUID) error {
	s.mu.Lock()
	si, l := s.suspendLocked(ctx, clusterID)
	s.mu.Unlock()

	if si == nil {
		return nil
	}
	if err := s.forEachClusterHealthCheckTask(clusterID, func(t *Task) error {
		s.schedule(ctx, t, false)
		return nil
	}); err != nil {
		return errors.Wrap(err, "schedule")
	}
	if err := s.drawer.Put(si); err != nil {
		return errors.Wrap(err, "save canceled tasks")
	}

	if l != nil {
		l.Wait()
	}
	return nil
}

func (s *Service) suspendLocked(ctx context.Context, clusterID uuid.UUID) (*suspendInfo, *scheduler.Scheduler) {
	s.logger.Info(ctx, "Suspending cluster", "cluster_id", clusterID)

	if s.suspended.Has(clusterID.Bytes16()) {
		s.logger.Info(ctx, "Cluster already suspended", "cluster_id", clusterID)
		return nil, nil
	}

	s.suspended.Add(clusterID.Bytes16())
	si := &suspendInfo{
		ClusterID: clusterID,
		StartedAt: timeutc.Now(),
	}
	l := s.scheduler[clusterID]
	if l != nil {
		si.RunningTask, si.PendingTasks = l.Close()
	}
	s.scheduler[clusterID] = s.newScheduler(clusterID)
	return si, l
}

// Resume resumes scheduler for a suspended cluster.
func (s *Service) Resume(ctx context.Context, clusterID uuid.UUID, startTasks bool) error {
	s.logger.Info(ctx, "Resuming cluster", "cluster_id", clusterID)

	s.mu.Lock()
	if !s.suspended.Has(clusterID.Bytes16()) {
		s.mu.Unlock()
		s.logger.Info(ctx, "Cluster not suspended", "cluster_id", clusterID)
		return nil
	}
	si := &suspendInfo{ClusterID: clusterID}
	if err := s.drawer.Get(si); err != nil {
		if errors.Is(err, service.ErrNotFound) {
			s.logger.Error(ctx, "Expected canceled tasks got none")
		} else {
			s.mu.Unlock()
			return errors.Wrap(err, "get canceled tasks")
		}
	}
	if err := s.drawer.Delete(si); err != nil {
		s.logger.Error(ctx, "Failed to delete canceled tasks", "error", err)
	}
	s.suspended.Remove(clusterID.Bytes16())
	s.mu.Unlock()

	running := b16set.New()
	if startTasks {
		for _, u := range si.RunningTask {
			running.Add(u.Bytes16())
		}
	}
	if err := s.forEachClusterTask(clusterID, func(t *Task) error {
		s.schedule(ctx, t, running.Has(t.ID.Bytes16()))
		return nil
	}); err != nil {
		return errors.Wrap(err, "schedule")
	}

	return nil
}

func (s *Service) forEachClusterHealthCheckTask(clusterID uuid.UUID, f func(t *Task) error) error {
	q := qb.Select(table.SchedulerTask.Name()).
		Where(qb.Eq("cluster_id"), qb.Eq("type")).
		Query(s.session).
		Bind(clusterID, HealthCheckTask)
	defer q.Release()

	return forEachTaskWithQuery(q, f)
}

func (s *Service) forEachClusterTask(clusterID uuid.UUID, f func(t *Task) error) error {
	q := qb.Select(table.SchedulerTask.Name()).Where(qb.Eq("cluster_id")).Query(s.session).Bind(clusterID)
	defer q.Release()
	return forEachTaskWithQuery(q, f)
}
