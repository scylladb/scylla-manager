// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"encoding/json"
	"sync"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/go-set/b16set"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/pkg/metrics"
	"github.com/scylladb/scylla-manager/pkg/scheduler"
	"github.com/scylladb/scylla-manager/pkg/scheduler/trigger"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
	"github.com/scylladb/scylla-manager/pkg/service"
	"github.com/scylladb/scylla-manager/pkg/store"
	"github.com/scylladb/scylla-manager/pkg/util/jsonutil"
	"github.com/scylladb/scylla-manager/pkg/util/pointer"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// PropertiesDecorator modifies task properties before running.
type PropertiesDecorator func(ctx context.Context, clusterID, taskID uuid.UUID, properties json.RawMessage) (json.RawMessage, error)

type Service struct {
	session gocqlx.Session
	metrics metrics.SchedulerMetrics
	drawer  store.Store
	logger  log.Logger

	decorators map[TaskType]PropertiesDecorator
	runners    map[TaskType]Runner
	runs       map[uuid.UUID]Run
	resolver   resolver
	scheduler  map[uuid.UUID]*scheduler.Scheduler
	suspended  *b16set.Set
	noContinue map[uuid.UUID]time.Time
	closed     bool
	mu         sync.Mutex
}

func NewService(session gocqlx.Session, metrics metrics.SchedulerMetrics, drawer store.Store, logger log.Logger) (*Service, error) {
	s := &Service{
		session: session,
		metrics: metrics,
		drawer:  drawer,
		logger:  logger,

		decorators: make(map[TaskType]PropertiesDecorator),
		runners:    make(map[TaskType]Runner),
		runs:       make(map[uuid.UUID]Run),
		resolver:   newResolver(),
		scheduler:  make(map[uuid.UUID]*scheduler.Scheduler),
		suspended:  b16set.New(),
		noContinue: make(map[uuid.UUID]time.Time),
	}
	if err := s.initSuspended(); err != nil {
		return nil, errors.Wrap(err, "init suspended")
	}
	return s, nil
}

func (s *Service) initSuspended() error {
	var clusters []uuid.UUID
	if err := qb.Select(table.SchedTask.Name()).Distinct("cluster_id").Query(s.session).SelectRelease(&clusters); err != nil {
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

// SetPropertiesDecorator sets optional decorator of properties for a given
// task type.
func (s *Service) SetPropertiesDecorator(tp TaskType, d PropertiesDecorator) {
	s.mu.Lock()
	s.decorators[tp] = d
	s.mu.Unlock()
}

// PropertiesDecorator returns the PropertiesDecorator for a task type.
func (s *Service) PropertiesDecorator(tp TaskType) PropertiesDecorator {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.decorators[tp]
}

// SetRunner assigns runner for a given task type.
// All runners need to be registered prior to running the service.
// The registration is separated from constructor to loosen coupling between services.
func (s *Service) SetRunner(tp TaskType, r Runner) {
	s.mu.Lock()
	s.runners[tp] = r
	s.mu.Unlock()
}

func (s *Service) mustRunner(tp TaskType) Runner {
	s.mu.Lock()
	r, ok := s.runners[tp]
	s.mu.Unlock()

	if !ok {
		panic("no runner")
	}
	return r
}

// LoadTasks should be called on start it loads and schedules task from database.
func (s *Service) LoadTasks(ctx context.Context) error {
	s.logger.Info(ctx, "Loading tasks from database")

	endTime := now()
	err := s.forEachTask(func(t *Task) error {
		s.initMetrics(t)
		ab, err := s.markRunningAsAborted(t, endTime)
		if err != nil {
			return errors.Wrap(err, "fix last run status")
		}
		if t.Type.isHealthCheck() || !s.IsSuspended(ctx, t.ClusterID) {
			s.schedule(ctx, t, ab)
		}
		return nil
	})
	if err != nil {
		s.logger.Info(ctx, "Failed to load task from database")
	} else {
		s.logger.Info(ctx, "All tasks scheduled")
	}
	return err
}

func (s *Service) forEachTask(f func(t *Task) error) error {
	q := qb.Select(table.SchedTask.Name()).Query(s.session)
	defer q.Release()
	return forEachTaskWithQuery(q, f)
}

func (s *Service) markRunningAsAborted(t *Task, endTime time.Time) (bool, error) {
	r, err := s.getLastRun(t)
	if err != nil {
		if errors.Is(err, service.ErrNotFound) {
			return false, nil
		}
		return false, err
	}

	if r.Status == StatusAborted {
		return true, nil
	}
	if r.Status == StatusRunning {
		r.Status = StatusAborted
		r.Cause = "service stopped"
		r.EndTime = &endTime
		return true, s.putRun(r)
	}

	return false, nil
}

func (s *Service) getLastRun(t *Task) (*Run, error) {
	q := s.getLastRunQuery(t, 1)
	var run Run
	return &run, q.GetRelease(&run)
}

func (s *Service) getLastRunQuery(t *Task, n int) *gocqlx.Queryx {
	return qb.Select(table.SchedRun.Name()).
		Where(qb.Eq("cluster_id"), qb.Eq("type"), qb.Eq("task_id")).
		Limit(uint(n)).
		Query(s.session).
		BindMap(qb.M{
			"cluster_id": t.ClusterID,
			"type":       t.Type,
			"task_id":    t.ID,
		})
}

// GetLastRuns returns n last runs of a task.
func (s *Service) GetLastRuns(ctx context.Context, t *Task, n int) ([]*Run, error) {
	s.logger.Debug(ctx, "GetLastRuns", "task", t, "n", n)
	q := s.getLastRunQuery(t, n)
	var runs []*Run
	return runs, q.SelectRelease(&runs)
}

// GetRun returns a run based on ID. If nothing was found ErrNotFound is returned.
func (s *Service) GetRun(ctx context.Context, t *Task, runID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetRun", "task", t, "run_id", runID)

	if err := t.Validate(); err != nil {
		return nil, err
	}

	r := &Run{
		ClusterID: t.ClusterID,
		Type:      t.Type,
		TaskID:    t.ID,
		ID:        runID,
	}
	q := table.SchedRun.GetQuery(s.session).BindStruct(r)
	return r, q.GetRelease(r)
}

// ListTasks returns all the tasks stored, tp is optional if empty all task
// types will loaded.
func (s *Service) ListTasks(ctx context.Context, clusterID uuid.UUID, tp TaskType) ([]*Task, error) {
	s.logger.Debug(ctx, "ListTasks", "cluster_id", clusterID, "task_type", tp)

	b := qb.Select(table.SchedTask.Name()).Where(qb.Eq("cluster_id"))
	m := qb.M{
		"cluster_id": clusterID,
	}

	if tp != "" {
		b.Where(qb.Eq("type"))
		m["type"] = tp
	}

	q := b.Query(s.session).BindMap(m)
	defer q.Release()

	if q.Err() != nil {
		return nil, q.Err()
	}

	var tasks []*Task
	err := q.Select(&tasks)
	return tasks, err
}

// PutTaskOnce upserts a task. Only one task of the same type can exist for the current cluster.
// If attempting to create a task of the same type a validation error is returned.
// The task instance must pass Validate() checks.
func (s *Service) PutTaskOnce(ctx context.Context, t *Task) error {
	s.logger.Debug(ctx, "PutTaskOnce", "task", t)

	if t == nil {
		return service.ErrNilPtr
	}

	hs, err := s.ListTasks(ctx, t.ClusterID, t.Type)
	if err != nil {
		return err
	}

	if len(hs) == 0 {
		// Create a new task
		return s.PutTask(ctx, t)
	}

	for _, h := range hs {
		if h.ID == t.ID {
			// Update an existing task
			return s.PutTask(ctx, t)
		}
	}

	return service.ErrValidate(errors.Errorf("a task of type %s exists for cluster %s", t.Type, t.ClusterID))
}

// PutTask upserts a task.
func (s *Service) PutTask(ctx context.Context, t *Task) error {
	create := false
	if t != nil && t.ID == uuid.Nil {
		id, err := uuid.NewRandom()
		if err != nil {
			return errors.Wrap(err, "couldn't generate random UUID for task")
		}
		t.ID = id
		create = true
	}
	if err := t.Validate(); err != nil {
		return err
	}

	s.logger.Info(ctx, "PutTask", "task", t, "schedule", t.Sched, "properties", t.Properties, "create", create)

	suspended := s.IsSuspended(ctx, t.ClusterID)
	if create && suspended {
		return service.ErrValidate(errors.New("cluster is suspended, scheduling tasks is not allowed"))
	}
	if err := s.putTask(t); err != nil {
		return err
	}
	if create {
		s.initMetrics(t)
	}
	if !suspended {
		s.schedule(ctx, t, false)
	}
	return nil
}

func (s *Service) putTask(t *Task) error {
	return table.SchedTask.InsertQuery(s.session).BindStruct(t).ExecRelease()
}

func (s *Service) initMetrics(t *Task) {
	s.metrics.Init(t.ClusterID, t.Type.String(), t.ID, *(*[]string)(unsafe.Pointer(&allStatuses))...)
}

func (s *Service) schedule(ctx context.Context, t *Task, run bool) {
	s.mu.Lock()
	s.resolver.Put(newTaskInfoFromTask(t))
	l, lok := s.scheduler[t.ClusterID]
	if !lok {
		l = s.newScheduler(t.ClusterID)
		s.scheduler[t.ClusterID] = l
	}
	s.mu.Unlock()

	if t.Enabled {
		d := details(t)
		if run {
			d.Trigger = trigger.NewMulti(trigger.NewOnce(), d.Trigger)
		}
		l.Schedule(ctx, t.ID, d)
	} else {
		l.Unschedule(ctx, t.ID)
	}
}

func (s *Service) newScheduler(clusterID uuid.UUID) *scheduler.Scheduler {
	l := scheduler.NewScheduler(now, s.run, newSchedulerListener(s.logger.Named(clusterID.String()[0:8])))
	go l.Start(context.Background())
	return l
}

const noContinueThreshold = 500 * time.Millisecond

func (s *Service) run(ctx scheduler.RunContext) (runErr error) {
	s.mu.Lock()
	ti, ok := s.resolver.Find(ctx.Key.String())
	c, cok := s.noContinue[ti.TaskID]
	if cok {
		delete(s.noContinue, ti.TaskID)
	}
	d := s.decorators[ti.TaskType]
	r := newRunFromTaskInfo(ti)
	s.runs[ti.TaskID] = *r
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.runs, ti.TaskID)
		s.mu.Unlock()
	}()

	if !ok {
		return service.ErrNotFound
	}
	if err := s.putRun(r); err != nil {
		return errors.Wrap(err, "put run")
	}
	s.metrics.BeginRun(ti.ClusterID, ti.TaskType.String(), ti.TaskID)

	runCtx := log.WithTraceID(ctx)
	logger := s.logger.Named(ti.ClusterID.String()[0:8])

	if !ti.TaskType.isHealthCheck() {
		logger.Info(runCtx, "Run started",
			"task", ti,
			"retry", ctx.Retry,
		)
	}

	defer func() {
		r.Status = statusFromError(runErr)
		if r.Status == StatusError {
			r.Cause = runErr.Error()
		}
		if r.Status == StatusStopped && s.isClosed() {
			r.Status = StatusAborted
		}
		r.EndTime = pointer.TimePtr(now())
		if err := s.putRun(r); err != nil {
			logger.Error(runCtx, "Cannot update the run", "task", ti, "run", r, "error", err)
		}
		s.metrics.EndRun(ti.ClusterID, ti.TaskType.String(), ti.TaskID, r.Status.String())
		if !ti.TaskType.isHealthCheck() {
			if r.Status == StatusError {
				logger.Error(runCtx, "Run ended with ERROR",
					"task", ti,
					"status", r.Status,
					"cause", r.Cause,
					"duration", r.EndTime.Sub(r.StartTime),
				)
			} else {
				logger.Info(runCtx, "Run ended",
					"task", ti,
					"status", r.Status,
					"duration", r.EndTime.Sub(r.StartTime),
				)
			}
		}
	}()

	if ctx.Properties == nil {
		ctx.Properties = json.RawMessage("{}")
	}
	if ctx.Retry == 0 && now().Sub(c) < noContinueThreshold {
		ctx.Properties = jsonutil.Set(ctx.Properties, "continue", false)
	}
	if d != nil {
		p, err := d(runCtx, ti.ClusterID, ti.TaskID, ctx.Properties)
		if err != nil {
			return errors.Wrap(err, "decorate properties")
		}
		ctx.Properties = p
	}
	return s.mustRunner(ti.TaskType).Run(runCtx, ti.ClusterID, ti.TaskID, r.ID, ctx.Properties)
}

func (s *Service) putRun(r *Run) error {
	return table.SchedRun.
		InsertQuery(s.session).
		BindStruct(r).
		ExecRelease()
}

func statusFromError(err error) Status {
	switch {
	case err == nil:
		return StatusDone
	case errors.Is(err, context.Canceled):
		return StatusStopped
	case errors.Is(err, context.DeadlineExceeded):
		return StatusWaiting
	default:
		return StatusError
	}
}

// GetTaskByID returns a task based on ID and type. If nothing was found
// scylla-manager.ErrNotFound is returned.
func (s *Service) GetTaskByID(ctx context.Context, clusterID uuid.UUID, tp TaskType, id uuid.UUID) (*Task, error) {
	s.logger.Debug(ctx, "GetTaskByID", "cluster_id", clusterID, "id", id)
	t := &Task{
		ClusterID: clusterID,
		Type:      tp,
		ID:        id,
	}
	q := table.SchedTask.GetQuery(s.session).BindStruct(t)
	return t, q.GetRelease(t)
}

// FindTaskByPrefix returns task by prefix of ID or name if the prefix is unique.
// ID is only checked if prefix length is over 8 characters.
func (s *Service) FindTaskByPrefix(ctx context.Context, pre string) (*Task, error) {
	s.logger.Debug(ctx, "FindTaskByPrefix", "prefix", pre)

	s.mu.Lock()
	ti, ok := s.resolver.Find(pre)
	s.mu.Unlock()

	if !ok {
		return nil, service.ErrNotFound
	}
	return s.GetTaskByID(ctx, ti.ClusterID, ti.TaskType, ti.TaskID)
}

// DeleteTask removes and stops task based on ID.
func (s *Service) DeleteTask(ctx context.Context, t *Task) error {
	s.logger.Debug(ctx, "DeleteTask", "task", t)

	q := table.SchedTask.DeleteQuery(s.session).BindMap(qb.M{
		"cluster_id": t.ClusterID,
		"type":       t.Type,
		"id":         t.ID,
	})
	if err := q.ExecRelease(); err != nil {
		return err
	}

	s.mu.Lock()
	l, lok := s.scheduler[t.ClusterID]
	s.mu.Unlock()
	if lok {
		l.Unschedule(ctx, t.ID)
	}

	s.logger.Info(ctx, "Task deleted",
		"cluster_id", t.ClusterID,
		"task_type", t.Type,
		"task_id", t.ID,
	)
	return nil
}

// StartTask starts execution of a task immediately.
func (s *Service) StartTask(ctx context.Context, t *Task) error {
	return s.startTask(ctx, t, false)
}

// StartTaskNoContinue starts execution of a task immediately and adds the
// "no_continue" flag to properties of the next run.
// The possible retries would not have the flag enabled.
func (s *Service) StartTaskNoContinue(ctx context.Context, t *Task) error {
	return s.startTask(ctx, t, true)
}

func (s *Service) startTask(ctx context.Context, t *Task, noContinue bool) error {
	s.logger.Debug(ctx, "StartTask", "task", t, "no_continue", noContinue)

	s.mu.Lock()
	if s.isSuspendedLocked(t.ClusterID) {
		s.mu.Unlock()
		return service.ErrValidate(errors.New("cluster is suspended"))
	}
	l, lok := s.scheduler[t.ClusterID]
	if !lok {
		l = s.newScheduler(t.ClusterID)
		s.scheduler[t.ClusterID] = l
	}
	if noContinue {
		s.noContinue[t.ID] = now()
	}
	s.mu.Unlock()

	// For regular tasks trigger will be enough but for one shot or disabled
	// tasks we need to reschedule them to run once.
	if !l.Trigger(ctx, t.ID) {
		d := details(t)
		d.Trigger = trigger.NewOnce()
		l.Schedule(ctx, t.ID, d)
	}
	return nil
}

// StopTask stops task execution of immediately, task is rescheduled according
// to its run interval.
func (s *Service) StopTask(ctx context.Context, t *Task) error {
	s.logger.Debug(ctx, "StopTask", "task", t)

	s.mu.Lock()
	l, lok := s.scheduler[t.ClusterID]
	r, rok := s.runs[t.ID]
	s.mu.Unlock()

	if !lok || !rok {
		return nil
	}

	r.Status = StatusStopping
	if err := s.updateRunStatus(&r); err != nil {
		return err
	}
	l.Stop(ctx, t.ID)

	return nil
}

func (s *Service) updateRunStatus(r *Run) error {
	// Only update if running as there is a race between manually stopping
	// a run and the run returning normally.
	return table.SchedRun.
		UpdateBuilder("status").
		If(qb.EqNamed("status", "from_status")).
		Query(s.session).
		BindStructMap(r, qb.M{"from_status": StatusRunning}).
		ExecRelease()
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
	s.suspended.Remove(clusterID.Bytes16())
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
	// We iterate over task types not use IN because it's not supported.
	// Cannot restrict clustering columns by IN relations when a collection is selected by the query.
	q := qb.Select(table.SchedTask.Name()).Where(qb.Eq("cluster_id"), qb.Eq("type")).Query(s.session)
	defer q.Release()

	hc := []TaskType{HealthCheckAlternatorTask, HealthCheckCQLTask, HealthCheckCQLTask}
	for _, tp := range hc {
		q.Bind(clusterID, tp)
		if err := forEachTaskWithQuery(q, f); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) forEachClusterTask(clusterID uuid.UUID, f func(t *Task) error) error {
	q := qb.Select(table.SchedTask.Name()).Where(qb.Eq("cluster_id")).Query(s.session).Bind(clusterID)
	defer q.Release()
	return forEachTaskWithQuery(q, f)
}

func (s *Service) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close cancels all tasks and waits for them to terminate.
func (s *Service) Close() {
	s.mu.Lock()
	s.closed = true
	v := make([]*scheduler.Scheduler, 0, len(s.scheduler))
	for _, l := range s.scheduler {
		v = append(v, l)
		l.Close()
	}
	s.mu.Unlock()

	for _, l := range v {
		l.Wait()
	}
}

func forEachTaskWithQuery(q *gocqlx.Queryx, f func(t *Task) error) error {
	var t Task
	iter := q.Iter()
	for iter.StructScan(&t) {
		if err := f(&t); err != nil {
			iter.Close()
			return err
		}
	}
	return iter.Close()
}
