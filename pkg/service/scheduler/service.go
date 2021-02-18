// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
	"github.com/scylladb/scylla-manager/pkg/service"
	"github.com/scylladb/scylla-manager/pkg/store"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// ClusterNameFunc returns name for a given ID.
type ClusterNameFunc func(ctx context.Context, clusterID uuid.UUID) (string, error)

// Service is a CRON alike scheduler. The scheduler is agnostic of logic it's
// executing, it can execute a Task of a given type provided that there is
// a Runner for that TaskType. Runners must be registered with SetRunner
// function and there can be only one Runner for a TaskType.
type Service struct {
	session     gocqlx.Session
	clusterName ClusterNameFunc
	logger      log.Logger
	wg          sync.WaitGroup
	mu          sync.Mutex
	runners     map[TaskType]Runner
	tasks       map[uuid.UUID]*trigger
	drawer      store.Store
	suspended   map[uuid.UUID]struct{}
	closing     bool
}

// Overridable knobs for tests
var (
	retryTaskWait = 10 * time.Minute
	stopTaskWait  = 60 * time.Second

	startDateThreshold          = -time.Hour
	suspendedStartDateThreshold = 8 * time.Hour
)

func NewService(session gocqlx.Session, drawer store.Store, clusterName ClusterNameFunc, logger log.Logger) (*Service, error) {
	if session.Session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if clusterName == nil {
		return nil, errors.New("invalid cluster name provider")
	}

	s := &Service{
		session:     session,
		clusterName: clusterName,
		logger:      logger,
		runners:     make(map[TaskType]Runner),
		tasks:       make(map[uuid.UUID]*trigger),
		drawer:      drawer,
		suspended:   make(map[uuid.UUID]struct{}),
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
			if err != service.ErrNotFound {
				return err
			}
		} else {
			s.suspended[c] = struct{}{}
		}
	}

	return nil
}

// SetRunner assigns runner for a given task type. All runners need to be
// registered prior to running the service. The registration is separated
// from constructor to loosen coupling between services.
func (s *Service) SetRunner(tp TaskType, r Runner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.runners[tp]; ok {
		s.logger.Info(context.Background(), "Overwriting runner", "task_type", tp)
	}
	s.runners[tp] = r
}

func (s *Service) mustRunner(tp TaskType) Runner {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, ok := s.runners[tp]
	if !ok {
		panic("no runner")
	}
	return r
}

// LoadTasks should be called on start it loads and schedules task from database.
func (s *Service) LoadTasks(ctx context.Context) error {
	s.logger.Info(ctx, "Loading tasks from database")

	now := timeutc.Now()
	err := s.forEachTask(func(t *Task) error {
		r, err := s.getLastRun(t)
		if err != nil && err != service.ErrNotFound {
			return errors.Wrap(err, "get last run")
		}
		if err := s.fixRunStatus(r, now); err != nil {
			return errors.Wrap(err, "fix run status")
		}
		if err := s.initMetrics(ctx, t, r); err != nil {
			return errors.Wrap(err, "init metrics")
		}
		s.schedule(ctx, t)

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
	var tasks []*Task
	q := qb.Select(table.SchedTask.Name()).Query(s.session)
	if err := q.SelectRelease(&tasks); err != nil {
		return err
	}

	for _, t := range tasks {
		if err := f(t); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) fixRunStatus(r *Run, now time.Time) error {
	if r == nil {
		return nil
	}

	if r.Status == StatusRunning {
		r.Status = StatusAborted
		r.Cause = "service stopped"
		r.EndTime = &now
		return s.putRun(r)
	}

	return nil
}

func (s *Service) initMetrics(ctx context.Context, t *Task, r *Run) error {
	clusterName, err := s.clusterName(ctx, t.ClusterID)
	if err != nil {
		return errors.Wrap(err, "get cluster name")
	}

	// Using Add(0) to not override existing values.

	// Init active_count
	taskActiveCount.With(prometheus.Labels{
		"cluster": clusterName,
		"type":    t.Type.String(),
		"task":    t.ID.String(),
	}).Add(0)

	// Init run_total and last_run_duration_seconds
	statuses := []Status{StatusNew, StatusRunning, StatusStopped, StatusDone, StatusError, StatusAborted}
	for _, s := range statuses {
		// If last run was aborted record it with taskRunTotal.
		// Otherwise it is impossible to notice ABORTED runs since they belong
		// to the last service run.
		// Counting all rows does not make sense either because the run rows
		// are TTLed, and after a while they would flatten out or could decrease.
		v := float64(0)
		if a := StatusAborted; s == a && r != nil && r.Status == a {
			v = 1
		}
		taskRunTotal.With(prometheus.Labels{
			"cluster": clusterName,
			"type":    t.Type.String(),
			"task":    t.ID.String(),
			"status":  s.String(),
		}).Add(v)

		taskLastRunDurationSeconds.With(prometheus.Labels{
			"cluster": clusterName,
			"task":    t.ID.String(),
			"type":    t.Type.String(),
			"status":  s.String(),
		}).Observe(0)
	}

	// Init last_success
	st, err := s.lastDoneRunStartTime(t, r)
	if err != nil {
		return err
	}
	taskLastSuccess.With(prometheus.Labels{
		"cluster": clusterName,
		"task":    t.ID.String(),
		"type":    t.Type.String(),
	}).Set(float64(st.Unix()))

	return nil
}

func (s *Service) lastDoneRunStartTime(t *Task, r *Run) (time.Time, error) {
	zero := time.Time{}

	// If there is no last run there is no previous run with some status.
	if r == nil {
		return zero, nil
	}

	if r.Status == StatusDone {
		return r.StartTime, nil
	}

	dr, err := s.GetLastRunWithStatus(t, StatusDone)
	if err != nil {
		if err == service.ErrNotFound {
			err = nil
		}
		return zero, errors.Wrap(err, "get last run with status done")
	}

	return dr.StartTime, nil
}

// schedule cancels any pending triggers for task and adds new trigger if needed.
// If task is running it will not be affected.
func (s *Service) schedule(ctx context.Context, t *Task) {
	// Calculate next activation time
	runs, err := s.GetLastRun(ctx, t, t.Sched.NumRetries+1)
	if err != nil {
		s.logger.Error(ctx, "Failed to get history of task", "task", t, "error", err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Cancel pending trigger for task, and skip if task is running
	if tg := s.tasks[t.ID]; !tg.CancelPending() && tg.State() == triggerRan {
		s.logger.Info(ctx, "Task not scheduled because it's already running, will be scheduled on task completion", "task", t)
		return
	}

	// Skip if service is suspended
	if !t.Type.IgnoreSuspended() {
		if _, ok := s.suspended[t.ClusterID]; ok {
			s.logger.Info(ctx, "Task not scheduled because service is suspended for cluster", "task", t)
			return
		}
	}

	// Skip if service is closing
	if s.closing {
		s.logger.Info(ctx, "Task not scheduled because service is closing", "task", t)
		return
	}

	// Skip if task is disabled
	if !t.Enabled {
		s.logger.Info(ctx, "Task not scheduled because it's disabled", "task", t)
		return
	}

	var (
		now        = timeutc.Now()
		activation = t.Sched.NextActivation(now, false, runs)
	)

	// Skip if not runnable
	if activation.IsZero() {
		s.logger.Info(ctx, "Task not scheduled due to lack of activation time", "task", t)
		return
	}

	tg := newTrigger(t)
	s.tasks[tg.TaskID] = tg
	s.logInfoOrDebug(t.Type)(ctx, "Task scheduled",
		"cluster_id", t.ClusterID,
		"task_type", t.Type,
		"task_id", t.ID,
		"run_id", tg.RunID,
		"activation", activation,
	)
	go s.runAfter(t, tg, activation.Sub(now))
}

func (s *Service) runAfter(t *Task, tg *trigger, after time.Duration) {
	timer := time.NewTimer(after)
	select {
	case <-timer.C:
		if tg.Run() {
			s.run(t, tg)
		}
	case <-tg.C:
		timer.Stop()
	}
}

func (s *Service) rescheduleIfNeeded(ctx context.Context, t *Task, run *Run) {
	// Try to update task from db
	var newTask Task
	q := table.SchedTask.GetQuery(s.session).BindStruct(t)
	if err := q.GetRelease(&newTask); err != nil {
		// Do not reschedule if deleted
		if err == service.ErrNotFound {
			return
		}
		// Otherwise log and recover
		s.logger.Error(ctx, "Failed to update task", "task", t, "error", err)
		newTask = *t
	}

	// Copy custom options
	newTask.opts = t.opts

	// Copy schedule for retries
	if !run.Status.isFinal() {
		newTask.Sched = t.Sched
	}

	// Remove task run before scheduling
	s.mu.Lock()
	delete(s.tasks, t.ID)
	s.mu.Unlock()

	// Don't schedule if done or error
	if newTask.Sched.Interval == 0 && run.Status.isFinal() {
		return
	}

	s.schedule(ctx, &newTask)
}

func (s *Service) run(t *Task, tg *trigger) {
	// register run in wait group so that it can be collected on service close
	s.wg.Add(1)
	defer s.wg.Done()

	// Create a new task run with the given ID
	run := t.newRun(tg.RunID)

	// Create a new context, the context lifecycle is managed by this function
	ctx := log.WithNewTraceID(context.Background())

	defer func() {
		clusterName, err := s.clusterName(ctx, t.ClusterID)
		if err != nil {
			clusterName = t.ClusterID.String()
		}

		taskLastRunDurationSeconds.With(prometheus.Labels{
			"cluster": clusterName,
			"task":    t.ID.String(),
			"type":    t.Type.String(),
			"status":  run.Status.String(),
		}).Observe(timeutc.Since(run.StartTime).Seconds())
	}()

	// Upon returning reschedule
	defer s.rescheduleIfNeeded(ctx, t, run)

	// Log task start and end
	s.logInfoOrDebug(t.Type)(ctx, "Task started",
		"cluster_id", t.ClusterID,
		"task_type", t.Type,
		"task_id", t.ID,
		"run_id", run.ID,
	)
	defer func() {
		if run.Status == StatusError {
			s.logger.Error(ctx, "Task ended with error",
				"cluster_id", t.ClusterID,
				"task_type", t.Type,
				"task_id", t.ID,
				"run_id", run.ID,
				"status", run.Status.String(),
				"cause", run.Cause,
			)
		} else {
			s.logInfoOrDebug(t.Type)(ctx, "Task ended",
				"cluster_id", t.ClusterID,
				"task_type", t.Type,
				"task_id", t.ID,
				"run_id", run.ID,
				"status", run.Status.String(),
			)
		}
	}()

	// Closing the context indicates that runner shall stop execution
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Ignore if cancelled
	select {
	case <-tg.C:
		return
	default:
		// continue
	}

	// Register the run
	run.Status = StatusRunning
	if err := s.putRun(run); err != nil {
		s.logger.Error(ctx, "Failed to register task run",
			"cluster_id", t.ClusterID,
			"task_type", t.Type,
			"task_id", t.ID,
			"run_id", run.ID,
			"error", err,
		)
		run.Status = StatusError
		return
	}

	// Get cluster name
	clusterName, err := s.clusterName(ctx, t.ClusterID)
	if err != nil {
		s.logger.Error(ctx, "Failed to get cluster name",
			"cluster_id", t.ClusterID,
			"task_type", t.Type,
			"task_id", t.ID,
			"run_id", run.ID,
			"error", err,
		)
		run.Status = StatusError
		return
	}

	// Update metrics
	taskActiveCount.With(prometheus.Labels{
		"cluster": clusterName,
		"type":    t.Type.String(),
		"task":    t.ID.String(),
	}).Inc()

	// Decorate task properties
	props := t.Properties
	for _, f := range t.opts {
		props = f(props)
	}

	// Run task async
	result := make(chan error, 1)

	go func() {
		result <- s.mustRunner(t.Type).Run(ctx, t.ClusterID, t.ID, run.ID, props)
	}()

	// Wait for run result
	var (
		taskStop        = tg.C
		taskStopTimeout <-chan time.Time
	)

wait:
	select {
	case <-taskStop:
		// Cancel the context
		cancel()
		// Set wait timer
		taskStopTimeout = time.After(stopTaskWait)
		// Skip this branch
		taskStop = nil
		// Re-enter select
		goto wait
	case err = <-result:
		// Continue to error handling
	case <-taskStopTimeout:
		// Check if we got a valid result
		select {
		case err = <-result:
			// Race won by task, continue to error handling
		default:
			s.logger.Error(ctx, "Task did not stop in time",
				"cluster_id", t.ClusterID,
				"task_type", t.Type,
				"task_id", t.ID,
				"run_id", run.ID,
				"wait", stopTaskWait,
			)
			err = errors.Errorf("stop task in %s", stopTaskWait)
		}
	}

	now := timeutc.Now()
	run.EndTime = &now

	run.Status = StatusDone
	if err != nil {
		if ctx.Err() != nil && strings.Contains(err.Error(), context.Canceled.Error()) {
			run.Status = StatusStopped
		} else {
			run.Status = StatusError
			run.Cause = err.Error()
		}
	}

	// If closing override StatusStopped to StatusAborted
	if run.Status == StatusStopped && s.isClosing() {
		run.Status = StatusAborted
		run.Cause = "service stopped"
	}

	s.putRunLogError(ctx, run)

	// Update metrics
	taskActiveCount.With(prometheus.Labels{
		"cluster": clusterName,
		"type":    t.Type.String(),
		"task":    t.ID.String(),
	}).Dec()

	taskRunTotal.With(prometheus.Labels{
		"cluster": clusterName,
		"type":    t.Type.String(),
		"task":    t.ID.String(),
		"status":  run.Status.String(),
	}).Inc()

	if run.Status == StatusDone {
		taskLastSuccess.With(prometheus.Labels{
			"cluster": clusterName,
			"type":    t.Type.String(),
			"task":    t.ID.String(),
		}).Set(float64(run.StartTime.Unix()))
	}
}

// StartTask starts execution of a task immediately, regardless of the task's schedule.
func (s *Service) StartTask(ctx context.Context, t *Task, opts ...Opt) error {
	s.logger.Debug(ctx, "StartTask", "task", t)

	if err := t.Validate(); err != nil {
		return err
	}
	t.opts = opts

	s.mu.Lock()
	defer s.mu.Unlock()

	// Prevent starting in suspended mode
	if !t.Type.IgnoreSuspended() && s.isSuspendedLocked(t.ClusterID) {
		return service.ErrValidate(errors.New("cluster is suspended"))
	}

	// Prevent starting an already running task
	tg := s.tasks[t.ID]
	if tg != nil {
		if s.taskIsRunning(ctx, t, tg.RunID) {
			return errors.New("task already running")
		}
	}

	s.cancelLocked(ctx, t.ID)

	tg = newTrigger(t)
	s.tasks[tg.TaskID] = tg
	s.logger.Info(ctx, "Force task execution",
		"cluster_id", tg.ClusterID,
		"task_type", tg.Type,
		"task_id", tg.TaskID,
		"run_id", tg.RunID,
	)
	if tg.Run() {
		go s.run(t, tg)
	}
	return nil
}

func (s *Service) taskIsRunning(ctx context.Context, t *Task, runID uuid.UUID) bool {
	run, err := s.GetRun(ctx, t, runID)
	if err != nil {
		return false
	}
	return run.Status == StatusRunning
}

// StopTask stops task execution of immediately, task is rescheduled according
// to its run interval.
func (s *Service) StopTask(ctx context.Context, t *Task) error {
	s.logger.Debug(ctx, "StopTask", "task", t)
	if err := t.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	s.cancelLocked(ctx, t.ID)
	s.mu.Unlock()

	return nil
}

func (s *Service) cancelLocked(ctx context.Context, taskID uuid.UUID) {
	if tg := s.tasks[taskID]; tg.Cancel() {
		s.logInfoOrDebug(tg.Type)(ctx, "Task execution canceled",
			"cluster_id", tg.ClusterID,
			"task_type", tg.Type,
			"task_id", tg.TaskID,
			"run_id", tg.RunID,
		)
		delete(s.tasks, taskID)
	}
}

// IsSuspended returns true iff cluster is suspended.
func (s *Service) IsSuspended(ctx context.Context, clusterID uuid.UUID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isSuspendedLocked(clusterID)
}

func (s *Service) isSuspendedLocked(clusterID uuid.UUID) bool {
	_, ok := s.suspended[clusterID]
	return ok
}

// Suspend stops scheduler for a given cluster. Running tasks will be stopped.
// Scheduled task executions will be canceled.
// Scheduler can be later resumed, see `Resume` function.
func (s *Service) Suspend(ctx context.Context, clusterID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ignore if already suspended
	if _, ok := s.suspended[clusterID]; ok {
		return service.ErrValidate(errors.New("cluster already suspended"))
	}

	// Mark service as suspended
	s.suspended[clusterID] = struct{}{}
	s.logger.Info(ctx, "Service suspended, cancelling tasks...")

	// Cancel tasks
	si := &suspendInfo{
		ClusterID: clusterID,
	}
	for _, tg := range s.tasks {
		if tg.ClusterID != clusterID {
			continue
		}
		if tg.Type.IgnoreSuspended() {
			continue
		}

		if tg.CancelPending() {
			s.logger.Info(ctx, "Canceled scheduled task ",
				"cluster_id", tg.ClusterID,
				"task_type", tg.Type,
				"task_id", tg.TaskID,
				"run_id", tg.RunID,
			)
			si.PendingTasks = append(si.PendingTasks, tg.TaskID)
		} else {
			if tg.Cancel() {
				s.logger.Info(ctx, "Canceled running task",
					"cluster_id", tg.ClusterID,
					"task_type", tg.Type,
					"task_id", tg.TaskID,
					"run_id", tg.RunID,
				)
			} else {
				// This should never happen
				s.logger.Error(ctx, "Could not cancel task", "task", tg.TaskID)
			}
			si.RunningTask = append(si.RunningTask, tg.TaskID)
		}
	}
	s.logger.Info(ctx, "Canceled tasks", "count", len(si.PendingTasks)+len(si.RunningTask))

	// Persist canceled tasks info
	if err := s.drawer.Put(si); err != nil {
		return errors.Wrap(err, "save canceled tasks")
	}

	return nil
}

// Resume resumes scheduler for a suspended cluster.
func (s *Service) Resume(ctx context.Context, clusterID uuid.UUID, startTasks bool) error {
	s.mu.Lock()

	if _, ok := s.suspended[clusterID]; !ok {
		s.mu.Unlock()
		return service.ErrValidate(errors.New("cluster is not suspended"))
	}

	// Resume early, if something goes wrong scheduler would still be usable...
	delete(s.suspended, clusterID)
	s.logger.Info(ctx, "Service resumed, rescheduling tasks...")

	// Get canceled tasks
	si := &suspendInfo{ClusterID: clusterID}
	if err := s.drawer.Get(si); err != nil {
		if err == service.ErrNotFound {
			s.logger.Error(ctx, "Expected canceled tasks got none")
		} else {
			s.mu.Unlock()
			return errors.Wrap(err, "get canceled tasks")
		}
	}
	// Delete canceled tasks
	if err := s.drawer.Delete(si); err != nil {
		s.logger.Error(ctx, "Failed to delete canceled tasks", "error", err)
	}

	// Release lock and schedule tasks
	s.mu.Unlock()

	s.forEachTask(func(t *Task) error { // nolint: errcheck
		// Reschedule pending tasks
		for _, id := range si.PendingTasks {
			if t.ID == id {
				s.schedule(ctx, t)
				return nil
			}
		}
		// Start or reschedule running tasks
		for _, id := range si.RunningTask {
			if t.ID == id {
				if startTasks {
					if err := s.StartTask(ctx, t); err != nil {
						s.logger.Error(ctx, "Failed to start task, falling back to schedule", "task", t)
						s.schedule(ctx, t)
					}
				} else {
					s.schedule(ctx, t)
				}
				return nil
			}
		}
		return nil
	})

	return nil
}

// GetTask returns a task based on ID or name. If nothing was found
// scylla-manager.ErrNotFound is returned.
func (s *Service) GetTask(ctx context.Context, clusterID uuid.UUID, tp TaskType, idOrName string) (*Task, error) {
	if id, err := uuid.Parse(idOrName); err == nil {
		return s.GetTaskByID(ctx, clusterID, tp, id)
	}

	return s.GetTaskByName(ctx, clusterID, tp, idOrName)
}

// GetTaskByID returns a task based on ID and type. If nothing was found
// scylla-manager.ErrNotFound is returned.
func (s *Service) GetTaskByID(ctx context.Context, clusterID uuid.UUID, tp TaskType, id uuid.UUID) (*Task, error) {
	s.logger.Debug(ctx, "GetTaskByID", "cluster_id", clusterID, "id", id)

	q := table.SchedTask.GetQuery(s.session).BindMap(qb.M{
		"cluster_id": clusterID,
		"type":       tp,
		"id":         id,
	})
	defer q.Release()

	if q.Err() != nil {
		return nil, q.Err()
	}

	var t Task
	if err := q.Get(&t); err != nil {
		return nil, err
	}

	return &t, nil
}

// GetTaskByName returns a task based on type and name. If nothing was found
// scylla-manager.ErrNotFound is returned.
func (s *Service) GetTaskByName(ctx context.Context, clusterID uuid.UUID, tp TaskType, name string) (*Task, error) {
	s.logger.Debug(ctx, "GetTaskByName", "cluster_id", clusterID, "name", name)

	if name == "" {
		return nil, errors.New("missing task")
	}

	b := qb.Select(table.SchedTask.Name()).Where(qb.Eq("cluster_id"), qb.Eq("type"))
	m := qb.M{
		"cluster_id": clusterID,
		"type":       tp,
	}

	q := b.Query(s.session).BindMap(m)
	defer q.Release()

	if q.Err() != nil {
		return nil, q.Err()
	}

	var tasks []*Task
	if err := q.Select(&tasks); err != nil {
		return nil, err
	}

	filtered := tasks[:0]
	for _, t := range tasks {
		if t.Name == name {
			filtered = append(filtered, t)
		}
	}
	for i := len(filtered); i < len(tasks); i++ {
		tasks[i] = nil
	}
	tasks = filtered

	switch len(tasks) {
	case 0:
		return nil, service.ErrNotFound
	case 1:
		return tasks[0], nil
	default:
		return nil, errors.Errorf("multiple tasks share the same name %q", name)
	}
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

// PutTask upserts a task, the task instance must pass Validate() checks.
func (s *Service) PutTask(ctx context.Context, t *Task) error {
	s.logger.Debug(ctx, "PutTask", "task", t)

	create := false
	if t != nil && t.ID == uuid.Nil {
		var err error
		if t.ID, err = uuid.NewRandom(); err != nil {
			return errors.Wrap(err, "couldn't generate random UUID for task")
		}
		create = true
	}

	if err := t.Validate(); err != nil {
		return err
	}

	if create {
		now := timeutc.Now()
		// Prevent scheduling task with too old start dates
		if t.Sched.StartDate.Before(now.Add(startDateThreshold)) {
			return service.ErrValidate(errors.New("start date in the past"))
		}
		// Prevent starting in suspended mode
		if t.Sched.StartDate.Before(now.Add(suspendedStartDateThreshold)) &&
			!t.Type.IgnoreSuspended() && s.IsSuspended(ctx, t.ClusterID) {
			return service.ErrValidate(errors.Errorf("cluster is suspended, scheduling tasks to start within next %s is not allowed", suspendedStartDateThreshold))
		}
	}

	if err := table.SchedTask.InsertQuery(s.session).BindStruct(t).ExecRelease(); err != nil {
		return err
	}

	s.schedule(ctx, t)

	if create {
		if err := s.initMetrics(ctx, t, nil); err != nil {
			return errors.Wrap(err, "init metrics")
		}
	}

	return nil
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

	s.logger.Info(ctx, "Task deleted",
		"cluster_id", t.ClusterID,
		"task_type", t.Type,
		"task_id", t.ID,
	)

	s.mu.Lock()
	s.cancelLocked(ctx, t.ID)
	delete(s.tasks, t.ID)
	s.mu.Unlock()

	return nil
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

// GetRun returns a run based on ID. If nothing was found scylla-manager.ErrNotFound
// is returned.
func (s *Service) GetRun(ctx context.Context, t *Task, runID uuid.UUID) (*Run, error) {
	s.logger.Debug(ctx, "GetRun", "task", t, "run_id", runID)

	// Validate the task
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

	if err := q.GetRelease(r); err != nil {
		return nil, err
	}

	return r, nil
}

// GetLastRunWithStatus returns most recent run with a given status.
func (s *Service) GetLastRunWithStatus(t *Task, status Status) (*Run, error) {
	run := t.newRun(uuid.Nil)
	run.Status = status

	q := table.SchedRun.SelectBuilder().Where(qb.Eq("status")).AllowFiltering().Limit(1).Query(s.session).BindStruct(run)
	return run, q.GetRelease(run)
}

// GetLastRun returns at most limit recent runs of the task.
func (s *Service) GetLastRun(ctx context.Context, t *Task, limit int) ([]*Run, error) {
	s.logger.Debug(ctx, "GetLastRun", "task", t, "limit", limit)

	// Validate the task
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, service.ErrValidate(errors.New("limit must be > 0"))
	}

	q := qb.Select(table.SchedRun.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("type"),
		qb.Eq("task_id"),
	).Limit(uint(limit)).Query(s.session).BindMap(qb.M{
		"cluster_id": t.ClusterID,
		"type":       t.Type,
		"task_id":    t.ID,
	})

	var runs []*Run
	return runs, q.SelectRelease(&runs)
}

func (s *Service) getLastRun(t *Task) (*Run, error) {
	q := qb.Select(table.SchedRun.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("type"),
		qb.Eq("task_id"),
	).Limit(1).Query(s.session).BindMap(qb.M{
		"cluster_id": t.ClusterID,
		"type":       t.Type,
		"task_id":    t.ID,
	})

	var run Run
	return &run, q.GetRelease(&run)
}

func (s *Service) putRun(r *Run) error {
	q := table.SchedRun.InsertQuery(s.session).BindStruct(r)
	return q.ExecRelease()
}

// putRunLogError executes putRun and consumes the error.
func (s *Service) putRunLogError(ctx context.Context, r *Run) {
	if err := s.putRun(r); err != nil {
		s.logger.Error(ctx, "Cannot update the run",
			"run", &r,
			"error", err,
		)
	}
}

// Close cancels all tasks and waits for them to terminate.
func (s *Service) Close() {
	ctx := context.Background()
	s.logger.Info(ctx, "Closing scheduler")

	s.mu.Lock()
	// enter closing state
	s.closing = true

	// Cancel all tasks
	for _, tg := range s.tasks {
		if tg.Cancel() {
			s.logInfoOrDebug(tg.Type)(ctx, "Task execution canceled",
				"cluster_id", tg.ClusterID,
				"task_type", tg.Type,
				"task_id", tg.TaskID,
				"run_id", tg.RunID,
			)
		}
	}
	s.tasks = nil
	s.mu.Unlock()

	// Wait for tasks to stop
	s.wg.Wait()
	s.logger.Info(ctx, "All tasks ended")
}

func (s *Service) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}
