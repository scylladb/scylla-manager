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
	"github.com/scylladb/mermaid/pkg/schema/table"
	"github.com/scylladb/mermaid/pkg/service"
	"github.com/scylladb/mermaid/pkg/util/timeutc"
	"github.com/scylladb/mermaid/pkg/util/uuid"
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

	mu      sync.Mutex
	runners map[TaskType]Runner
	tasks   map[uuid.UUID]*trigger
	closing bool
	wg      sync.WaitGroup
}

// overridable knobs for tests
var (
	retryTaskWait     = 10 * time.Minute
	stopTaskWait      = 60 * time.Second
	startTaskNowSlack = 10 * time.Second
)

func NewService(session gocqlx.Session, clusterName ClusterNameFunc, logger log.Logger) (*Service, error) {
	if session.Session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if clusterName == nil {
		return nil, errors.New("invalid cluster name provider")
	}

	return &Service{
		session:     session,
		clusterName: clusterName,
		logger:      logger,
		runners:     make(map[TaskType]Runner),
		tasks:       make(map[uuid.UUID]*trigger),
	}, nil
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

	var tasks []*Task
	q := qb.Select(table.SchedTask.Name()).Query(s.session)
	if err := q.SelectRelease(&tasks); err != nil {
		return err
	}

	for _, t := range tasks {
		if err := s.fixRunStatus(ctx, t); err != nil {
			return errors.Wrap(err, "fix run status")
		}
	}

	for _, t := range tasks {
		s.schedule(ctx, t)
	}
	s.logger.Info(ctx, "All tasks scheduled")

	return nil
}

func (s *Service) fixRunStatus(ctx context.Context, t *Task) error {
	runs, err := s.GetLastRun(ctx, t, 1)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		return nil
	}
	r := runs[0]
	if r.Status != StatusRunning {
		return nil
	}

	r.Status = StatusAborted
	r.Cause = "service stopped"
	return s.putRun(r)
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
		activation = t.Sched.NextActivation(now, runs)
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

	// Prevent starting an already running task.
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

// GetTask returns a task based on ID or name. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetTask(ctx context.Context, clusterID uuid.UUID, tp TaskType, idOrName string) (*Task, error) {
	if id, err := uuid.Parse(idOrName); err == nil {
		return s.GetTaskByID(ctx, clusterID, tp, id)
	}

	return s.GetTaskByName(ctx, clusterID, tp, idOrName)
}

// GetTaskByID returns a task based on ID and type. If nothing was found
// mermaid.ErrNotFound is returned.
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
// mermaid.ErrNotFound is returned.
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
		if t.Sched.StartDate.Before(timeutc.Now()) {
			return service.ErrValidate(errors.New("start date in the past"))
		}
	}

	if err := table.SchedTask.InsertQuery(s.session).BindStruct(t).ExecRelease(); err != nil {
		return err
	}

	s.schedule(ctx, t)

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

// GetRun returns a run based on ID. If nothing was found mermaid.ErrNotFound
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

	b := qb.Select(table.SchedRun.Name()).Where(
		qb.Eq("cluster_id"),
		qb.Eq("type"),
		qb.Eq("task_id"),
	)
	b.Limit(uint(limit))

	q := b.Query(s.session).BindMap(qb.M{
		"cluster_id": t.ClusterID,
		"type":       t.Type,
		"task_id":    t.ID,
	})
	defer q.Release()

	if err := q.Err(); err != nil {
		return nil, err
	}

	var r []*Run
	if err := q.Select(&r); err != nil && err != service.ErrNotFound {
		return nil, err
	}

	return r, nil
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
