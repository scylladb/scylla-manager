// Copyright (C) 2017 ScyllaDB

package sched

import (
	"context"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/uuid"
)

// Service schedules tasks.
type Service struct {
	session   *gocql.Session
	runners   map[TaskType]runner.Runner
	runnersMu sync.Mutex
	logger    log.Logger

	cronCtx  context.Context
	tasks    map[uuid.UUID]cancelableTrigger
	wg       sync.WaitGroup
	taskLock sync.Mutex
}

type cancelableTrigger struct {
	timer  *time.Timer
	cancel func()
	done   chan struct{}
}

// overridable knobs for tests
var (
	timeNow             = time.Now
	retryTaskWait       = 10 * time.Minute
	taskStartNowSlack   = 10 * time.Second
	monitorTaskInterval = time.Second

	reschedTaskDone = func(*Task) {}
)

// RetryFor converts task best before date into number of retries.
func RetryFor(d time.Duration) int {
	return int(int64(d) / int64(retryTaskWait))
}

// NewService creates a new service instance.
func NewService(session *gocql.Session, l log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	return &Service{
		session: session,
		logger:  l,

		cronCtx: log.WithTraceID(context.Background()),
		runners: make(map[TaskType]runner.Runner),
		tasks:   make(map[uuid.UUID]cancelableTrigger),
	}, nil
}

// LoadTasks should be called on start. It attaches to running tasks if there are such,
// marking no-longer running ones as stopped. It then proceeds to schedule future tasks.
func (s *Service) LoadTasks(ctx context.Context) error {
	s.logger.Debug(ctx, "LoadTasks")

	stmt, _ := qb.Select(schema.SchedTask.Name).ToCql()
	iter := gocqlx.Iter(s.session.Query(stmt).WithContext(ctx))

	defer func() {
		iter.Close()
		iter.ReleaseQuery()
	}()

	now := timeNow().UTC()

	var t Task
	for iter.StructScan(&t) {
		if !t.Enabled {
			continue
		}

		runs, err := s.GetLastRunN(ctx, &t, 1)
		if err != nil {
			s.logger.Error(ctx, "failed to get last run of task", "Task", t, "error", err)
			continue
		}

		if n := len(runs); n > 0 {
			r := runs[n-1]

			switch r.Status {
			case runner.StatusStarting, runner.StatusRunning, runner.StatusStopping:
				curStatus, err := s.taskRunner(&t).Status(ctx, t.ClusterID, r.ID, t.Properties)
				if err != nil {
					s.logger.Error(ctx, "failed to get task status", "task", t, "run", r, "error", err)
					continue
				}
				switch curStatus {
				case runner.StatusRunning, runner.StatusStopping:
					t := t
					s.attachTask(ctx, &t, r)
					continue
				case runner.StatusStopped, runner.StatusError:
					r.Status = curStatus
					r.EndTime = now
				default:
					s.logger.Error(ctx, "unexpected task status", "status", curStatus, "task", t, "run", r)
					continue
				}
				if err := s.putRun(ctx, r); err != nil {
					s.logger.Error(ctx, "failed to write run", "run", r)
					return err
				}
			}
		}

		if t.Sched.IntervalDays > 0 || t.Sched.StartDate.After(now) {
			t := t
			s.schedTask(ctx, now, &t)
		}
	}

	return iter.Close()
}

func (s *Service) taskRunner(t *Task) runner.Runner {
	s.runnersMu.Lock()
	defer s.runnersMu.Unlock()

	r := s.runners[t.Type]
	if r != nil {
		return r
	}

	return nilRunner{}
}

// SetRunner assigns a given runner for a given task type.
func (s *Service) SetRunner(tp TaskType, r runner.Runner) {
	s.runnersMu.Lock()
	defer s.runnersMu.Unlock()
	s.runners[tp] = r
}

func (s *Service) schedTask(ctx context.Context, now time.Time, t *Task) {
	runs, err := s.GetLastRunN(ctx, t, t.Sched.NumRetries)
	if err != nil {
		s.logger.Error(ctx, "failed to get history of task", "Task", t, "error", err)
		return
	}
	activation := t.Sched.nextActivation(now, runs)
	if activation.IsZero() {
		s.logger.Debug(ctx, "schedTask no activation", "task", t)
		return
	}
	if !now.Before(activation) {
		s.logger.Error(ctx, "schedTask task in the past", "now", now, "activation", activation, "task", t)
		return
	}

	s.logger.Debug(ctx, "schedTask", "activation", activation, "task", t)
	triggerCtx, cancel := context.WithCancel(s.cronCtx)
	doneCh := make(chan struct{})
	s.taskLock.Lock()
	timer := time.AfterFunc(activation.Sub(now), func() { s.execTrigger(triggerCtx, t, doneCh) })
	s.tasks[t.ID] = cancelableTrigger{
		timer: timer,
		cancel: func() {
			cancel()
			if timer.Stop() {
				close(doneCh)
			}
		},
		done: doneCh,
	}
	s.taskLock.Unlock()
}

func (s *Service) attachTask(ctx context.Context, t *Task, run *Run) {
	s.logger.Debug(ctx, "attachTask", "task", t, "run", run)
	triggerCtx, cancel := context.WithCancel(s.cronCtx)
	doneCh := make(chan struct{})
	s.taskLock.Lock()
	s.tasks[t.ID] = cancelableTrigger{
		cancel: cancel,
		done:   doneCh,
	}
	s.taskLock.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.reschedTask(triggerCtx, t, doneCh)

		run.Status = runner.StatusRunning
		if err := s.putRun(ctx, run); err != nil {
			s.logger.Error(ctx, "failed to write run", "run", run)
			return
		}

		s.waitTask(ctx, t, run)
	}()
}

func (s *Service) reschedTask(ctx context.Context, t *Task, done chan struct{}) {
	defer reschedTaskDone(t)
	defer close(done)
	defer s.wg.Done()
	s.taskLock.Lock()
	if prevTrigger, ok := s.tasks[t.ID]; ok {
		delete(s.tasks, t.ID)
		defer prevTrigger.cancel()
	}
	s.taskLock.Unlock()

	if ctx.Err() != nil {
		s.logger.Debug(ctx, "task canceled, not re-scheduling", "Task", t)
		return
	}
	if t.Sched.IntervalDays == 0 {
		s.logger.Debug(ctx, "one-shot task, not re-scheduling", "Task", t)
		return
	}
	s.schedTask(ctx, timeNow().UTC(), t)
}

func (s *Service) execTrigger(ctx context.Context, t *Task, done chan struct{}) {
	s.wg.Add(1)
	defer s.reschedTask(ctx, t, done)

	now := timeNow().UTC()
	run := &Run{
		ID:        uuid.NewTime(),
		Type:      t.Type,
		ClusterID: t.ClusterID,
		TaskID:    t.ID,
		Status:    runner.StatusStarting,
		StartTime: now,
	}
	s.logger.Debug(ctx, "execTrigger", "now", now, "task", t, "run", run)

	if err := s.putRun(ctx, run); err != nil {
		s.logger.Error(ctx, "failed to write run", "run", run)
		return
	}

	if err := s.taskRunner(t).Run(ctx, run.ClusterID, run.ID, t.Properties); err != nil {
		s.logger.Info(ctx, "failed to start task", "Task", t, "run ID", run.ID, "error", err)
		run.Status = runner.StatusError
		run.EndTime = now
		if err := s.putRun(ctx, run); err != nil {
			s.logger.Error(ctx, "failed to write run", "run", run)
		}
		return
	}

	run.Status = runner.StatusRunning
	if err := s.putRun(ctx, run); err != nil {
		s.logger.Error(ctx, "failed to write run", "run", run)
		return
	}

	s.waitTask(ctx, t, run)
}

func (s *Service) waitTask(ctx context.Context, t *Task, run *Run) {
	ticker := time.NewTicker(monitorTaskInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			ctx = context.Background()
			if err := s.taskRunner(t).Stop(ctx, run.ClusterID, run.ID, t.Properties); err != nil {
				s.logger.Info(ctx, "failed to stop task", "Task", t, "run ID", run.ID, "error", err)
				continue
			}
			run.Status = runner.StatusStopping
			if err := s.putRun(ctx, run); err != nil {
				s.logger.Error(ctx, "failed to write run", "run", run)
			}

		case now := <-ticker.C:
			curStatus, err := s.taskRunner(t).Status(ctx, t.ClusterID, run.ID, t.Properties)
			if err != nil {
				s.logger.Error(ctx, "failed to get task status", "task", t, "run", run)
				continue
			}
			switch curStatus {
			case runner.StatusStopped, runner.StatusError:
				run.Status = curStatus
				run.EndTime = now
				if err := s.putRun(ctx, run); err != nil {
					s.logger.Error(ctx, "failed to write run", "run", run)
				}
				return
			}
		}
	}
}

// StartTask starts execution of a task immediately, regardless of the task's schedule.
func (s *Service) StartTask(ctx context.Context, t *Task) error {
	s.logger.Debug(ctx, "StartTask", "Task", t)
	if t == nil {
		return errors.New("nil task")
	}

	s.cancelTask(t)
	triggerCtx, cancel := context.WithCancel(s.cronCtx)
	doneCh := make(chan struct{})
	s.taskLock.Lock()
	s.tasks[t.ID] = cancelableTrigger{
		cancel: cancel,
		done:   doneCh,
	}
	go s.execTrigger(triggerCtx, t, doneCh)
	s.taskLock.Unlock()
	return nil
}

func (s *Service) cancelTask(t *Task) {
	var doneCh chan struct{}
	s.taskLock.Lock()
	if trigger, ok := s.tasks[t.ID]; ok {
		delete(s.tasks, t.ID)
		doneCh = trigger.done
		trigger.cancel()
	}
	s.taskLock.Unlock()
	if doneCh != nil {
		<-doneCh
	}
}

// StopTask stops task execution of immediately, regardless and re-schedule if Enabled.
func (s *Service) StopTask(ctx context.Context, t *Task) error {
	s.logger.Debug(ctx, "StopTask", "Task", t)
	if t == nil {
		return errors.New("nil task")
	}

	s.cancelTask(t)
	if t.Enabled {
		s.schedTask(ctx, timeNow().UTC(), t)
	}
	return nil
}

// GetTask returns a task based on ID or name. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetTask(ctx context.Context, clusterID uuid.UUID, tp TaskType, idOrName string) (*Task, error) {
	var id uuid.UUID

	if err := id.UnmarshalText([]byte(idOrName)); err == nil {
		return s.GetTaskByID(ctx, clusterID, tp, id)
	}

	return s.GetTaskByName(ctx, clusterID, tp, idOrName)
}

// GetTaskByID returns a task based on ID and type. If nothing was found
// mermaid.ErrNotFound is returned.
func (s *Service) GetTaskByID(ctx context.Context, clusterID uuid.UUID, tp TaskType, id uuid.UUID) (*Task, error) {
	s.logger.Debug(ctx, "GetTaskByID", "cluster_id", clusterID, "id", id)

	stmt, names := schema.SchedTask.Get()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"type":       tp,
		"id":         id,
	})
	if q.Err() != nil {
		return nil, q.Err()
	}

	var t Task
	if err := gocqlx.Get(&t, q.Query); err != nil {
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

	b := qb.Select(schema.SchedTask.Name).Where(qb.Eq("cluster_id"), qb.Eq("type"))
	m := qb.M{
		"cluster_id": clusterID,
		"type":       tp,
	}

	stmt, names := b.ToCql()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(m)

	if q.Err() != nil {
		return nil, q.Err()
	}

	var tasks []*Task
	if err := gocqlx.Select(&tasks, q.Query); err != nil {
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
		return nil, mermaid.ErrNotFound
	case 1:
		return tasks[0], nil
	default:
		return nil, errors.Errorf("multiple tasks share the same name %q", name)
	}
}

// PutTask upserts a task, the task instance must pass Validate() checks.
func (s *Service) PutTask(ctx context.Context, t *Task) error {
	s.logger.Debug(ctx, "PutTask", "Task", t)
	if t == nil {
		return errors.New("nil task")
	}

	if t.ID == uuid.Nil {
		var err error
		if t.ID, err = uuid.NewRandom(); err != nil {
			return errors.Wrap(err, "couldn't generate random UUID for Task")
		}
	}

	if err := t.Validate(); err != nil {
		return err
	}

	stmt, names := schema.SchedTask.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(t)

	if err := q.ExecRelease(); err != nil {
		return err
	}
	s.cancelTask(t)
	if t.Enabled {
		s.schedTask(ctx, timeNow().UTC(), t)
	}
	return nil
}

// DeleteTask removes a task based on ID.
func (s *Service) DeleteTask(ctx context.Context, t *Task) error {
	s.logger.Debug(ctx, "DeleteTask", "cluster_id", t.ClusterID, "id", t.ID, "type", t.Type)

	stmt, names := schema.SchedTask.Delete()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": t.ClusterID,
		"type":       t.Type,
		"id":         t.ID,
	})

	err := q.ExecRelease()
	if err != nil {
		return err
	}
	s.cancelTask(t)
	return nil
}

// ListTasks returns all the tasks stored, tp is optional if empty all task
// types will loaded.
func (s *Service) ListTasks(ctx context.Context, clusterID uuid.UUID, tp TaskType) ([]*Task, error) {
	s.logger.Debug(ctx, "ListTasks", "cluster_id", clusterID, "task_type", tp)

	b := qb.Select(schema.SchedTask.Name).Where(qb.Eq("cluster_id"))
	m := qb.M{
		"cluster_id": clusterID,
	}

	if tp != "" {
		b.Where(qb.Eq("type"))
		m["type"] = tp
	}

	stmt, names := b.ToCql()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(m)

	if q.Err() != nil {
		return nil, q.Err()
	}

	var tasks []*Task
	err := gocqlx.Select(&tasks, q.Query)
	return tasks, err
}

// GetLastRunN returns at most N recent runs of the task.
// If N is <= -1, return all runs. N == 0 returns an empty slice.
func (s *Service) GetLastRunN(ctx context.Context, t *Task, n int) ([]*Run, error) {
	s.logger.Debug(ctx, "GetLastRunN", "task", t, "n", n)

	// validate the task
	if err := t.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid task")
	}

	if n == 0 {
		return nil, nil
	}
	b := qb.Select(schema.SchedRun.Name).Where(
		qb.Eq("cluster_id"),
		qb.Eq("type"),
		qb.Eq("task_id"),
	)
	if n > 0 {
		b.Limit(uint(n))
	}
	stmt, names := b.ToCql()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": t.ClusterID,
		"type":       t.Type,
		"task_id":    t.ID,
	})
	if err := q.Err(); err != nil {
		return nil, err
	}

	var r []*Run
	if err := gocqlx.Select(&r, q.Query); err != nil && err != mermaid.ErrNotFound {
		return nil, err
	}

	return r, nil
}

func (s *Service) putRun(ctx context.Context, r *Run) error {
	s.logger.Debug(ctx, "PutRun", "run", r)

	stmt, names := schema.SchedRun.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(r)

	return q.ExecRelease()
}

// Close cancels all future task timers and waits for all running goroutines to terminate.
func (s *Service) Close(ctx context.Context) {
	s.logger.Info(ctx, "Waiting for tasks to finish")
	s.taskLock.Lock()
	for _, t := range s.tasks {
		t.cancel()
	}
	s.tasks = nil
	s.taskLock.Unlock()
	s.wg.Wait()
}

var errNilRunnerUsed = errors.New("task type maps to nil runner")

type nilRunner struct{}

func (nilRunner) Run(ctx context.Context, clusterID, runID uuid.UUID, props runner.TaskProperties) error {
	return errNilRunnerUsed
}

func (nilRunner) Stop(ctx context.Context, clusterID, runID uuid.UUID, props runner.TaskProperties) error {
	return errNilRunnerUsed
}

func (nilRunner) Status(ctx context.Context, clusterID, runID uuid.UUID, props runner.TaskProperties) (runner.Status, error) {
	return "", errNilRunnerUsed
}
