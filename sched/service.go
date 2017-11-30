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
	session *gocql.Session
	runners map[TaskType]runner.Runner
	logger  log.Logger

	cronCtx  context.Context
	tasks    map[uuid.UUID]cancelableTrigger
	wg       sync.WaitGroup
	taskLock sync.Mutex
}

type cancelableTrigger struct {
	timer  *time.Timer
	cancel func()
}

// overridable knobs for tests
var (
	timeNow             = time.Now
	retryTaskWait       = 10 * time.Minute
	monitorTaskInterval = time.Second

	reschedTaskDone = func(*Task) {}
)

// NewService creates a new service instance.
func NewService(session *gocql.Session, runners map[TaskType]runner.Runner, l log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	return &Service{
		session: session,
		logger:  l,
		cronCtx: log.WithTraceID(context.Background()),
		tasks:   make(map[uuid.UUID]cancelableTrigger),
		runners: runners,
	}, nil
}

// LoadTasks should be called on startr. It attaches to running tasks if there are such,
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
				curStatus, err := s.taskRunner(&t).TaskStatus(ctx, t.ClusterID, r.ID, t.Properties)
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

		if t.Sched.Repeat || t.Sched.NumRetries > 0 || t.Sched.StartDate.After(now) {
			t := t
			s.schedTask(ctx, now, &t)
		}
	}

	return iter.Close()
}

func (s *Service) taskRunner(t *Task) runner.Runner {
	r := s.runners[t.Type]
	if r != nil {
		return r
	}

	return nilRunner{}
}

func (s *Service) schedTask(ctx context.Context, now time.Time, t *Task) {
	runs, err := s.GetLastRunN(ctx, t, t.Sched.NumRetries)
	if err != nil {
		s.logger.Error(ctx, "failed to get history of task", "Task", t, "error", err)
		return
	}
	activation := t.Sched.nextActivation(now, runs)
	s.logger.Debug(ctx, "schedTask", "activation", activation, "task", t)
	triggerCtx, cancel := context.WithCancel(s.cronCtx)

	if !now.Before(activation) {
		s.logger.Error(ctx, "schedTask task in the past", "now", now, "activation", activation, "task", t)
		cancel()
		return
	}

	s.taskLock.Lock()
	timer := time.AfterFunc(activation.Sub(now), func() { s.execTrigger(triggerCtx, t) })
	s.tasks[t.ID] = cancelableTrigger{
		timer: timer,
		cancel: func() {
			cancel()
			timer.Stop()
		},
	}
	s.taskLock.Unlock()
}

func (s *Service) attachTask(ctx context.Context, t *Task, run *Run) {
	s.logger.Debug(ctx, "attachTask", "task", t, "run", run)
	triggerCtx, cancel := context.WithCancel(s.cronCtx)
	s.taskLock.Lock()
	s.tasks[t.ID] = cancelableTrigger{
		cancel: cancel,
	}
	s.taskLock.Unlock()

	go func() {
		defer s.reschedTask(triggerCtx, t)

		run.Status = runner.StatusRunning
		if err := s.putRun(ctx, run); err != nil {
			s.logger.Error(ctx, "failed to write run", "run", run)
			return
		}

		s.waitTask(ctx, t, run)
	}()
}

func (s *Service) reschedTask(ctx context.Context, t *Task) {
	defer reschedTaskDone(t)
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
	if !t.Sched.Repeat {
		s.logger.Debug(ctx, "one-shot task, not re-scheduling", "Task", t)
		return
	}
	s.schedTask(ctx, timeNow().UTC(), t)
}

func (s *Service) execTrigger(ctx context.Context, t *Task) {
	defer s.reschedTask(ctx, t)

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

	if err := s.taskRunner(t).RunTask(ctx, run.ClusterID, run.ID, t.Properties); err != nil {
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
	s.wg.Add(1)
	ticker := time.NewTicker(monitorTaskInterval)
	defer func() {
		ticker.Stop()
		s.wg.Done()
	}()

	for {
		select {
		case <-ctx.Done():
			ctx = context.Background()
			if err := s.taskRunner(t).StopTask(ctx, run.ClusterID, run.ID, t.Properties); err != nil {
				s.logger.Info(ctx, "failed to stop task", "Task", t, "run ID", run.ID, "error", err)
				continue
			}
			run.Status = runner.StatusStopping
			if err := s.putRun(ctx, run); err != nil {
				s.logger.Error(ctx, "failed to write run", "run", run)
			}

		case now := <-ticker.C:
			curStatus, err := s.taskRunner(t).TaskStatus(ctx, t.ClusterID, run.ID, t.Properties)
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

func (s *Service) cancelTask(t *Task) {
	s.taskLock.Lock()
	if trigger, ok := s.tasks[t.ID]; ok {
		delete(s.tasks, t.ID)
		trigger.cancel()
	}
	s.taskLock.Unlock()
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
	s.logger.Debug(ctx, "GetLastRunN", "task", t)

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

func (nilRunner) RunTask(ctx context.Context, clusterID, taskID uuid.UUID, props runner.TaskProperties) error {
	return errNilRunnerUsed
}

func (nilRunner) StopTask(ctx context.Context, clusterID, taskID uuid.UUID, props runner.TaskProperties) error {
	return errNilRunnerUsed
}

func (nilRunner) TaskStatus(ctx context.Context, clusterID, taskID uuid.UUID, props runner.TaskProperties) (runner.Status, error) {
	return "", errNilRunnerUsed
}
