// Copyright (C) 2017 ScyllaDB

package sched

import (
	"context"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
	log "github.com/scylladb/golog"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/sched/runner"
	"github.com/scylladb/mermaid/schema"
	"github.com/scylladb/mermaid/timeutc"
	"github.com/scylladb/mermaid/uuid"
)

var (
	taskActiveCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "scylla_manager",
		Subsystem: "task",
		Name:      "active_count",
		Help:      "Total number of active tasks.",
	}, []string{"cluster", "type"})

	taskStatusTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "scylla_manager",
		Subsystem: "task",
		Name:      "status_total",
		Help:      "Total number of tasks .",
	}, []string{"cluster", "type", "status"})
)

func init() {
	prometheus.MustRegister(
		taskActiveCount,
		taskStatusTotal,
	)
}

// Service schedules tasks.
type Service struct {
	session   *gocql.Session
	cluster   cluster.ProviderFunc
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
func NewService(session *gocql.Session, cp cluster.ProviderFunc, l log.Logger) (*Service, error) {
	if session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	if cp == nil {
		return nil, errors.New("invalid cluster provider")
	}

	return &Service{
		session: session,
		cluster: cp,
		logger:  l,

		cronCtx: log.WithTraceID(context.Background()),
		runners: make(map[TaskType]runner.Runner),
		tasks:   make(map[uuid.UUID]cancelableTrigger),
	}, nil
}

// LoadTasks should be called on start. It attaches to running tasks if there are such,
// marking no-longer running ones as stopped. It then proceeds to schedule future tasks.
func (s *Service) LoadTasks(ctx context.Context) error {
	s.logger.Info(ctx, "Loading tasks")

	now := timeutc.Now()

	stmt, _ := qb.Select(schema.SchedTask.Name).ToCql()
	q := s.session.Query(stmt).WithContext(ctx)
	defer q.Release()

	iter := gocqlx.Iter(q)
	defer iter.Close()

	var t Task
	for iter.StructScan(&t) {
		if !t.Enabled {
			continue
		}

		runs, err := s.GetLastRun(ctx, &t, 1)
		if err != nil {
			s.logger.Error(ctx, "Failed to get last run of task", "task", t, "error", err)
			continue
		}

		if n := len(runs); n > 0 {
			r := runs[0]

			switch r.Status {
			case runner.StatusStarting, runner.StatusRunning, runner.StatusStopping:
				curStatus, cause, err := s.taskRunner(&t).Status(ctx, t.ClusterID, r.ID, t.Properties)
				if err != nil {
					s.logger.Error(ctx, "Failed to get task status", "task", t, "run", r, "error", err)
					continue
				}
				switch curStatus {
				case runner.StatusRunning, runner.StatusStopping:
					t := t
					s.attachTask(ctx, &t, r)
					continue
				case runner.StatusStopped, runner.StatusError:
					r.Status = curStatus
					r.EndTime = &now
					if curStatus == runner.StatusError {
						r.Cause = cause
					}
				default:
					s.logger.Error(ctx, "Unexpected task status",
						"task", t,
						"run", r,
						"status", curStatus,
					)
					continue
				}
				if err := s.putRun(ctx, r); err != nil {
					s.logger.Error(ctx, "Failed to write run",
						"run", r,
						"error", err,
					)
					return err
				}
			}
		}

		if t.Sched.IntervalDays > 0 || t.Sched.StartDate.After(now) {
			t := t
			s.schedTask(ctx, now, &t)
		}
	}

	s.logger.Info(ctx, "Done")

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
	runs, err := s.GetLastRun(ctx, t, t.Sched.NumRetries)
	if err != nil {
		s.logger.Error(ctx, "Failed to get history of task", "task", t, "error", err)
		return
	}
	activation := t.Sched.nextActivation(now, runs)
	if activation.IsZero() {
		s.logger.Debug(ctx, "No activation", "task", t)
		return
	}
	if !now.Before(activation) {
		s.logger.Error(ctx, "Task in the past",
			"task", t,
			"activation", activation,
			"now", now,
		)
		return
	}

	s.logger.Info(ctx, "Task scheduled",
		"cluster_id", t.ClusterID,
		"task_type", t.Type,
		"task_id", t.ID,
		"activation", activation,
	)

	triggerCtx, cancel := context.WithCancel(log.WithTraceID(s.cronCtx))

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
	triggerCtx, cancel := context.WithCancel(log.WithTraceID(s.cronCtx))
	doneCh := make(chan struct{})
	s.taskLock.Lock()
	s.tasks[t.ID] = cancelableTrigger{
		cancel: cancel,
		done:   doneCh,
	}
	s.taskLock.Unlock()

	s.logger.Info(ctx, "Attached task run",
		"cluster_id", t.ClusterID,
		"task_type", t.Type,
		"task_id", t.ID,
		"run_id", run.ID,
	)

	s.wg.Add(1)
	go func() {
		defer s.reschedTask(triggerCtx, t, run, doneCh)

		run.Status = runner.StatusRunning
		if err := s.putRun(ctx, run); err != nil {
			s.logger.Error(ctx, "Failed to write run",
				"run", run,
				"error", err,
			)
			return
		}

		s.waitTask(ctx, t, run)
	}()
}

func (s *Service) reschedTask(ctx context.Context, t *Task, run *Run, done chan struct{}) {
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
		s.logger.Debug(ctx, "Task canceled, not re-scheduling", "task", t)
		return
	}
	if t.Sched.IntervalDays == 0 && run.Status == runner.StatusStopped {
		s.logger.Debug(ctx, "One-shot task, not re-scheduling", "task", t)
		return
	}
	s.schedTask(ctx, timeutc.Now(), t)
}

func (s *Service) execTrigger(ctx context.Context, t *Task, done chan struct{}) {
	s.logger.Debug(ctx, "execTrigger", "task", t)

	now := timeutc.Now()
	run := &Run{
		ID:        uuid.NewTime(),
		Type:      t.Type,
		ClusterID: t.ClusterID,
		TaskID:    t.ID,
		Status:    runner.StatusStarting,
		StartTime: now,
	}

	s.wg.Add(1)
	defer s.reschedTask(ctx, t, run, done) // reschedTask calls s.wg.Done()

	if err := s.putRun(ctx, run); err != nil {
		s.logger.Error(ctx, "Failed to write run",
			"run", run,
			"error", err,
		)
		return
	}

	s.updateClusterName(ctx, t)

	if err := s.taskRunner(t).Run(ctx, run.ClusterID, run.ID, t.Properties); err != nil {
		s.logger.Info(ctx, "Failed to start task",
			"cluster_id", t.ClusterID,
			"task_type", t.Type,
			"task_id", t.ID,
			"run_id", run.ID,
			"error", err,
		)
		run.Status = runner.StatusError
		run.EndTime = &now
		run.Cause = err.Error()
		if err := s.putRun(ctx, run); err != nil {
			s.logger.Error(ctx, "Failed to write run",
				"run", run,
				"error", err,
			)
		}
		return
	}

	s.logger.Info(ctx, "Task started",
		"cluster_id", t.ClusterID,
		"task_type", t.Type,
		"task_id", t.ID,
		"run_id", run.ID,
	)

	taskActiveCount.With(prometheus.Labels{
		"cluster": t.clusterName,
		"type":    t.Type.String(),
	}).Inc()

	run.Status = runner.StatusRunning
	if err := s.putRun(ctx, run); err != nil {
		s.logger.Error(ctx, "Failed to write run",
			"run", run,
			"error", err,
		)
		return
	}

	s.waitTask(ctx, t, run)
}

func (s *Service) waitTask(ctx context.Context, t *Task, run *Run) {
	ticker := time.NewTicker(monitorTaskInterval)
	defer ticker.Stop()

	defer func() {
		taskActiveCount.With(prometheus.Labels{
			"cluster": t.clusterName,
			"type":    t.Type.String(),
		}).Dec()

		taskStatusTotal.With(prometheus.Labels{
			"cluster": t.clusterName,
			"type":    t.Type.String(),
			"status":  run.Status.String(),
		}).Inc()
	}()

	for {
		select {
		case <-ctx.Done():
			ctx = log.CopyTraceID(context.Background(), ctx)

			if err := s.taskRunner(t).Stop(ctx, run.ClusterID, run.ID, t.Properties); err != nil {
				s.logger.Error(ctx, "Failed to stop task",
					"task", t,
					"run", run,
					"error", err,
				)
				continue
			}
			run.Status = runner.StatusStopping
			if err := s.putRun(ctx, run); err != nil {
				s.logger.Error(ctx, "Failed to write run",
					"run", run,
					"error", err,
				)
			}
		case now := <-ticker.C:
			curStatus, cause, err := s.taskRunner(t).Status(ctx, t.ClusterID, run.ID, t.Properties)
			if err != nil {
				s.logger.Error(ctx, "Failed to get task status",
					"task", t,
					"run", run,
					"error", err,
				)
				continue
			}
			switch curStatus {
			case runner.StatusStopped, runner.StatusError:
				run.Status = curStatus
				run.EndTime = &now
				if curStatus == runner.StatusError {
					run.Cause = cause
				}
				if err := s.putRun(ctx, run); err != nil {
					s.logger.Error(ctx, "Failed to write run",
						"run", run,
						"error", err,
					)
				}

				s.logger.Info(ctx, "Status",
					"status", curStatus,
					"cause", cause,
				)

				return
			}
		}
	}
}

func (s *Service) updateClusterName(ctx context.Context, t *Task) {
	c, _ := s.cluster(ctx, t.ClusterID)
	if c != nil {
		t.clusterName = c.String()
	} else {
		t.clusterName = t.ClusterID.String()
	}
}

// StartTask starts execution of a task immediately, regardless of the task's schedule.
func (s *Service) StartTask(ctx context.Context, t *Task) error {
	s.logger.Debug(ctx, "StartTask", "task", t)
	if t == nil {
		return errors.New("nil task")
	}

	s.cancelTask(t)
	triggerCtx, cancel := context.WithCancel(log.WithTraceID(s.cronCtx))
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
	s.logger.Debug(ctx, "StopTask", "task", t)
	if t == nil {
		return errors.New("nil task")
	}

	s.cancelTask(t)
	if t.Enabled {
		s.schedTask(ctx, timeutc.Now(), t)
	}

	s.logger.Info(ctx, "Stopping task",
		"cluster_id", t.ClusterID,
		"task_type", t.Type,
		"task_id", t.ID,
	)

	return nil
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

	stmt, names := schema.SchedTask.Get()

	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": clusterID,
		"type":       tp,
		"id":         id,
	})
	defer q.Release()

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
	defer q.Release()

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
	s.logger.Debug(ctx, "PutTask", "task", t)

	if t != nil && t.ID == uuid.Nil {
		var err error
		if t.ID, err = uuid.NewRandom(); err != nil {
			return errors.Wrap(err, "couldn't generate random UUID for task")
		}
	}

	if err := t.Validate(); err != nil {
		return mermaid.ParamError{Cause: errors.Wrap(err, "invalid task")}
	}

	if t.Sched.StartDate.Before(timeutc.Now()) {
		return mermaid.ParamError{Cause: errors.New("start date in the past")}
	}

	stmt, names := schema.SchedTask.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(t)

	if err := q.ExecRelease(); err != nil {
		return err
	}
	s.cancelTask(t)
	if t.Enabled {
		s.schedTask(ctx, timeutc.Now(), t)
	}
	return nil
}

// DeleteTask removes a task based on ID.
func (s *Service) DeleteTask(ctx context.Context, t *Task) error {
	s.logger.Debug(ctx, "DeleteTask", "task", t)

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

	s.logger.Info(ctx, "Task deleted",
		"cluster_id", t.ClusterID,
		"task_type", t.Type,
		"task_id", t.ID,
	)

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
	defer q.Release()

	if q.Err() != nil {
		return nil, q.Err()
	}

	var tasks []*Task
	err := gocqlx.Select(&tasks, q.Query)
	return tasks, err
}

// GetLastRun returns at most limit recent runs of the task.
func (s *Service) GetLastRun(ctx context.Context, t *Task, limit int) ([]*Run, error) {
	s.logger.Debug(ctx, "GetLastRun", "task", t, "limit", limit)

	// validate the task
	if err := t.Validate(); err != nil {
		return nil, mermaid.ParamError{Cause: errors.Wrap(err, "invalid task")}
	}
	if limit <= 0 {
		return nil, mermaid.ParamError{Cause: errors.New("limit must be > 0")}
	}

	b := qb.Select(schema.SchedRun.Name).Where(
		qb.Eq("cluster_id"),
		qb.Eq("type"),
		qb.Eq("task_id"),
	)
	b.Limit(uint(limit))

	stmt, names := b.ToCql()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindMap(qb.M{
		"cluster_id": t.ClusterID,
		"type":       t.Type,
		"task_id":    t.ID,
	})
	defer q.Release()

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
	stmt, names := schema.SchedRun.Insert()
	q := gocqlx.Query(s.session.Query(stmt).WithContext(ctx), names).BindStruct(r)

	return q.ExecRelease()
}

// Close cancels all future task timers and waits for all running goroutines to terminate.
func (s *Service) Close() {
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

func (nilRunner) Status(ctx context.Context, clusterID, runID uuid.UUID, props runner.TaskProperties) (runner.Status, string, error) {
	return "", "", errNilRunnerUsed
}
