// Copyright (C) 2022 ScyllaDB

package scheduler

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/duration"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type suspendInfo struct {
	ClusterID                  uuid.UUID       `json:"-"`
	StartedAt                  time.Time       `json:"started_at"`
	PendingTasks               []uuid.UUID     `json:"pending_tasks"`
	PendingTasksNextActivation []time.Time     `json:"pending_tasks_next_activation"`
	RunningTask                []uuid.UUID     `json:"running_tasks"`
	AllowTask                  AllowedTaskType `json:"allow_task_type"`
}

var _ store.Entry = &suspendInfo{}

func (v *suspendInfo) Key() (clusterID uuid.UUID, key string) {
	return v.ClusterID, "scheduler_suspended"
}

func (v *suspendInfo) MarshalBinary() (data []byte, err error) {
	return json.Marshal(v)
}

func (v *suspendInfo) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, v)
}

// SuspendPolicy describes behavior towards running tasks (other than AllowTask) when suspend is requested.
type SuspendPolicy string

const (
	// SuspendPolicyStopRunningTasks results in stopping running tasks.
	SuspendPolicyStopRunningTasks SuspendPolicy = "stop_running_tasks"
	// SuspendPolicyFailIfRunningTasks results in failing to suspend cluster and returning ErrNotAllowedTasksRunning.
	SuspendPolicyFailIfRunningTasks SuspendPolicy = "fail_if_running_tasks"
)

// ErrNotAllowedTasksRunning is returned when there are not allowed tasks running
// during suspend and SuspendPolicyFailIfRunningTasks is specified.
var ErrNotAllowedTasksRunning = errors.New("not allowed tasks are running. Use suspend_policy='stop_running_tasks' to stop those tasks before suspending cluster")

// SuspendProperties specify properties of Suspend task.
type SuspendProperties struct {
	Resume        bool              `json:"resume"`
	Duration      duration.Duration `json:"duration"`
	StartTasks    bool              `json:"start_tasks"`
	AllowTask     AllowedTaskType   `json:"allow_task_type"`
	SuspendPolicy SuspendPolicy     `json:"suspend_policy"`
	NoContinue    bool              `json:"no_continue"`
}

// GetSuspendProperties unmarshals suspend properties and validates them.
func GetSuspendProperties(data []byte) (SuspendProperties, error) {
	properties := SuspendProperties{}
	if err := json.Unmarshal(data, &properties); err != nil {
		return properties, err
	}

	if properties.StartTasks {
		if properties.Duration == 0 {
			return properties, errors.New("can't use startTasks without a duration")
		}
	}

	return properties, nil
}

func (s *Service) initSuspended() error {
	var clusters []uuid.UUID
	if err := qb.Select(table.SchedulerTask.Name()).Distinct("cluster_id").Query(s.session).SelectRelease(&clusters); err != nil {
		return errors.Wrap(err, "list clusters")
	}

	for _, c := range clusters {
		si := &suspendInfo{ClusterID: c}
		if err := s.drawer.Get(si); err != nil {
			if !errors.Is(err, util.ErrNotFound) {
				return err
			}
		} else {
			s.suspended[c] = suspendParams{
				AllowTask: si.AllowTask,
			}
			s.metrics.Suspend(c)
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

// SuspendStatus contains information about the suspension state of a cluster.
type SuspendStatus struct {
	Suspended bool
	AllowTask AllowedTaskType
}

// SuspendStatus returns detailed information about cluster suspend state.
func (s *Service) SuspendStatus(_ context.Context, clusterID uuid.UUID) SuspendStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	params, ok := s.suspended[clusterID]
	return SuspendStatus{
		Suspended: ok,
		AllowTask: params.AllowTask,
	}
}

func (s *Service) isSuspendedLocked(clusterID uuid.UUID) bool {
	_, ok := s.suspended[clusterID]
	return ok
}

// isSuspendedForTaskLocked checks if the cluster is suspended for the provided task type.
func (s *Service) isSuspendedForTaskLocked(clusterID uuid.UUID, taskType TaskType) bool {
	p, suspended := s.suspended[clusterID]
	if !suspended {
		return false
	}
	if p.AllowTask.IsEmpty() {
		return true
	}
	return p.AllowTask.TaskType != taskType
}

// Suspend stops scheduler for a given cluster.
// Behavior towards currently running tasks is described with suspendPolicy.
// Scheduled task executions will be canceled.
// Scheduler can be later resumed, see `Resume` function.
// If allowTaskType is provided, it will allow the specified task type to be scheduled while the cluster is suspended.
// If noContinue is set, next activation of disabled tasks will start from scratch.
// In such case, task cleanup will also be performed.
// If wait is set, this method call waits for the suspended tasks to stop running.
func (s *Service) Suspend(ctx context.Context, clusterID uuid.UUID, allowTaskType string, suspendPolicy SuspendPolicy, noContinue bool) error {
	var (
		allowTask AllowedTaskType
		props     SuspendProperties
	)
	if err := allowTask.UnmarshalText([]byte(allowTaskType)); err != nil {
		return err
	}
	props.AllowTask = allowTask
	props.SuspendPolicy = suspendPolicy
	props.NoContinue = noContinue
	return s.suspend(ctx, clusterID, true, props)
}

func (s *Service) suspend(ctx context.Context, clusterID uuid.UUID, wait bool, p SuspendProperties) error {
	if p.Duration > 0 {
		s.logger.Info(ctx, "Suspending cluster", "cluster_id", clusterID, "target", p, "allow_task_type", p.AllowTask)
	} else {
		s.logger.Info(ctx, "Suspending cluster", "cluster_id", clusterID, "allow_task_type", p.AllowTask)
	}

	si := &suspendInfo{
		ClusterID: clusterID,
		StartedAt: timeutc.Now(),
		AllowTask: p.AllowTask,
	}

	var tasks []Task
	if err := s.forEachClusterTask(clusterID, func(t *Task) error {
		tasks = append(tasks, *t)
		return nil
	}); err != nil {
		return errors.Wrap(err, "list tasks")
	}

	s.mu.Lock()
	if s.isSuspendedLocked(clusterID) {
		s.logger.Info(ctx, "Cluster already suspended", "cluster_id", clusterID)
		s.mu.Unlock()
		return nil // nolint: nilnil
	}

	if p.SuspendPolicy == SuspendPolicyFailIfRunningTasks {
		notAllowedRunning := s.notAllowedRunningTasksLocked(clusterID, p.AllowTask, tasks)
		if len(notAllowedRunning) != 0 {
			s.mu.Unlock()
			return errors.Wrapf(ErrNotAllowedTasksRunning, "running tasks %v", notAllowedRunning)
		}
	}

	s.suspended[clusterID] = suspendParams{
		AllowTask: p.AllowTask,
	}
	s.metrics.Suspend(clusterID)
	l := s.resetSchedulerLocked(si)
	s.mu.Unlock()

	for i := range tasks {
		if tasks[i].Type == p.AllowTask.TaskType {
			s.schedule(ctx, &tasks[i], slices.Contains(si.RunningTask, tasks[i].ID))
			continue
		}
		if tasks[i].Type == HealthCheckTask {
			s.schedule(ctx, &tasks[i], false)
			continue
		}
		if p.NoContinue {
			s.SetTaskNoContinue(tasks[i].ID, true)
		}
	}

	if p.Duration > 0 {
		rt, err := newResumeTask(si, p)
		if err != nil {
			return errors.Wrap(err, "new resume task")
		}
		if err := table.SchedulerTask.InsertQuery(s.session).BindStruct(rt).ExecRelease(); err != nil {
			return errors.Wrap(err, "put task")
		}
		s.schedule(ctx, rt, false)
	}

	if err := s.drawer.Put(si); err != nil {
		return errors.Wrap(err, "save canceled tasks")
	}

	if wait && l != nil {
		l.Wait()
	}
	if p.NoContinue {
		if err := s.cleanup(ctx, clusterID, p.AllowTask); err != nil {
			return err
		}
	}
	return nil
}

// notAllowedRunningTasksLocked returns all currently running task IDs of types other than allowed.
func (s *Service) notAllowedRunningTasksLocked(clusterID uuid.UUID, allowed AllowedTaskType, tasks []Task) []uuid.UUID {
	l, ok := s.scheduler[clusterID]
	if !ok || l == nil {
		return nil
	}
	running := l.Running()
	var notAllowedRunning []uuid.UUID
	for i := range tasks {
		if tasks[i].Type == allowed.TaskType || tasks[i].Type == HealthCheckTask || tasks[i].Type == SuspendTask {
			continue
		}
		if slices.Contains(running, tasks[i].ID) {
			notAllowedRunning = append(notAllowedRunning, tasks[i].ID)
		}
	}
	return notAllowedRunning
}

// resetSchedulerLocked closes the current scheduler, records the information on running tasks, and creates a new empty scheduler.
// It returns the old closed scheduler.
func (s *Service) resetSchedulerLocked(si *suspendInfo) *Scheduler {
	cid := si.ClusterID
	l := s.scheduler[cid]
	if l != nil {
		si.RunningTask, si.PendingTasks, si.PendingTasksNextActivation = l.Close()
	}
	s.scheduler[cid] = s.newScheduler(cid)
	return l
}

// cleanup all tasks types except for the allowed one with Service.cleaners.
func (s *Service) cleanup(ctx context.Context, clusterID uuid.UUID, allowed AllowedTaskType) error {
	for tt, c := range s.cleaners {
		if tt == allowed.TaskType {
			continue
		}
		if err := c(ctx, clusterID); err != nil {
			return errors.Wrapf(err, "cleanup %s tasks", tt)
		}
	}
	return nil
}

// ResumeTaskID is a special task ID reserved for scheduled resume of suspended cluster.
// It can be reused for different suspend tasks at different times.
// Note that a suspended cluster cannot be suspended.
var ResumeTaskID = uuid.MustParse("805E43B0-2C0A-481E-BAB8-9C2418940D67")

func newResumeTask(si *suspendInfo, p SuspendProperties) (*Task, error) {
	p.Resume = true

	b, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}

	return &Task{
		ClusterID: si.ClusterID,
		Type:      SuspendTask,
		ID:        ResumeTaskID,
		Name:      "resume",
		Enabled:   true,
		Sched: Schedule{
			StartDate:  si.StartedAt.Add(p.Duration.Duration()),
			NumRetries: 3,
			RetryWait:  duration.Duration(5 * time.Second),
		},
		Status:     StatusNew,
		Properties: b,
	}, nil
}

func newDisabledResumeTask(clusterID uuid.UUID) *Task {
	return &Task{
		ClusterID: clusterID,
		Type:      SuspendTask,
		ID:        ResumeTaskID,
		Name:      "resume",
	}
}

// Resume resumes scheduler for a suspended cluster.
func (s *Service) Resume(ctx context.Context, clusterID uuid.UUID, startTasks, startTasksMissedActivation, noContinue bool) error {
	s.logger.Info(ctx, "Resuming cluster", "cluster_id", clusterID)

	s.mu.Lock()
	if !s.isSuspendedLocked(clusterID) {
		s.mu.Unlock()
		s.logger.Info(ctx, "Cluster not suspended", "cluster_id", clusterID)
		return nil
	}
	si := &suspendInfo{ClusterID: clusterID}
	if err := s.drawer.Get(si); err != nil {
		if errors.Is(err, util.ErrNotFound) {
			s.logger.Error(ctx, "Expected canceled tasks got none")
		} else {
			s.mu.Unlock()
			return errors.Wrap(err, "get canceled tasks")
		}
	}
	if err := s.drawer.Delete(si); err != nil {
		s.logger.Error(ctx, "Failed to delete canceled tasks", "error", err)
	}
	delete(s.suspended, clusterID)
	s.metrics.Resume(clusterID)
	s.mu.Unlock()

	if err := s.forEachClusterTask(clusterID, func(t *Task) error {
		if t.Type == si.AllowTask.TaskType {
			return nil
		}

		if noContinue {
			s.SetTaskNoContinue(t.ID, true)
		}

		s.schedule(ctx, t, s.shouldStartTaskOnResume(si, t, startTasks, startTasksMissedActivation))
		return nil
	}); err != nil {
		return errors.Wrap(err, "schedule")
	}

	if err := s.PutTask(ctx, newDisabledResumeTask(clusterID)); err != nil {
		return errors.Wrap(err, "disable resume task")
	}

	return nil
}

func (s *Service) shouldStartTaskOnResume(si *suspendInfo, task *Task, startTasks, startTasksMissedActivation bool) bool {
	if task.Type == SuspendTask || task.Type == HealthCheckTask {
		return false
	}

	if startTasks {
		if slices.Contains(si.RunningTask, task.ID) {
			return true
		}
	}

	if startTasksMissedActivation {
		idx := slices.Index(si.PendingTasks, task.ID)
		if idx >= 0 && len(si.PendingTasks) == len(si.PendingTasksNextActivation) {
			if next := si.PendingTasksNextActivation[idx]; !next.IsZero() && next.Before(now()) {
				return true
			}
		}
	}

	if needsOneShotRun(task) {
		return true
	}

	return false
}

func (s *Service) forEachClusterTask(clusterID uuid.UUID, f func(t *Task) error) error {
	q := qb.Select(table.SchedulerTask.Name()).Where(qb.Eq("cluster_id")).Query(s.session).Bind(clusterID)
	defer q.Release()
	return forEachTaskWithQuery(q, f)
}

type suspendRunner struct {
	service *Service
}

func (s suspendRunner) Run(ctx context.Context, clusterID, _, _ uuid.UUID, properties json.RawMessage) error {
	p, err := GetSuspendProperties(properties)
	if err != nil {
		return util.ErrValidate(err)
	}

	if p.Resume {
		err = s.service.Resume(ctx, clusterID, p.StartTasks, false, false)
	} else {
		// Suspend close scheduler while running for this reason we need to
		// - detach from the context
		// - ignore wait for tasks completion
		ctx = log.CopyTraceID(context.Background(), ctx)
		err = s.service.suspend(ctx, clusterID, false, p)
	}

	return err
}
