// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Policy decides if given task can be run.
type Policy interface {
	PreRun(clusterID, taskID, runID uuid.UUID, taskType TaskType) error
	PostRun(clusterID, taskID, runID uuid.UUID, taskType TaskType)
}

// PolicyRunner is a runner that uses policy to check if a task can be run.
type PolicyRunner struct {
	Policy Policy
	Runner Runner

	// TaskType of a task that this runner is executing
	TaskType TaskType
}

// Run implements Runner.
func (pr PolicyRunner) Run(ctx context.Context, clusterID, taskID, runID uuid.UUID, properties json.RawMessage) error {
	if err := pr.Policy.PreRun(clusterID, taskID, runID, pr.TaskType); err != nil {
		return err
	}
	defer pr.Policy.PostRun(clusterID, taskID, runID, pr.TaskType)
	return pr.Runner.Run(ctx, clusterID, taskID, runID, properties)
}

var errClusterBusy = errors.New("another task is running")

// TaskExclusiveLockPolicy is a policy that executes the exclusiveTask only if there are no other tasks in the cluster.
// Conversely, other tasks can run only if the exclusiveTask is not running.
// Additionally this policy ensures that only one task of a task type can be executed at a time in a cluster.
type TaskExclusiveLockPolicy struct {
	mu      sync.Mutex
	running map[uuid.UUID]map[TaskType]struct{}

	exclusiveTask TaskType
}

func NewTaskExclusiveLockPolicy(exclusiveTask TaskType) *TaskExclusiveLockPolicy {
	return &TaskExclusiveLockPolicy{
		running: map[uuid.UUID]map[TaskType]struct{}{},

		exclusiveTask: exclusiveTask,
	}
}

// PreRun acquires exclusive lock on a cluster for a provided taskType.
func (t *TaskExclusiveLockPolicy) PreRun(clusterID, _, _ uuid.UUID, taskType TaskType) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	cluster, ok := t.running[clusterID]
	if !ok {
		cluster = map[TaskType]struct{}{}
		t.running[clusterID] = cluster
	}

	if err := t.canRunTaskExclusively(cluster, taskType); err != nil {
		// cluster is busy
		return err
	}

	cluster[taskType] = struct{}{}

	return nil
}

// canRunTaskExclusively returns nil if taskType can be run in the cluster, otherwise err is returned.
func (t *TaskExclusiveLockPolicy) canRunTaskExclusively(cluster map[TaskType]struct{}, taskType TaskType) error {
	// No tasks are running, so we can start the task.
	if len(cluster) == 0 {
		return nil
	}

	// Exclusive task can be run only when no other tasks is running.
	if taskType == t.exclusiveTask {
		return fmt.Errorf("run exclusive task %s: %w", taskType, errClusterBusy)
	}

	// Any other task can't be run when exclusive task is running.
	if _, ok := cluster[t.exclusiveTask]; ok {
		return fmt.Errorf("exclusive task (%s) is running: %w", taskType, errClusterBusy)
	}

	// Only one task of a taskType can run in a cluster at a time.
	if _, ok := cluster[taskType]; ok {
		return errClusterBusy
	}

	return nil
}

// PostRun releases a lock on a cluster for a provided taskType.
func (t *TaskExclusiveLockPolicy) PostRun(clusterID, _, _ uuid.UUID, taskType TaskType) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// No need to check if t.running[clusterID] exists, because built-in delete will no-op in case of nil map.
	delete(t.running[clusterID], taskType)
	// Cleaning up the map if no more tasks left in the cluster.
	if len(t.running[clusterID]) == 0 {
		delete(t.running, clusterID)
	}
}
