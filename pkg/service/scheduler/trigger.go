// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"sync"

	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

type triggerState int

const (
	triggerPending  triggerState = 0
	triggerRan      triggerState = 1
	triggerCanceled triggerState = 2
)

type trigger struct {
	ClusterID uuid.UUID
	Type      TaskType
	TaskID    uuid.UUID
	RunID     uuid.UUID
	C         chan struct{}

	state triggerState
	mu    sync.Mutex
}

func newTrigger(t *Task) *trigger {
	return &trigger{
		ClusterID: t.ClusterID,
		Type:      t.Type,
		TaskID:    t.ID,
		RunID:     uuid.NewTime(),
		C:         make(chan struct{}),
	}
}

func (tg *trigger) Cancel() bool {
	if tg == nil {
		return false
	}

	tg.mu.Lock()
	defer tg.mu.Unlock()

	if tg.state == triggerCanceled {
		return false
	}

	close(tg.C)
	tg.state = triggerCanceled
	return true
}

func (tg *trigger) CancelPending() bool {
	if tg == nil {
		return false
	}

	tg.mu.Lock()
	defer tg.mu.Unlock()

	if tg.state != triggerPending {
		return false
	}

	close(tg.C)
	tg.state = triggerCanceled
	return true
}

func (tg *trigger) Run() bool {
	if tg == nil {
		return false
	}

	tg.mu.Lock()
	defer tg.mu.Unlock()

	if tg.state != triggerPending {
		return false
	}

	tg.state = triggerRan
	return true
}

func (tg *trigger) State() triggerState {
	if tg == nil {
		return -1
	}

	tg.mu.Lock()
	defer tg.mu.Unlock()
	return tg.state
}
