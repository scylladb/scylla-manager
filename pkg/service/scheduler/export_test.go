// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func (t *Task) NewRun() *Run {
	return &Run{
		ID:        uuid.NewTime(),
		Type:      t.Type,
		ClusterID: t.ClusterID,
		TaskID:    t.ID,
		StartTime: timeutc.Now(),
	}
}

func (s *Service) GetLastRun(t *Task) (*Run, error) {
	return s.getLastRun(t)
}

func (s *Service) PutTestRun(r *Run) error {
	return s.putRun(r)
}

func (s *Service) PutTestTask(t *Task) error {
	return s.putTask(t)
}
