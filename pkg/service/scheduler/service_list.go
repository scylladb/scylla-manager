// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/pkg/schema/table"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// TaskListItem decorates Task with information about task runs and scheduled
// activations.
type TaskListItem struct {
	Task
	Status         Status     `json:"status,omitempty"`
	Cause          string     `json:"cause,omitempty"`
	StartTime      *time.Time `json:"start_time,omitempty"`
	EndTime        *time.Time `json:"end_time,omitempty"`
	NextActivation *time.Time `json:"next_activation,omitempty"`
	Suspended      bool       `json:"suspended,omitempty"`
	Failures       int        `json:"failures,omitempty"`
}

// ListFilter specifies filtering parameters to ListTasks.
type ListFilter struct {
	TaskType []TaskType
	Status   []Status
	Disabled bool
	Short    bool
}

// ListTasks returns cluster tasks given the filtering criteria.
func (s *Service) ListTasks(ctx context.Context, clusterID uuid.UUID, filter ListFilter) ([]*TaskListItem, error) {
	s.logger.Debug(ctx, "ListTasks", "filter", filter)

	b := qb.Select(table.SchedulerTask.Name())
	b.Where(qb.Eq("cluster_id"))
	if len(filter.TaskType) > 0 {
		b.Where(qb.Eq("type"))
	}
	if !filter.Disabled {
		b.Where(qb.EqLit("enabled", "true"))
		b.AllowFiltering()
	}
	if filter.Short {
		m := table.SchedulerTask.Metadata()
		var cols []string
		cols = append(cols, m.PartKey...)
		cols = append(cols, m.SortKey...)
		cols = append(cols, "name")
		b.Columns(cols...)
	}

	q := b.Query(s.session)
	defer q.Release()

	// This is workaround for the following error using IN keyword
	// Cannot restrict clustering columns by IN relations when a collection is selected by the query
	var tasks []*Task
	if len(filter.TaskType) == 0 {
		q.BindMap(qb.M{
			"cluster_id": clusterID,
		})
		if err := q.Select(&tasks); err != nil {
			return nil, err
		}
	} else {
		for _, tt := range filter.TaskType {
			q.BindMap(qb.M{
				"cluster_id": clusterID,
				"type":       tt,
			})
			if err := q.Select(&tasks); err != nil {
				return nil, err
			}
		}
	}

	if filter.Short {
		return convertToTaskListItem(tasks), nil
	}

	return s.decorateTasks(clusterID, filter, tasks)
}

func convertToTaskListItem(tasks []*Task) []*TaskListItem {
	items := make([]*TaskListItem, len(tasks))
	for i, t := range tasks {
		items[i] = &TaskListItem{Task: *t}
	}
	return items
}

func (s *Service) decorateTasks(clusterID uuid.UUID, filter ListFilter, tasks []*Task) ([]*TaskListItem, error) {
	statuses := strset.New()
	for _, s := range filter.Status {
		statuses.Add(s.String())
	}
	items := make([]*TaskListItem, 0, len(tasks))
	for _, t := range tasks {
		tli := &TaskListItem{Task: *t}
		err := table.SchedulerTaskRun.
			SelectBuilder("status", "cause", "start_time", "end_time").
			Limit(1).
			Query(s.session).
			BindStructMap(t, qb.M{"task_id": t.ID}). // in task id is just ID...
			GetRelease(tli)
		if errors.Is(err, gocql.ErrNotFound) {
			tli.Status = StatusNew
			err = nil
		}
		if err != nil {
			return nil, err
		}
		if !statuses.IsEmpty() && !statuses.Has(tli.Status.String()) {
			continue
		}
		items = append(items, tli)
	}

	s.mu.Lock()
	l, lok := s.scheduler[clusterID]
	suspended := s.isSuspendedLocked(clusterID)
	s.mu.Unlock()

	if !lok {
		return items, nil
	}

	keys := make([]uuid.UUID, len(items))
	for i := range items {
		keys[i] = items[i].ID
	}
	a := l.Activations(keys...)
	for i := range items {
		if !a[i].IsZero() {
			items[i].NextActivation = &a[i].Time
		}
		items[i].Suspended = suspended
		items[i].Failures = int(a[i].Retry)
	}
	return items, nil
}
