// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

// TaskMatcher gomock.Matcher interface implementation for scheduler.Task.
type TaskMatcher struct {
	expected *scheduler.Task
}

// NewTaskMatcher returns gomock.Matcher for tasks. It compares only ID field.
func NewTaskMatcher(expected *scheduler.Task) *TaskMatcher {
	return &TaskMatcher{
		expected: expected,
	}
}

// Matches returns whether v is a match.
func (m TaskMatcher) Matches(v interface{}) bool {
	task, ok := v.(*scheduler.Task)
	if !ok {
		return false
	}
	return cmp.Equal(m.expected.ID, task.ID, UUIDComparer())
}

func (m TaskMatcher) String() string {
	return fmt.Sprintf("is equal to task with ID: %s", m.expected.ID.String())
}

// UUIDMatcher gomock.Matcher interface implementation for UUID.
type UUIDMatcher struct {
	expected uuid.UUID
}

// NewUUIDMatcher returns gomock.Matcher for UUIDs.
func NewUUIDMatcher(expected uuid.UUID) *UUIDMatcher {
	return &UUIDMatcher{expected: expected}
}

// Matches returns whether v is a match.
func (m UUIDMatcher) Matches(v interface{}) bool {
	id, ok := v.(uuid.UUID)
	if !ok {
		return false
	}
	return cmp.Equal(m.expected, id, UUIDComparer())
}

func (m *UUIDMatcher) String() string {
	return fmt.Sprintf("is equal to: %s", m.expected.String())
}
