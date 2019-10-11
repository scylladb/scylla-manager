// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/service/cluster"
	"github.com/scylladb/mermaid/service/scheduler"
	"github.com/scylladb/mermaid/uuid"
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

// Matches returns whether x is a match.
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

// Matches returns whether x is a match.
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

// ClusterMatcher gomock.Matcher interface implementation for cluster.Cluster.
type ClusterMatcher struct {
	expected *cluster.Cluster
}

// NewClusterMatcher returns gomock.Matcher for clusters. It compares only ID field.
func NewClusterMatcher(expected *cluster.Cluster) *ClusterMatcher {
	return &ClusterMatcher{
		expected: expected,
	}
}

// Matches returns whether x is a match.
func (m ClusterMatcher) Matches(v interface{}) bool {
	c, ok := v.(*cluster.Cluster)
	if !ok {
		return false
	}
	return cmp.Equal(m.expected.ID, c.ID, UUIDComparer())
}

func (m ClusterMatcher) String() string {
	return fmt.Sprintf("is equal to cluster with ID: %s", m.expected.ID.String())
}
