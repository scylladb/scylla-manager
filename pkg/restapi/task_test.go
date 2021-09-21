// Copyright (C) 2017 ScyllaDB

//go:generate mockgen -destination mock_schedservice_test.go -mock_names SchedService=MockSchedService -package restapi github.com/scylladb/scylla-manager/pkg/restapi SchedService

package restapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/restapi"
	"github.com/scylladb/scylla-manager/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func givenTask(clusterID uuid.UUID, taskType scheduler.TaskType) *scheduler.Task {
	return &scheduler.Task{
		ID:        uuid.NewTime(),
		ClusterID: clusterID,
		Type:      taskType,
		Enabled:   true,
		Sched:     scheduler.Schedule{},
	}
}

func givenTaskRun(clusterID, taskID uuid.UUID, taskType scheduler.TaskType, status scheduler.Status) *scheduler.Run {
	return &scheduler.Run{
		Type:      taskType,
		ClusterID: clusterID,
		TaskID:    taskID,
		Status:    status,
	}
}

func givenListTasksRequest(clusterID uuid.UUID, taskType scheduler.TaskType, status scheduler.Status) *http.Request {
	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/cluster/%s/tasks", clusterID.String()), nil)
	r.Form = url.Values{}
	r.Form.Add("type", string(taskType))
	r.Form.Add("status", string(status))
	return r
}

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
	return cmp.Equal(m.expected.ID, task.ID, testutils.UUIDComparer())
}

func (m TaskMatcher) String() string {
	return fmt.Sprintf("is equal to task with ID: %s", m.expected.ID.String())
}

func TestListTaskStatusFiltering(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sm := restapi.NewMockSchedService(ctrl)
	cm := restapi.NewMockClusterService(ctrl)

	taskType := scheduler.RepairTask

	c := givenCluster()
	t0 := givenTask(c.ID, taskType)
	t1 := givenTask(c.ID, taskType)
	run0 := givenTaskRun(c.ID, t0.ID, taskType, scheduler.StatusRunning)
	run1 := givenTaskRun(c.ID, t1.ID, taskType, scheduler.StatusError)

	services := restapi.Services{
		Scheduler: sm,
		Cluster:   cm,
	}

	h := restapi.New(services, log.Logger{})
	r := givenListTasksRequest(c.ID, taskType, scheduler.StatusRunning)
	w := httptest.NewRecorder()

	cm.EXPECT().GetCluster(gomock.Any(), c.ID.String()).Return(c, nil)
	sm.EXPECT().ListTasks(gomock.Any(), testutils.NewUUIDMatcher(c.ID), t0.Type).Return([]*scheduler.Task{t0, t1}, nil)
	sm.EXPECT().GetLastRun(gomock.Any(), NewTaskMatcher(t0), gomock.Any()).Return([]*scheduler.Run{run0}, nil)
	sm.EXPECT().GetLastRun(gomock.Any(), NewTaskMatcher(t1), gomock.Any()).Return([]*scheduler.Run{run1}, nil)
	sm.EXPECT().IsSuspended(gomock.Any(), testutils.NewUUIDMatcher(c.ID)).Return(false)

	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatal(w.Code)
	}

	responseTasks := []*scheduler.Task{}
	if err := json.Unmarshal(w.Body.Bytes(), &responseTasks); err != nil {
		t.Fatal(err)
	}

	if len(responseTasks) != 1 && cmp.Equal(responseTasks[0].ID, t0.ID, testutils.UUIDComparer()) {
		t.Error(fmt.Sprintf("Expected to receive only %s task with %s id", scheduler.StatusRunning, t0.ID.String()))
	}
}
