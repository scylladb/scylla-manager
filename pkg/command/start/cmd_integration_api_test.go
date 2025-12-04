// Copyright (C) 2025 ScyllaDB

//go:build all || api_integration

package start

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
)

const (
	authToken        = "token"
	clusterIntroHost = "192.168.200.11"
)

func TestSoftStartIntegrationAPITest(t *testing.T) {
	client, err := managerclient.NewClient("http://localhost:5080/api/v1")
	if err != nil {
		t.Fatalf("Unable to create managerclient to consume manager HTTP API, err = {%v}", err)
	}

	clusterID, err := client.CreateCluster(context.Background(), &models.Cluster{
		AuthToken: authToken,
		Host:      clusterIntroHost,
	})
	if err != nil {
		t.Fatalf("Unable to create cluster, err = {%v}", err)
	}

	defer func() {
		if err := client.DeleteCluster(context.Background(), clusterID); err != nil {
			t.Fatalf("Failed to delete cluster, err = {%v}", err)
		}
	}()

	t.Log("Create task with schedule in the far future")
	sd := strfmt.DateTime(timeutc.Now().Add(time.Hour))
	taskID, err := client.CreateTask(context.Background(), clusterID, &managerclient.Task{
		Type:       managerclient.RepairTask,
		Enabled:    true,
		Properties: make(map[string]interface{}),
		Schedule: &managerclient.Schedule{
			Cron:      "* * * * *",
			StartDate: &sd,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create task, err = {%v}", err)
	}

	t.Log("Disable task")
	if err := client.StopTask(context.Background(), clusterID, managerclient.RepairTask, taskID, true); err != nil {
		t.Fatal(err)
	}

	t.Log("Enable and soft start task for the first time")
	err = client.StartTaskWithParams(context.Background(), clusterID, managerclient.RepairTask, taskID, managerclient.StartTaskParams{
		Enable: true,
		Soft:   true,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Wait for task to start running")
	waitTaskStatus(t, client, clusterID, managerclient.RepairTask, taskID.String(), managerclient.TaskStatusRunning, time.Second, 5*time.Second)

	t.Log("Stop task")
	if err := client.StopTask(context.Background(), clusterID, managerclient.RepairTask, taskID, false); err != nil {
		t.Fatal(err)
	}

	t.Log("Wait for task to stop")
	waitTaskStatus(t, client, clusterID, managerclient.RepairTask, taskID.String(), managerclient.TaskStatusStopped, time.Second, 5*time.Second)

	t.Log("Ensure that the task has no success")
	task := getTask(t, client, clusterID, managerclient.RepairTask, taskID.String())
	if task.LastSuccess != nil || task.SuccessCount != 0 {
		t.Fatal("Expected no task success")
	}

	t.Log("Soft start task after stop")
	err = client.StartTaskWithParams(context.Background(), clusterID, managerclient.RepairTask, taskID, managerclient.StartTaskParams{
		Soft: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Wait for task to start running")
	waitTaskStatus(t, client, clusterID, managerclient.RepairTask, taskID.String(), managerclient.TaskStatusRunning, time.Second, 5*time.Second)

	t.Log("Wait for task to be done")
	waitTaskStatus(t, client, clusterID, managerclient.RepairTask, taskID.String(), managerclient.TaskStatusDone, time.Second, 5*time.Minute)

	t.Log("Ensure that task has success")
	task = getTask(t, client, clusterID, managerclient.RepairTask, taskID.String())
	if task.LastSuccess == nil || task.SuccessCount != 1 {
		t.Fatal("Expected 1 task success")
	}

	t.Log("Soft start task after success")
	err = client.StartTaskWithParams(context.Background(), clusterID, managerclient.RepairTask, taskID, managerclient.StartTaskParams{
		Soft: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Ensure that task was not started again")
	task = getTask(t, client, clusterID, managerclient.RepairTask, taskID.String())
	if task.Status != managerclient.TaskStatusDone {
		t.Fatalf("Expected task to be done, got %q", task.Status)
	}
	if task.LastSuccess == nil || task.SuccessCount != 1 {
		t.Fatal("Expected 1 task success")
	}
}

func getTask(t *testing.T, client managerclient.Client, clusterID, taskType, taskID string) *models.TaskListItem {
	t.Helper()

	tasks, err := client.ListTasks(context.Background(), clusterID, taskType, true, "", taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks.TaskListItemSlice) != 1 {
		t.Fatalf("Expected one task, got %d", len(tasks.TaskListItemSlice))
	}
	return tasks.TaskListItemSlice[0]
}

func waitTaskStatus(t *testing.T, client managerclient.Client, clusterID, taskType, taskID, status string, interval, wait time.Duration) {
	testutils.WaitCond(t, func() bool {
		task := getTask(t, client, clusterID, taskType, taskID)
		return task.Status == status
	}, interval, wait)
}
