// Copyright (C) 2025 ScyllaDB

//go:build all || api_integration
// +build all api_integration

package stop

import (
	"context"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
)

const (
	authToken        = "token"
	clusterIntroHost = "192.168.200.11"
)

func TestDeletedTaskIntegrationAPITest(t *testing.T) {
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

	taskID, err := client.CreateTask(context.Background(), clusterID, &managerclient.Task{
		Type:       managerclient.RepairTask,
		Enabled:    true,
		Properties: make(map[string]interface{}),
	})
	if err != nil {
		t.Fatalf("Failed to create task, err = {%v}", err)
	}

	if _, err := client.GetTask(context.Background(), clusterID, managerclient.RepairTask, taskID); err != nil {
		t.Fatalf("Failed to get newly created task, err = {%v}", err)
	}

	if err := client.DeleteTask(context.Background(), clusterID, managerclient.RepairTask, taskID); err != nil {
		t.Fatalf("Failed to delete task, err = {%v}", err)
	}

	if _, err := client.GetTask(context.Background(), clusterID, managerclient.RepairTask, taskID); err == nil {
		t.Fatal("Expected error on getting deleted task")
	}
}
