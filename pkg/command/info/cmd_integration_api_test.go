// Copyright (C) 2024 ScyllaDB

//go:build all || api_integration
// +build all api_integration

package info

import (
	"bytes"
	"context"
	"os/exec"
	"regexp"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
)

const (
	authToken        = "token"
	clusterIntroHost = "192.168.200.11"
)

func TestSctoolInfoLabelsIntegrationAPITest(t *testing.T) {
	client, err := managerclient.NewClient("http://localhost:5080/api/v1")
	if err != nil {
		t.Fatalf("Unable to create managerclient to consume managet HTTP API, err = {%v}", err)
	}

	clusterID, err := client.CreateCluster(context.Background(), &models.Cluster{
		AuthToken: authToken,
		Host:      clusterIntroHost,
	})
	if err != nil {
		t.Fatalf("Unable to create cluster for further listing, err = {%v}", err)
	}

	defer func() {
		if err := client.DeleteCluster(context.Background(), clusterID); err != nil {
			t.Logf("Failed to delete cluster, err = {%v}", err)
		}
	}()

	taskID, err := client.CreateTask(context.Background(), clusterID, &managerclient.Task{
		Type:    "repair",
		Enabled: true,
		Labels: map[string]string{
			"k1": "v1",
		},
		Properties: make(map[string]interface{}),
	})
	if err != nil {
		t.Logf("Failed to create task, err = {%v}", err)
	}

	if err := client.UpdateTask(context.Background(), clusterID, &managerclient.Task{
		ID:      taskID.String(),
		Type:    "repair",
		Enabled: true,
		Labels: map[string]string{
			"k1": "v3",
		},
		Properties: make(map[string]interface{}),
	}); err != nil {
		t.Fatalf("Unable to update cluster for further listing, err = {%v}", err)
	}

	cmd := exec.Command("./sctool.api-tests", "info", "--cluster", clusterID, "repair/"+taskID.String())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Dir = "/scylla-manager"

	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Unable to list tasks with sctool tasks, err = {%v}, stderr = {%v}", err, stderr.String())
	}

	re := regexp.MustCompile(`k1: v3`)
	if !re.Match(output) {
		t.Fatalf("Expected to get pattern {k1: k3}, got {%s}", string(output))
	}
}
