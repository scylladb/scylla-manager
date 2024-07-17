// Copyright (C) 2024 ScyllaDB

//go:build all || api_integration
// +build all api_integration

package repair

import (
	"bytes"
	"context"
	"maps"
	"os/exec"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
)

const (
	authToken        = "token"
	clusterIntroHost = "192.168.200.11"
)

func TestSctoolRepairLabelIntegrationAPITest(t *testing.T) {
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

	cmd := exec.Command("./sctool.api-tests", "repair", "--cluster", clusterID, "--label", "k1=v1,k2=v2")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Dir = "/scylla-manager"

	rawRepairTaskID, err := cmd.Output()
	if err != nil {
		t.Fatalf("Unable to create task with sctool repair, err = {%v}, stderr = {%v}", err, stderr.String())
	}

	repairTaskID, _ := strings.CutSuffix(string(rawRepairTaskID), "\n")
	rawTaskID, _ := strings.CutPrefix(repairTaskID, "repair/")
	taskID, err := uuid.Parse(rawTaskID)
	if err != nil {
		t.Fatalf("Unable to parse created task ID, err = {%v}", err)
	}

	cmd = exec.Command("./sctool.api-tests", "repair", "update", "--cluster", clusterID, repairTaskID, "--label", "k1=v3")
	cmd.Stderr = &stderr
	cmd.Dir = "/scylla-manager"

	_, err = cmd.Output()
	if err != nil {
		t.Fatalf("Unable to update task with sctool repair update, err = {%v}, stderr = {%v}", err, stderr.String())
	}

	task, err := client.GetTask(context.Background(), clusterID, "repair", taskID)
	if err != nil {
		t.Fatalf("Unable to get updated task wtih client, err = {%v}", err)
	}

	if !maps.Equal(task.Labels, map[string]string{
		"k2": "v2",
		"k1": "v3",
	}) {
		t.Fatalf("Labels mismatch {%v}", task.Labels)
	}
}
