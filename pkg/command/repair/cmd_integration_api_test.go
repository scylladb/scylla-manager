// Copyright (C) 2024 ScyllaDB

//go:build all || api_integration
// +build all api_integration

package repair

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

const (
	authToken        = "token"
	clusterIntroHost = "192.168.200.11"
	label            = "hello fellow mellow"
)

func TestSctoolRepairIntegrationAPITest(t *testing.T) {
	cmd := exec.Command("./sctool.api-tests", "cluster", "add", "--host", clusterIntroHost, "--auth-token",
		authToken)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Dir = "/scylla-manager"
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("Unable to create cluster with sctool cluster add, err = {%v}, stderr = {%v}", err, stderr.String())
	}
	clusterID := strings.Split(string(output), "\n")[0]
	client, err := managerclient.NewClient("http://localhost:5080/api/v1")
	if err != nil {
		t.Fatalf("Unable to create managerclient to consume managet HTTP API, err = {%v}", err)
	}

	defer func() {
		if err != client.DeleteCluster(context.Background(), clusterID) {
			t.Logf("Failed to delete cluster, err = {%v}", err)
		}
	}()

	for _, tc := range []struct {
		name string
		args []string
	}{
		{
			name: "create repairtask with label",
			args: []string{"repair", "-c", clusterID, "--label", label, "--start-date", time.Now().Add(24 * time.Hour).Format(time.RFC3339),
				"--cron", "0 12 * * *"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			cmd := exec.Command("./sctool.api-tests", tc.args...)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			cmd.Dir = "/scylla-manager"

			// when
			output, err := cmd.Output()
			fmt.Println(string(output))
			if err != nil {
				t.Fatalf("Unable to update repair task with sctool repair, err = {%v}, stderr = {%v}", err, stderr.String())
			}
			repairTaskID := strings.Split(string(output), "\n")[0]
			repairTaskSplit := strings.Split(repairTaskID, "/")

			// then
			task, err := client.GetTask(context.Background(), clusterID, repairTaskSplit[0], uuid.MustParse(repairTaskSplit[1]))
			if err != nil {
				t.Fatalf("Unable to retrieve task data using managerclient, err = {%v}", err)
			}

			if task.Label != label {
				t.Fatalf("Expected task.label = {%v}, but was {%v}", label, task.Label)
			}
		})
	}

}
