// Copyright (C) 2024 ScyllaDB

//go:build all || api_integration
// +build all api_integration

package backup

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

const (
	bucketName = "backuptest-api"

	authToken        = "token"
	clusterIntroHost = "192.168.200.11"
	label            = "hello fellow mellow"
)

func TestSctoolBackupIntegrationAPITest(t *testing.T) {
	p := filepath.Join("/scylla-manager/testing/minio/data", bucketName)
	if err := os.RemoveAll(p); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("./sctool.api-tests", "cluster", "add", "--host", clusterIntroHost, "--auth-token",
		authToken)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Dir = "/scylla-manager"
	output, err := cmd.Output()
	fmt.Println(string(output))
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
			name: "create backup task with label",
			args: []string{"backup", "-c", clusterID, "-L", fmt.Sprintf("s3:%s", bucketName), "--label", label},
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
				t.Fatalf("Unable to create backup task with sctool backup, err = {%v}, stderr = {%v}", err, stderr.String())
			}
			backupTaskID := strings.Split(string(output), "\n")[0]
			backTaskSplit := strings.Split(backupTaskID, "/")

			// then
			task, err := client.GetTask(context.Background(), clusterID, backTaskSplit[0], uuid.MustParse(backTaskSplit[1]))
			if err != nil {
				t.Fatalf("Unable to retrieve task data using managerclient, err = {%v}", err)
			}

			if task.Label != label {
				t.Fatalf("Expected task.label = {%v}, but was {%v}", label, task.Label)
			}
		})
	}

}
