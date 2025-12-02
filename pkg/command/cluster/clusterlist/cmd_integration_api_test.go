// Copyright (C) 2024 ScyllaDB

//go:build all || api_integration

package clusterlist

import (
	"bytes"
	"context"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
)

const (
	authToken        = "token"
	clusterIntroHost = "192.168.200.11"
)

func TestSctoolClusterListIntegrationAPITest(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		createCluster         *models.Cluster
		updateCluster         *models.Cluster
		args                  []string
		expectedOutputPattern string
	}{
		{
			name: "update cluster with label",
			createCluster: &models.Cluster{
				AuthToken: authToken,
				Host:      clusterIntroHost,
				Labels: map[string]string{
					"k1": "v1",
				},
			},
			updateCluster: &models.Cluster{
				AuthToken: authToken,
				Host:      clusterIntroHost,
				Labels: map[string]string{
					"k2": "v2",
				},
			},
			args:                  []string{"cluster", "list"},
			expectedOutputPattern: `<cluster_id> *\| *\| *k2=v2`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			client, err := managerclient.NewClient("http://localhost:5080/api/v1")
			if err != nil {
				t.Fatalf("Unable to create managerclient to consume manager HTTP API, err = {%v}", err)
			}

			clusterID, err := client.CreateCluster(context.Background(), tc.createCluster)
			if err != nil {
				t.Fatalf("Unable to create cluster for further listing, err = {%v}", err)
			}
			if tc.updateCluster != nil {
				uc := tc.updateCluster
				uc.ID = clusterID
				if err := client.UpdateCluster(context.Background(), uc); err != nil {
					t.Fatalf("Unable to update cluster for further listing, err = {%v}", err)
				}
			}

			cmd := exec.Command("./sctool.api-tests", tc.args...)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			cmd.Dir = "/scylla-manager"

			output, err := cmd.Output()
			if err != nil {
				t.Fatalf("Unable to list clusters with sctool cluster list, err = {%v}, stderr = {%v}", err, stderr.String())
			}

			defer func() {
				if err := client.DeleteCluster(context.Background(), clusterID); err != nil {
					t.Fatalf("Failed to delete cluster, err = {%v}", err)
				}
			}()

			re := regexp.MustCompile(strings.ReplaceAll(tc.expectedOutputPattern, "<cluster_id>", clusterID))
			if !re.Match(output) {
				t.Fatalf("Expected to get pattern {%v}, got {%s}", tc.expectedOutputPattern, string(output))
			}
		})
	}

}

func TestSctoolClusterListCredentialsIntegrationAPITest(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		createArgs            []string
		updateArgs            []string
		expectedOutputPattern string
	}{
		{
			name: "create cluster with alternator creds",
			createArgs: []string{"--auth-token", authToken, "--host", clusterIntroHost,
				"--username", "user", "--password", "pass",
				"--alternator-access-key-id", "id", "--alternator-secret-access-key", "key",
			},
			expectedOutputPattern: `<cluster_id> .*\| .*\| .*\| .*\| CQL, Alternator`,
		},
		{
			name: "update alternator creds",
			createArgs: []string{"--auth-token", authToken, "--host", clusterIntroHost,
				"--alternator-access-key-id", "id1", "--alternator-secret-access-key", "key1",
			},
			updateArgs:            []string{"--alternator-access-key-id", "id2", "--alternator-secret-access-key", "key2"},
			expectedOutputPattern: `<cluster_id> .*\| .*\| .*\| .*\| Alternator`,
		},
		{
			name: "delete alternator creds",
			createArgs: []string{"--auth-token", authToken, "--host", clusterIntroHost,
				"--username", "user", "--password", "pass",
				"--alternator-access-key-id", "id", "--alternator-secret-access-key", "key",
			},
			updateArgs:            []string{"--delete-alternator-credentials"},
			expectedOutputPattern: `<cluster_id> .*\| .*\| .*\| .*\| CQL`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := managerclient.NewClient("http://localhost:5080/api/v1")
			if err != nil {
				t.Fatalf("Unable to create managerclient to consume manager HTTP API, err = {%v}", err)
			}
			var stderr bytes.Buffer
			// Create cluster
			cmd := exec.Command("./sctool.api-tests", append([]string{"cluster", "add"}, tc.createArgs...)...)
			cmd.Stderr = &stderr
			cmd.Dir = "/scylla-manager"

			output, err := cmd.Output()
			if err != nil {
				t.Fatalf("Unable to create cluster with sctool cluster add, err = {%v}, stderr = {%v}", err, stderr.String())
			}
			clusterID := strings.Split(string(output), "\n")[0]

			defer func() {
				if err := client.DeleteCluster(context.Background(), clusterID); err != nil {
					t.Fatalf("Failed to delete cluster, err = {%v}", err)
				}
			}()
			// Update cluster if needed
			if len(tc.updateArgs) != 0 {
				cmd = exec.Command("./sctool.api-tests", append([]string{"cluster", "update", "-c", clusterID}, tc.updateArgs...)...)
				cmd.Stderr = &stderr
				cmd.Dir = "/scylla-manager"

				_, err = cmd.Output()
				if err != nil {
					t.Fatalf("Unable to update cluster with sctool cluster update, err = {%v}, stderr = {%v}", err, stderr.String())
				}
			}
			// List clusters
			cmd = exec.Command("./sctool.api-tests", "cluster", "list")
			cmd.Stderr = &stderr
			cmd.Dir = "/scylla-manager"

			output, err = cmd.Output()
			if err != nil {
				t.Fatalf("Unable to list clusters with sctool cluster list, err = {%v}, stderr = {%v}", err, stderr.String())
			}
			// Validate output
			pattern := strings.ReplaceAll(tc.expectedOutputPattern, "<cluster_id>", clusterID)
			re := regexp.MustCompile(pattern)
			if !re.Match(output) {
				t.Fatalf("Expected to get pattern {%v}, got {%s}", pattern, string(output))
			}
		})
	}
}
