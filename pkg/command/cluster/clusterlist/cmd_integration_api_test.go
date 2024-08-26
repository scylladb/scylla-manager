// Copyright (C) 2024 ScyllaDB

//go:build all || api_integration
// +build all api_integration

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
					t.Logf("Failed to delete cluster, err = {%v}", err)
				}
			}()

			re := regexp.MustCompile(strings.ReplaceAll(tc.expectedOutputPattern, "<cluster_id>", clusterID))
			if !re.Match(output) {
				t.Fatalf("Expected to get pattern {%v}, got {%s}", tc.expectedOutputPattern, string(output))
			}
		})
	}

}
