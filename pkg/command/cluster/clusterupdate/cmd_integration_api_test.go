// Copyright (C) 2024 ScyllaDB

//go:build all || api_integration
// +build all api_integration

package clusterupdate

import (
	"bytes"
	"context"
	"os/exec"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
)

const (
	authToken        = "token"
	clusterIntroHost = "192.168.200.11"
	testUsername     = "cassandra"
	testPass         = "cassandra"
	label            = "hello fellow mellow"
)

func TestSctoolClusterUpdateIntegrationAPITest(t *testing.T) {
	for _, tc := range []struct {
		name            string
		args            []string
		expectedCluster *models.Cluster
	}{
		{
			name: "update cluster, add label",
			args: []string{"cluster", "update", "--label", label},
			expectedCluster: &models.Cluster{
				ForceTLSDisabled:       true,
				ForceNonSslSessionPort: false,
				Label:                  label,
			},
		},
		{
			name: "update cluster, no-changes",
			args: []string{"cluster", "update", "--auth-token", authToken},
			expectedCluster: &models.Cluster{
				ForceTLSDisabled:       true,
				ForceNonSslSessionPort: false,
			},
		},
		{
			name: "update cluster, force TLS enabled",
			args: []string{"cluster", "update", "--force-non-ssl-session-port"},
			expectedCluster: &models.Cluster{
				ForceTLSDisabled:       true,
				ForceNonSslSessionPort: true,
			},
		},
		{
			name: "update cluster, force TLS disabled",
			args: []string{"cluster", "update", "--force-tls-disabled=false"},
			expectedCluster: &models.Cluster{
				ForceTLSDisabled:       false,
				ForceNonSslSessionPort: false,
			},
		},
		{
			name: "update cluster, clean TLS flag",
			args: []string{"cluster", "update", "--force-tls-disabled=false", "--force-non-ssl-session-port"},
			expectedCluster: &models.Cluster{
				ForceTLSDisabled:       false,
				ForceNonSslSessionPort: true,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			client, err := managerclient.NewClient("http://localhost:5080/api/v1")
			if err != nil {
				t.Fatalf("Unable to create managerclient to consume managet HTTP API, err = {%v}", err)
			}
			clusterID, err := client.CreateCluster(context.Background(), &models.Cluster{
				AuthToken:        authToken,
				Host:             clusterIntroHost,
				Password:         testPass,
				Username:         testUsername,
				ForceTLSDisabled: true,
			})
			if err != nil {
				t.Fatalf("Unable to create cluster for further updates, err = {%v}", err)
			}

			cmd := exec.Command("./sctool.api-tests", append([]string{"--cluster", clusterID}, tc.args...)...)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			cmd.Dir = "/scylla-manager"

			// when
			output, err := cmd.Output()
			if err != nil {
				t.Fatalf("Unable to update cluster with sctool cluster update, err = {%v}, stderr = {%v}", err, stderr.String())
			}

			defer func() {
				if err != client.DeleteCluster(context.Background(), clusterID) {
					t.Logf("Failed to delete cluster, err = {%v}", err)
				}
			}()

			// then
			c, err := client.GetCluster(context.Background(), clusterID)
			if err != nil {
				t.Fatalf("Unable to retrieve cluster data using managerclient, err = {%v}", err)
			}

			if c.ID != clusterID {
				t.Fatalf("ClusterID mismatch {%v} != {%v}, output={%v}", c.ID, clusterID, string(output))
			}
			if c.ForceTLSDisabled != tc.expectedCluster.ForceTLSDisabled {
				t.Fatalf("ForceTLSDisabled mismatch {%v} != {%v}, output={%v}", c.ForceTLSDisabled,
					tc.expectedCluster.ForceTLSDisabled, string(output))
			}
			if c.ForceNonSslSessionPort != tc.expectedCluster.ForceNonSslSessionPort {
				t.Fatalf("ForceNonSslPort mismatch {%v} != {%v}, output={%v}", c.ForceNonSslSessionPort,
					tc.expectedCluster.ForceNonSslSessionPort, string(output))
			}
			if c.Label != tc.expectedCluster.Label {
				t.Fatalf("Comment mismatch {%v} != {%v}, output={%v}", c.Label,
					tc.expectedCluster.Label, string(output))
			}
		})
	}

}
