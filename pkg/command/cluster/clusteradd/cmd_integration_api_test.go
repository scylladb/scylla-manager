// Copyright (C) 2024 ScyllaDB

//go:build all || api_integration
// +build all api_integration

package clusteradd

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"os/exec"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
)

const (
	authToken        = "token"
	clusterIntroHost = "192.168.200.11"
	testUsername     = "cassandra"
	testPass         = "cassandra"
)

func TestSctoolClusterAddIntegrationAPITest(t *testing.T) {
	for _, tc := range []struct {
		name            string
		args            []string
		expectedCluster *models.Cluster
	}{
		{
			name: "create cluster, default TLS enablement",
			args: []string{"cluster", "add", "--auth-token", authToken, "--host", clusterIntroHost,
				"--password", testPass, "--username", testUsername},
			expectedCluster: &models.Cluster{
				ForceTLSDisabled:       false,
				ForceNonSslSessionPort: false,
				Name:                   "",
				Port:                   0,
			},
		},
		{
			name: "create cluster, force TLS enabled",
			args: []string{"cluster", "add", "--auth-token", authToken, "--host", clusterIntroHost,
				"--password", testPass, "--username", testUsername, "--force-tls-disabled=false",
				"--force-non-ssl-session-port"},
			expectedCluster: &models.Cluster{
				ForceTLSDisabled:       false,
				ForceNonSslSessionPort: true,
				Name:                   "",
				Port:                   0,
			},
		},
		{
			name: "create cluster, force TLS disabled",
			args: []string{"cluster", "add", "--auth-token", authToken, "--host", clusterIntroHost,
				"--password", testPass, "--username", testUsername, "--force-tls-disabled",
				"--force-non-ssl-session-port=false"},
			expectedCluster: &models.Cluster{
				ForceTLSDisabled:       true,
				ForceNonSslSessionPort: false,
				Name:                   "",
				Port:                   0,
			},
		},
		{
			name: "create cluster with label",
			args: []string{"cluster", "add", "--auth-token", authToken, "--host", clusterIntroHost,
				"--label", "k1=v1,with space=with-dash_underscore666",
			},
			expectedCluster: &models.Cluster{
				Labels: map[string]string{
					"k1":         "v1",
					"with space": "with-dash_underscore666",
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			client, err := managerclient.NewClient("http://localhost:5080/api/v1")
			if err != nil {
				t.Fatalf("Unable to create managerclient to consume manager HTTP API, err = {%v}", err)
			}
			cmd := exec.Command("./sctool.api-tests", tc.args...)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			cmd.Dir = "/scylla-manager"

			// when
			output, err := cmd.Output()
			fmt.Println(string(output))
			if err != nil {
				t.Fatalf("Unable to create cluster with sctool cluster add, err = {%v}, stderr = {%v}", err, stderr.String())
			}
			clusterID := strings.Split(string(output), "\n")[0]

			defer func() {
				if err := client.DeleteCluster(context.Background(), clusterID); err != nil {
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
				t.Fatalf("ForceNonSslSessionPort mismatch {%v} != {%v}, output={%v}", c.ForceNonSslSessionPort,
					tc.expectedCluster.ForceNonSslSessionPort, string(output))
			}
			if c.Port != tc.expectedCluster.Port {
				t.Fatalf("Port mismatch {%v} != {%v}, output={%v}", c.Port, tc.expectedCluster.Port, string(output))
			}
			if !maps.Equal(c.Labels, tc.expectedCluster.Labels) {
				t.Fatalf("Labels mismatch {%v} != {%v}", c.Labels, tc.expectedCluster.Labels)
			}
		})
	}

}
