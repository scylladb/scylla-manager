// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/scyllaclient/scyllaclienttest"
)

type configClientFunc func(context.Context) (string, error)
type configClientBindFunc func(client *scyllaclient.ConfigClient) configClientFunc

func TestClientConfigReturnsResponseFromScylla(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name             string
		ResponseFilePath string
		BindClientFunc   configClientBindFunc
		Golden           string
	}{
		{
			Name:             "Prometheus port",
			ResponseFilePath: "testdata/scylla_api/v2_config_prometheus_port.json",
			BindClientFunc: func(client *scyllaclient.ConfigClient) configClientFunc {
				return client.PrometheusPort
			},
			Golden: "9180",
		},
		{
			Name:             "Prometheus address",
			ResponseFilePath: "testdata/scylla_api/v2_config_prometheus_address.json",
			BindClientFunc: func(client *scyllaclient.ConfigClient) configClientFunc {
				return client.PrometheusAddress
			},
			Golden: "0.0.0.0",
		},
		{
			Name:             "Broadcast address",
			ResponseFilePath: "testdata/scylla_api/v2_config_broadcast_address.json",
			BindClientFunc: func(client *scyllaclient.ConfigClient) configClientFunc {
				return client.BroadcastAddress
			},
			Golden: "192.168.100.100",
		},
		{
			Name:             "Listen address",
			ResponseFilePath: "testdata/scylla_api/v2_config_listen_address.json",
			BindClientFunc: func(client *scyllaclient.ConfigClient) configClientFunc {
				return client.ListenAddress
			},
			Golden: "192.168.100.100",
		},
		{
			Name:             "Broadcast RPC address",
			ResponseFilePath: "testdata/scylla_api/v2_config_broadcast_rpc_address.json",
			BindClientFunc: func(client *scyllaclient.ConfigClient) configClientFunc {
				return client.BroadcastRPCAddress
			},
			Golden: "1.2.3.4",
		},
		{
			Name:             "RPC port",
			ResponseFilePath: "testdata/scylla_api/v2_config_rpc_port.json",
			BindClientFunc: func(client *scyllaclient.ConfigClient) configClientFunc {
				return client.RPCPort
			},
			Golden: "9160",
		},
		{
			Name:             "RPC address",
			ResponseFilePath: "testdata/scylla_api/v2_config_rpc_address.json",
			BindClientFunc: func(client *scyllaclient.ConfigClient) configClientFunc {
				return client.RPCAddress
			},
			Golden: "192.168.100.101",
		},
		{
			Name:             "Native transport port",
			ResponseFilePath: "testdata/scylla_api/v2_config_native_transport_port.json",
			BindClientFunc: func(client *scyllaclient.ConfigClient) configClientFunc {
				return client.NativeTransportPort
			},
			Golden: "9042",
		},
		{
			Name:             "Data directories",
			ResponseFilePath: "testdata/scylla_api/v2_config_data_file_directories.json",
			BindClientFunc: func(client *scyllaclient.ConfigClient) configClientFunc {
				return client.DataDirectory
			},
			Golden: "/var/lib/scylla/data",
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			client, closeServer := scyllaclienttest.NewFakeScyllaV2Server(t, test.ResponseFilePath)
			defer closeServer()

			testFunc := test.BindClientFunc(client)
			v, err := testFunc(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if v != test.Golden {
				t.Fatalf("Expected %s got %s", test.Golden, v)
			}
		})
	}
}

func TestConfigClientPullsNodeInformationUsingScyllaAPI(t *testing.T) {
	client, closeServer := scyllaclienttest.NewFakeScyllaV2ServerMatching(t,
		scyllaclienttest.MultiPathFileMatcher(
			scyllaclienttest.PathFileMatcher("/v2/config/broadcast_address", "testdata/scylla_api/v2_config_broadcast_address.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/broadcast_rpc_address", "testdata/scylla_api/v2_config_broadcast_rpc_address.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/listen_address", "testdata/scylla_api/v2_config_listen_address.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/native_transport_port", "testdata/scylla_api/v2_config_native_transport_port.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/prometheus_address", "testdata/scylla_api/v2_config_prometheus_address.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/prometheus_port", "testdata/scylla_api/v2_config_prometheus_port.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/rpc_address", "testdata/scylla_api/v2_config_rpc_address.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/rpc_port", "testdata/scylla_api/v2_config_rpc_port.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/data_file_directories", "testdata/scylla_api/v2_config_data_file_directories.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/client_encryption_options", "testdata/scylla_api/v2_config_client_encryption_options.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/authenticator", "testdata/scylla_api/v2_config_authenticator.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/alternator_port", "testdata/scylla_api/v2_config_alternator_port.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/alternator_https_port", "testdata/scylla_api/v2_config_alternator_https_port.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/alternator_address", "testdata/scylla_api/v2_config_alternator_address.json"),
			scyllaclienttest.PathFileMatcher("/v2/config/alternator_enforce_authorization", "testdata/scylla_api/v2_config_alternator_enforce_authorization.json"),
		),
	)
	defer closeServer()

	v, err := client.NodeInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	golden, err := ioutil.ReadFile("testdata/scylla_api/v2_config_node_info.golden.json")
	if err != nil {
		t.Fatal(err)
	}

	var goldenNodeInfo scyllaclient.NodeInfo
	if err := json.Unmarshal(golden, &goldenNodeInfo); err != nil {
		t.Fatal(err)
	}

	diffOpts := []cmp.Option{
		cmpopts.IgnoreFields(scyllaclient.NodeInfo{}, "APIPort"),
	}

	if diff := cmp.Diff(v, &goldenNodeInfo, diffOpts...); diff != "" {
		t.Fatal(diff)
	}
}

func TestTextPlainError(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"message": "Not found", "code": 404}`)
	})

	client, closeServer := scyllaclienttest.NewFakeScyllaV2ServerWithHandler(t, h)
	defer closeServer()

	_, err := client.ListenAddress(context.Background())
	if err == nil {
		t.Fatalf("ListenAddress() expected error")
	}
	if err.Error() != "agent [HTTP 404] Not found" {
		t.Fatalf("ListenAddress() error %s expected not found", err)
	}
}
