// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"testing"
)

func TestConfigClientListenAddress(t *testing.T) {
	t.Parallel()

	c, close := newMockConfigServer(t, "testdata/scylla_api/v2_config_listen_address.json")
	defer close()

	v, err := c.ListenAddress(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v != "192.168.100.100" {
		t.Fatalf("Expected %s got %s", "192.168.100.100", v)
	}
}

func TestConfigClientBroadcastAddress(t *testing.T) {
	t.Parallel()

	c, close := newMockConfigServer(t, "testdata/scylla_api/v2_config_broadcast_address.json")
	defer close()

	v, err := c.BroadcastAddress(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v != "192.168.100.100" {
		t.Fatalf("Expected %s got %s", "192.168.100.100", v)
	}
}

func TestConfigClientPrometheusAddress(t *testing.T) {
	t.Parallel()

	c, close := newMockConfigServer(t, "testdata/scylla_api/v2_config_prometheus_address.json")
	defer close()

	v, err := c.PrometheusAddress(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v != "0.0.0.0" {
		t.Fatalf("Expected %s got %s", "0.0.0.0", v)
	}
}

func TestConfigClientPrometheusPort(t *testing.T) {
	t.Parallel()

	c, close := newMockConfigServer(t, "testdata/scylla_api/v2_config_prometheus_port.json")
	defer close()

	v, err := c.PrometheusPort(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if v != "9180" {
		t.Fatalf("Expected %s got %s", "9180", v)
	}
}
