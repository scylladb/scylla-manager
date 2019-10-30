// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"net"
	"testing"

	"github.com/scylladb/mermaid/scyllaclient"
)

func TestNodeInfoCQLAddr(t *testing.T) {
	t.Parallel()

	const fallback = "4.3.2.1"

	table := []struct {
		Name          string
		NodeInfo      *scyllaclient.NodeInfo
		GoldenAddress string
	}{
		{
			Name: "Broadcast RPC address is set",
			NodeInfo: &scyllaclient.NodeInfo{
				BroadcastRPCAddress: "1.2.3.4",
				RPCAddress:          "1.2.3.5",
				ListenAddress:       "1.2.3.6",
				NativeTransportPort: "1234",
			},
			GoldenAddress: "1.2.3.4:1234",
		},
		{
			Name: "RPC address is set",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				RPCAddress:          "1.2.3.5",
				ListenAddress:       "1.2.3.6",
			},
			GoldenAddress: "1.2.3.5:1234",
		},
		{
			Name: "Listen Address is set",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				ListenAddress:       "1.2.3.6",
			},
			GoldenAddress: "1.2.3.6:1234",
		},
		{
			Name: "Fallback is returned when RPC Address is IPv4 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				RPCAddress:          "0.0.0.0",
			},
			GoldenAddress: net.JoinHostPort(fallback, "1234"),
		},
		{
			Name: "Fallback is returned when RPC Address is IPv6 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				RPCAddress:          "::0",
			},
			GoldenAddress: net.JoinHostPort(fallback, "1234"),
		},
		{
			Name: "Fallback is returned when Listen Address is IPv4 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				ListenAddress:       "0.0.0.0",
			},
			GoldenAddress: net.JoinHostPort(fallback, "1234"),
		},
		{
			Name: "Fallback is returned when Listen Address is IPv6 zero",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort: "1234",
				ListenAddress:       "::0",
			},
			GoldenAddress: net.JoinHostPort(fallback, "1234"),
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			addr := test.NodeInfo.CQLAddr(fallback)
			if addr != test.GoldenAddress {
				t.Errorf("expected %s address, got %s", test.GoldenAddress, addr)
			}
		})
	}
}
