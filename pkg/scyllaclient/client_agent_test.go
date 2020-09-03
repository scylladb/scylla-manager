// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"net"
	"testing"

	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"go.uber.org/atomic"
)

const fallback = "4.3.2.1"

func TestNodeInfoCQLAddr(t *testing.T) {
	t.Parallel()

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
		{
			Name: "Native Transport Port SSL with client encryption enabled without server listening on",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort:     "4321",
				NativeTransportPortSsl:  "1234",
				ListenAddress:           "1.2.3.4",
				ClientEncryptionEnabled: true,
			},
			GoldenAddress: net.JoinHostPort("1.2.3.4", "4321"),
		},
		{
			Name: "Native Transport Port SSL with client encryption disabled",
			NodeInfo: &scyllaclient.NodeInfo{
				NativeTransportPort:     "4321",
				NativeTransportPortSsl:  "1234",
				ListenAddress:           "1.2.3.4",
				ClientEncryptionEnabled: false,
			},
			GoldenAddress: net.JoinHostPort("1.2.3.4", "4321"),
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

// Test workaround used in NodeInfo.CQLPort().
func TestNodeInfoCQLAddrNativeTransportPortSSL(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	address, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	var connections atomic.Int64
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			connections.Inc()
			_ = c.Close()
		}
	}()

	ni := &scyllaclient.NodeInfo{
		NativeTransportPort:     "4321",
		NativeTransportPortSsl:  port,
		ListenAddress:           address,
		ClientEncryptionEnabled: true,
	}
	addr := ni.CQLAddr(fallback)
	golden := net.JoinHostPort(ni.ListenAddress, ni.NativeTransportPortSsl)
	if addr != golden {
		t.Errorf("expected %s address, got %s", golden, addr)
	}
	if connections.Load() == 0 {
		t.Error("expected connection during figuring out CQL port")
	}
}
