// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"net"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
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

	var (
		connections atomic.Int64
		ready       = make(chan struct{})
	)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			connections.Inc()
			_ = c.Close()
			close(ready)
			break
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

	<-ready
	if c := connections.Load(); c == 0 {
		t.Errorf("expected connection during figuring out CQL port got %d", c)
	}
}

func TestNodeInfoSupportsAlternatorQuery(t *testing.T) {
	t.Parallel()

	table := []struct {
		Version string
		Golden  bool
	}{
		{
			Version: "2019.1.2-0.20190814.2772d52",
			Golden:  false,
		},
		{
			Version: "3.1.0-0.20191012.9c3cdded9",
			Golden:  false,
		},
		{
			Version: "3.2.2-0.20200222.0b23e7145d0",
			Golden:  false,
		},
		{
			Version: "666.development",
			Golden:  true,
		},
		{
			Version: "9999.enterprise_dev",
			Golden:  true,
		},
		{
			Version: "3.3.rc2",
			Golden:  false,
		},
		{
			Version: "3.1.hotfix",
			Golden:  false,
		},
		{
			Version: "3.0.rc8",
			Golden:  false,
		},
		{
			Version: "2019.1.1-2.reader_concurrency_semaphore.20190730.f0071c669",
			Golden:  false,
		},
		{
			Version: "2019.1.5-2.many_tables.20200311.be960ed96",
			Golden:  false,
		},
		{
			Version: "4.0.0",
			Golden:  false,
		},
		{
			Version: "4.1.0",
			Golden:  true,
		},
		{
			Version: "2020.1",
			Golden:  false,
		},
		{
			Version: "2021.1",
			Golden:  false,
		},
	}

	for i := range table {
		test := table[i]

		supports, err := scyllaclient.NodeInfo{ScyllaVersion: test.Version}.SupportsAlternatorQuery()
		if err != nil {
			t.Error(err)
		}

		if !cmp.Equal(supports, test.Golden) {
			t.Errorf("SupportsAlternatorQuery(%s) = %+v, expected %+v", test.Version, supports, test.Golden)
		}
	}
}
