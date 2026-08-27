// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"net/netip"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
)

func TestDCHostPicker(t *testing.T) {
	nodeConfig := map[netip.Addr]configcache.NodeConfig{
		netip.MustParseAddr("192.168.100.11"): testNodeConfig("dc1"),
		netip.MustParseAddr("192.168.100.12"): testNodeConfig("dc1"),
		netip.MustParseAddr("192.168.100.13"): testNodeConfig("dc1"),
		netip.MustParseAddr("192.168.100.21"): testNodeConfig("dc2"),
		netip.MustParseAddr("192.168.100.22"): testNodeConfig("dc2"),
	}
	p := newDCHostPicker(nodeConfig)

	if _, err := p.pick("dc3"); err == nil {
		t.Fatal("expected error for unknown datacenter, got nil")
	}

	const rounds = 3
	for _, dc := range []string{"dc1", "dc2"} {
		dcSize := len(p.dcHosts[dc])
		hits := make(map[netip.Addr]int)
		for range rounds * dcSize {
			ip, err := p.pick(dc)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if nodeConfig[ip].Datacenter != dc {
				t.Fatalf("picked host %s from datacenter %s, expected %s", ip, nodeConfig[ip].Datacenter, dc)
			}
			hits[ip]++
		}
		if len(hits) != dcSize {
			t.Fatalf("expected all %d hosts from %s to be picked, got %d", dcSize, dc, len(hits))
		}
		for ip, cnt := range hits {
			if cnt != rounds {
				t.Fatalf("expected host %s to be picked %d times, got %d", ip, rounds, cnt)
			}
		}
	}
}

func testNodeConfig(dc string) configcache.NodeConfig {
	nc := configcache.NodeConfig{}
	nc.Datacenter = dc
	return nc
}
