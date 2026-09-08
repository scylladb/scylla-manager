// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"net/netip"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
)

func hostPickerNodeConfig(hostDC map[string]string) map[netip.Addr]configcache.NodeConfig {
	out := make(map[netip.Addr]configcache.NodeConfig, len(hostDC))
	for host, dc := range hostDC {
		out[netip.MustParseAddr(host)] = configcache.NodeConfig{Datacenter: dc}
	}
	return out
}

func TestHostPickerPick(t *testing.T) {
	dc1Hosts := map[netip.Addr]struct{}{
		netip.MustParseAddr("192.168.100.11"): {},
		netip.MustParseAddr("192.168.100.12"): {},
		netip.MustParseAddr("192.168.100.13"): {},
	}
	dc2Hosts := map[netip.Addr]struct{}{
		netip.MustParseAddr("192.168.100.21"): {},
		netip.MustParseAddr("192.168.100.22"): {},
	}
	p := newHostPicker(hostPickerNodeConfig(map[string]string{
		"192.168.100.11": "dc1",
		"192.168.100.12": "dc1",
		"192.168.100.13": "dc1",
		"192.168.100.21": "dc2",
		"192.168.100.22": "dc2",
	}))

	// Picking from a single DC spreads the load evenly over all of its hosts
	picked := make(map[netip.Addr]int)
	rounds := 3
	for range rounds * len(dc1Hosts) {
		ip, err := p.pick("dc1")
		if err != nil {
			t.Fatalf("pick(dc1): %s", err)
		}
		if _, ok := dc1Hosts[ip]; !ok {
			t.Fatalf("pick(dc1) = %s, expected a host from dc1", ip)
		}
		picked[ip]++
	}
	for ip, cnt := range picked {
		if cnt != rounds {
			t.Fatalf("host %s picked %d times, expected each dc1 host to be picked %d times", ip, cnt, rounds)
		}
	}

	// With all dc1 hosts already utilized, picking from both DCs
	// returns a free dc2 host
	for range rounds * len(dc2Hosts) {
		ip, err := p.pick("dc1", "dc2")
		if err != nil {
			t.Fatalf("pick(dc1, dc2): %s", err)
		}
		if _, ok := dc2Hosts[ip]; !ok {
			t.Fatalf("pick(dc1, dc2) = %s, expected a free host from dc2", ip)
		}
	}

	// Unknown or missing DC results in an error
	if _, err := p.pick("dc3"); err == nil {
		t.Fatal("pick(dc3): expected error for unknown datacenter")
	}
	if _, err := p.pick(); err == nil {
		t.Fatal("pick(): expected error for no datacenters")
	}
}

func TestHostPickerRelease(t *testing.T) {
	hostDC := map[string]string{
		"192.168.100.11": "dc1",
		"192.168.100.12": "dc1",
		"192.168.100.13": "dc1",
	}
	p := newHostPicker(hostPickerNodeConfig(hostDC))

	// Utilize all hosts
	for range len(hostDC) {
		if _, err := p.pick("dc1"); err != nil {
			t.Fatalf("pick(dc1): %s", err)
		}
	}

	for h := range hostDC {
		// After release, the released host is the least utilized one
		released := netip.MustParseAddr(h)
		p.release(released)
		ip, err := p.pick("dc1")
		if err != nil {
			t.Fatalf("pick(dc1): %s", err)
		}
		if ip != released {
			t.Fatalf("pick(dc1) = %s, expected released host %s", ip, released)
		}
	}
}
