// Copyright (C) 2026 ScyllaDB

package topology

import (
	"context"
	"fmt"
	"iter"
	"sort"
	"strings"

	"github.com/gocql/gocql"
)

// ClusterTopology describes the topology of a Scylla cluster.
type ClusterTopology struct {
	DCs map[string]DCTopology
}

// DCTopology describes the topology of a single datacenter.
type DCTopology struct {
	DC    string
	Racks map[string]RackTopology
	Nodes int
}

// RackTopology describes the topology of a single rack within a datacenter.
type RackTopology struct {
	DC    string
	Rack  string
	Nodes int
}

func (rt RackTopology) String() string {
	return fmt.Sprintf("{DC: %s, Rack: %s, Nodes: %d}", rt.DC, rt.Rack, rt.Nodes)
}

func (dt DCTopology) String() string {
	racks := make([]string, 0, len(dt.Racks))
	rackNames := make([]string, 0, len(dt.Racks))
	for name := range dt.Racks {
		rackNames = append(rackNames, name)
	}
	sort.Strings(rackNames)
	for _, name := range rackNames {
		racks = append(racks, dt.Racks[name].String())
	}
	return fmt.Sprintf("{DC: %s, Nodes: %d, Racks: [%s]}", dt.DC, dt.Nodes, strings.Join(racks, ", "))
}

func (ct ClusterTopology) String() string {
	dcs := make([]string, 0, len(ct.DCs))
	dcNames := make([]string, 0, len(ct.DCs))
	for name := range ct.DCs {
		dcNames = append(dcNames, name)
	}
	sort.Strings(dcNames)
	for _, name := range dcNames {
		dcs = append(dcs, ct.DCs[name].String())
	}
	return fmt.Sprintf("{DCs: [%s]}", strings.Join(dcs, ", "))
}

// ContainsDCsAndRacks checks if all DCs from other are present in ct,
// and for each such DC the set of rack names is exactly the same.
// Node counts are ignored. ct may have additional DCs not present in other.
func (ct ClusterTopology) ContainsDCsAndRacks(other ClusterTopology) bool {
	for dc, otherDCTopo := range other.DCs {
		dcTopo, ok := ct.DCs[dc]
		if !ok {
			return false
		}
		if len(dcTopo.Racks) != len(otherDCTopo.Racks) {
			return false
		}
		for rack := range otherDCTopo.Racks {
			if _, ok := dcTopo.Racks[rack]; !ok {
				return false
			}
		}
	}
	return true
}

// BuildClusterTopology constructs a ClusterTopology from a dc/rack pair iterator.
func BuildClusterTopology(dcRackIter iter.Seq2[string, string]) ClusterTopology {
	clusterTopology := ClusterTopology{
		DCs: make(map[string]DCTopology),
	}
	for dc, rack := range dcRackIter {
		// Initialize DC info if needed
		if _, ok := clusterTopology.DCs[dc]; !ok {
			clusterTopology.DCs[dc] = DCTopology{
				DC:    dc,
				Racks: make(map[string]RackTopology),
			}
		}
		dci := clusterTopology.DCs[dc]
		// Update rack info
		ri := dci.Racks[rack]
		ri.DC = dc
		ri.Rack = rack
		ri.Nodes++
		// Update dc info
		dci.Nodes++
		// Update nested structure
		dci.Racks[rack] = ri
		clusterTopology.DCs[dc] = dci
	}
	return clusterTopology
}

// IterWithErr wraps an iterator with an error that may be set during iteration.
type IterWithErr struct {
	Iter iter.Seq2[string, string]
	Err  error
}

// BuildSessionIter creates a dc/rack iterator from a CQL session querying system.peers and system.local.
// singleHostSession indicates whether session was created with DisableInitialHostLookup or NewSingleHostQueryExecutor,
// as in those cases we don't need to explicitly ensure that the queries will be routed to the same host.
func BuildSessionIter(ctx context.Context, session *gocql.Session, singleHostSession bool) *IterWithErr {
	// Since we need to execute multiple queries to local tables,
	// we need to target the same host for all of them. For regular
	// sessions, we can rely on SetHostID. For single host sessions,
	// GetHosts might return ID of a host to which the session is not
	// connected. Because of that, we can't use SetHostID there, but
	// in those cases queries are already routed to a single host,
	// so it's not a problem.
	var hostID string
	if hosts := session.GetHosts(); len(hosts) > 0 && !singleHostSession {
		hostID = hosts[0].HostID()
	}
	var dc, rack string

	si := new(IterWithErr)
	si.Iter = func(yield func(string, string) bool) {
		it := session.QueryWithContext(ctx, "SELECT data_center, rack FROM system.peers").SetHostID(hostID).Iter()
		for it.Scan(&dc, &rack) {
			yield(dc, rack)
		}
		if err := it.Close(); err != nil {
			si.Err = err
			return
		}
		if err := session.QueryWithContext(ctx, "SELECT data_center, rack FROM system.local").SetHostID(hostID).Scan(&dc, &rack); err != nil {
			si.Err = err
			return
		}
		yield(dc, rack)
	}
	return si
}
