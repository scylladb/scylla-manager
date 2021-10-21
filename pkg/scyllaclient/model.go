// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"strings"

	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/util/slice"
	"github.com/scylladb/scylla-manager/swagger/gen/scylla/v1/models"
)

// NodeStatus represents nodetool Status=Up/Down.
type NodeStatus string

// NodeStatus enumeration.
const (
	NodeStatusUp   NodeStatus = "UP"
	NodeStatusDown NodeStatus = "DOWN"
)

func (s NodeStatus) String() string {
	switch s {
	case NodeStatusUp:
		return "U"
	case NodeStatusDown:
		return "D"
	default:
		return ""
	}
}

// NodeState represents nodetool State=Normal/Leaving/Joining/Moving.
type NodeState string

// NodeState enumeration.
const (
	NodeStateNormal  NodeState = ""
	NodeStateLeaving NodeState = "LEAVING"
	NodeStateJoining NodeState = "JOINING"
	NodeStateMoving  NodeState = "MOVING"
)

func (s NodeState) String() string {
	switch s {
	case NodeStateNormal:
		return "N"
	case NodeStateLeaving:
		return "L"
	case NodeStateJoining:
		return "J"
	case NodeStateMoving:
		return "M"
	}
	return ""
}

// NodeStatusInfo represents a nodetool status line.
type NodeStatusInfo struct {
	Datacenter string
	HostID     string
	Addr       string
	Status     NodeStatus
	State      NodeState
}

// IsUN returns true if host is Up and NORMAL meaning it's a fully functional
// live node.
func (s NodeStatusInfo) IsUN() bool {
	return s.Status == NodeStatusUp && s.State == NodeStateNormal
}

// NodeStatusInfoSlice adds functionality to Status response.
type NodeStatusInfoSlice []NodeStatusInfo

// Datacenter returns sub slice containing only nodes from given datacenters.
func (s NodeStatusInfoSlice) Datacenter(dcs []string) NodeStatusInfoSlice {
	m := strset.New(dcs...)
	return s.filter(func(i int) bool {
		return m.Has(s[i].Datacenter)
	})
}

func (s NodeStatusInfoSlice) Status(status NodeStatus) NodeStatusInfoSlice {
	return s.filter(func(i int) bool {
		return s[i].Status == status
	})
}

// Up returns sub slice containing only nodes with status up.
func (s NodeStatusInfoSlice) Up() NodeStatusInfoSlice {
	return s.filter(func(i int) bool {
		return s[i].Status == NodeStatusUp
	})
}

// Down returns sub slice containing only nodes with status down.
func (s NodeStatusInfoSlice) Down() NodeStatusInfoSlice {
	return s.filter(func(i int) bool {
		return s[i].Status == NodeStatusDown
	})
}

// State returns sub slice containing only nodes in a given state.
func (s NodeStatusInfoSlice) State(state NodeState) NodeStatusInfoSlice {
	return s.filter(func(i int) bool {
		return s[i].State == state
	})
}

// Live returns sub slice of nodes in UN state.
func (s NodeStatusInfoSlice) Live() NodeStatusInfoSlice {
	return s.filter(func(i int) bool {
		return s[i].IsUN()
	})
}

func (s NodeStatusInfoSlice) filter(f func(i int) bool) NodeStatusInfoSlice {
	var filtered NodeStatusInfoSlice
	for i, h := range s {
		if f(i) {
			filtered = append(filtered, h)
		}
	}
	return filtered
}

// HostIDs returns slice of IDs of all nodes.
func (s NodeStatusInfoSlice) HostIDs() []string {
	var ids []string
	for _, h := range s {
		ids = append(ids, h.HostID)
	}
	return ids
}

// Hosts returns slice of address of all nodes.
func (s NodeStatusInfoSlice) Hosts() []string {
	var hosts []string
	for _, h := range s {
		hosts = append(hosts, h.Addr)
	}
	return hosts
}

// CommandStatus specifies a result of a command.
type CommandStatus string

// Command statuses.
const (
	CommandRunning    CommandStatus = "RUNNING"
	CommandSuccessful CommandStatus = "SUCCESSFUL"
	CommandFailed     CommandStatus = "FAILED"
)

// Partitioners.
const (
	Murmur3Partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
)

// ReplicationStrategy specifies type of a keyspace replication strategy.
type ReplicationStrategy string

// Replication strategies.
const (
	LocalStrategy           = "org.apache.cassandra.locator.LocalStrategy"
	SimpleStrategy          = "org.apache.cassandra.locator.SimpleStrategy"
	NetworkTopologyStrategy = "org.apache.cassandra.locator.NetworkTopologyStrategy"
)

// Ring describes token ring of a keyspace.
type Ring struct {
	Tokens      []TokenRange
	HostDC      map[string]string
	Replication ReplicationStrategy
}

// Datacenters returns a list of datacenters the keyspace is replicated in.
func (r Ring) Datacenters() []string {
	v := strset.NewWithSize(len(r.HostDC))
	for _, dc := range r.HostDC {
		v.Add(dc)
	}
	return v.List()
}

// HostTokenRanges returns a list of token ranges given host is a replica.
// It returns pairs of token range start and end.
func (r Ring) HostTokenRanges(host string) []int64 {
	var tr []int64
	for _, t := range r.Tokens {
		if slice.ContainsString(t.Replicas, host) {
			tr = append(tr, t.StartToken, t.EndToken)
		}
	}
	return tr
}

// TokenRange describes replicas of a token (range).
type TokenRange struct {
	StartToken int64
	EndToken   int64
	Replicas   []string
}

// Unit describes keyspace and some tables in that keyspace.
type Unit struct {
	Keyspace string
	Tables   []string
}

// ScyllaFeatures specifies features supported by the Scylla version.
type ScyllaFeatures struct {
	RowLevelRepair    bool
	RepairLongPolling bool
}

type gossipApplicationState int32

const (
	// Reference: https://github.com/scylladb/scylla/blob/master/gms/application_state.hh

	gossipSupportedFeatures gossipApplicationState = 14
)

type featureFlag = string

const (
	rowLevelRepair featureFlag = "ROW_LEVEL_REPAIR"
)

func makeScyllaFeatures(endpointStates []*models.EndpointState) map[string]ScyllaFeatures {
	supportedFeatures := make(map[string][]string, len(endpointStates))

	for _, state := range endpointStates {
		for _, as := range state.ApplicationState {
			if gossipApplicationState(as.ApplicationState) == gossipSupportedFeatures {
				supportedFeatures[state.Addrs] = strings.Split(as.Value, ",")
			}
		}
	}

	sfs := make(map[string]ScyllaFeatures, len(supportedFeatures))
	for host, sf := range supportedFeatures {
		sfs[host] = ScyllaFeatures{
			RowLevelRepair: slice.ContainsString(sf, rowLevelRepair),
		}
	}

	return sfs
}
