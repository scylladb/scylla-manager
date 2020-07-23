// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"regexp"
	"strings"

	"github.com/hashicorp/go-version"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
)

// NodeStatus represents nodetool Status=Up/Down.
type NodeStatus bool

// NodeStatus enumeration
const (
	NodeStatusUp   NodeStatus = true
	NodeStatusDown NodeStatus = false
)

func (s NodeStatus) String() string {
	if s {
		return "U"
	}
	return "D"
}

// NodeState represents nodetool State=Normal/Leaving/Joining/Moving
type NodeState string

// NodeState enumeration
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

// Datacenter resturns sub slice containing only nodes from given datacenters.
func (s NodeStatusInfoSlice) Datacenter(dcs []string) NodeStatusInfoSlice {
	m := strset.New(dcs...)

	var filtered NodeStatusInfoSlice
	for _, h := range s {
		if m.Has(h.Datacenter) {
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

// LiveHosts returns slice of address of nodes in UN state.
func (s NodeStatusInfoSlice) LiveHosts() []string {
	var hosts []string
	for _, h := range s {
		if h.IsUN() {
			hosts = append(hosts, h.Addr)
		}
	}
	return hosts
}

// DownHosts returns slice of address of nodes that are down.
func (s NodeStatusInfoSlice) DownHosts() []string {
	var hosts []string
	for _, h := range s {
		if h.Status == NodeStatusDown {
			hosts = append(hosts, h.Addr)
		}
	}
	return hosts
}

// CommandStatus specifies a result of a command
type CommandStatus string

// Command statuses
const (
	CommandRunning    CommandStatus = "RUNNING"
	CommandSuccessful CommandStatus = "SUCCESSFUL"
	CommandFailed     CommandStatus = "FAILED"
)

// Partitioners
const (
	Murmur3Partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
)

// ReplicationStrategy specifies type of a keyspace replication strategy.
type ReplicationStrategy string

// Replication strategies
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
		if contains(t.Replicas, host) {
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
	RowLevelRepair bool
}

var masterScyllaFeatures = ScyllaFeatures{
	RowLevelRepair: true,
}

const (
	scyllaMasterVersion           = "666.development"
	scyllaEnterpriseMasterVersion = "9999.enterprise_dev"
)

var (
	verRe = regexp.MustCompile(`^[0-9]+\.[0-9]+(\.[0-9]+)?`)
)

func makeScyllaFeatures(ver string) (ScyllaFeatures, error) {
	// Trim build version suffix as it breaks constraints
	ver = strings.Split(ver, "-")[0]

	// Detect master builds
	if ver == scyllaMasterVersion || ver == scyllaEnterpriseMasterVersion {
		return masterScyllaFeatures, nil
	}

	if verRe.FindString(ver) == "" {
		return ScyllaFeatures{}, errors.Errorf("Unsupported Scylla version: %s", ver)
	}

	// Extract only version number
	ver = verRe.FindString(ver)

	v, err := version.NewSemver(ver)
	if err != nil {
		return ScyllaFeatures{}, err
	}

	rowLevelRepairOpenSource, err := version.NewConstraint(">= 3.1, < 2000")
	if err != nil {
		panic(err) // must
	}

	rowLevelRepairEnterprise, err := version.NewConstraint(">= 2020")
	if err != nil {
		panic(err) // must
	}
	return ScyllaFeatures{
		RowLevelRepair: rowLevelRepairOpenSource.Check(v) || rowLevelRepairEnterprise.Check(v),
	}, nil
}

func contains(v []string, s string) bool {
	for _, e := range v {
		if e == s {
			return true
		}
	}
	return false
}
