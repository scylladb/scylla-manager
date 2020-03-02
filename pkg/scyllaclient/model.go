// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"strings"

	"github.com/hashicorp/go-version"
	"github.com/scylladb/go-set/strset"
)

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

// Datacenters returs a list of datacenters the keyspace is replicated in.
func (r Ring) Datacenters() []string {
	v := strset.NewWithSize(len(r.HostDC))
	for _, dc := range r.HostDC {
		v.Add(dc)
	}
	return v.List()
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

func makeScyllaFeatures(ver string) (ScyllaFeatures, error) {
	// Trim build version suffix as it breaks constraints.
	v, err := version.NewSemver(strings.Split(ver, "-")[0])
	if err != nil {
		return ScyllaFeatures{}, err
	}

	rowLevelRepair, err := version.NewConstraint(">= 3.1, < 2000")
	if err != nil {
		panic(err)
	}
	return ScyllaFeatures{
		RowLevelRepair: rowLevelRepair.Check(v),
	}, nil
}
