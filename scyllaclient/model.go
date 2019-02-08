// Copyright (C) 2017 ScyllaDB

package scyllaclient

import "github.com/scylladb/go-set/strset"

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

// Ring describes token ring of a keyspace.
type Ring struct {
	Tokens []TokenRange
	HostDC map[string]string
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
