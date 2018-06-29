// Copyright (C) 2017 ScyllaDB

package scyllaclient

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

// TokenRange specifies which hosts hold data between start and end token.
type TokenRange struct {
	StartToken int64
	EndToken   int64
	Hosts      map[string][]string
}

// DC specifies a datacenter and it's hosts.
type DC struct {
	Name  string
	Hosts []string
}
