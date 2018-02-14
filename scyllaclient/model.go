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

// Config is a node run configuration.
type Config map[string]interface{}

// ShardCount returns value for shard_count key as uint.
func (c Config) ShardCount() (uint, bool) {
	v, ok := c["shard_count"].(float64)
	return uint(v), ok
}

// TokenRange specifies which hosts hold data between start and end token.
type TokenRange struct {
	StartToken int64
	EndToken   int64
	Hosts      map[string][]string
}
