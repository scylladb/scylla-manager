// Copyright (C) 2025 ScyllaDB

package table

// CQLTable describe CQL representation of a table.
type CQLTable struct {
	Keyspace string
	Name     string
}

// LWTStateTableSuffix describes a suffix of the colocated table storing LWT state.
// Starting from ScyllaDB 2025.4, using LWT on a tablet table 'ks.tab' results in
// creation of 'ks.tab$paxos' table used for storing LWT state.
// For older ScyllaDB versions or vnodes, LWT state is stored in 'system.paxos' table.
// See https://docs.scylladb.com/manual/stable/features/lwt.html#paxos-state-tables.
const LWTStateTableSuffix = "$paxos"

// LWTSystemTable describes the system table storing LWT state.
// See LWTStateTableSuffix for more details.
var LWTSystemTable = CQLTable{
	Keyspace: "system",
	Name:     "paxos",
}
