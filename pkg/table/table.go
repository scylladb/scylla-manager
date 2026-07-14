// Copyright (C) 2026 ScyllaDB

package table

import (
	"regexp"
)

// CQLTable describe CQL representation of a table.
type CQLTable struct {
	Keyspace string
	Name     string
}

func (t CQLTable) String() string {
	return t.Keyspace + "." + t.Name
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

// ScyllaBackupTables lists internal scylla tables used for storing
// scylla orchestrated backup state. These tables should be excluded
// from SM backup/restore procedures.
var ScyllaBackupTables = []CQLTable{
	{Keyspace: "system_distributed", Name: "snapshot_sstables"},
	{Keyspace: "system_distributed", Name: "snapshot_remote_locations"},
	{Keyspace: "system_distributed", Name: "snapshots"},
	{Keyspace: "system_distributed", Name: "snapshot_nodes"},
	{Keyspace: "system_distributed", Name: "snapshot_keyspaces"},
	{Keyspace: "system_distributed", Name: "snapshot_tables"},
	{Keyspace: "system_distributed", Name: "snapshot_tablets"},
}

// AuditKeyspace stores audit data. It lacks "system" prefix
// and can be altered by users, but it's still an internal keyspace.
// See https://docs.scylladb.com/manual/stable/operating-scylla/security/auditing.html.
const AuditKeyspace = "audit"

// AuditTable describes the system table storing audit data.
var AuditTable = CQLTable{
	Keyspace: AuditKeyspace,
	Name:     "audit_log",
}

// MaterializedViewBackingIndex given the logical index name
// returns the name of the materialized view backing the index.
func MaterializedViewBackingIndex(indexName string) string {
	return indexName + "_index"
}

// CDCTableSuffix describes a suffix of the table storing CDC log entries.
// CDC table lives in the same user keyspace as its parent table.
const CDCTableSuffix = "_scylla_cdc_log"

var systemCDCTableRegex = regexp.MustCompile(`(^|_)cdc(_|$)`)

// IsCDCSystemTable checks if table living in internal
// scylla keyspace is a system CDC table.
func IsCDCSystemTable(name string) bool {
	return systemCDCTableRegex.MatchString(name)
}
