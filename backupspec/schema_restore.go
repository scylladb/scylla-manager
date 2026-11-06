// Copyright (C) 2026 ScyllaDB

package backupspec

import "strings"

// LWTStateTableSuffix describes a suffix of the colocated table storing LWT state.
// Starting from ScyllaDB 2025.4, using LWT on a tablet table 'ks.tab' results in
// creation of 'ks.tab$paxos' table used for storing LWT state.
// See https://docs.scylladb.com/manual/stable/features/lwt.html#paxos-state-tables.
const LWTStateTableSuffix = "$paxos"

// AuditKeyspace stores audit data. It lacks "system" prefix
// and can be altered by users, but it's still an internal keyspace.
// See https://docs.scylladb.com/manual/stable/operating-scylla/security/auditing.html.
const AuditKeyspace = "audit"

// ReplicatedKeysKeyspace stores encryption keys of the replicated key provider.
// See https://github.com/scylladb/scylla-enterprise/issues/4168.
const ReplicatedKeysKeyspace = "system_replicated_keys"

// AlternatorKeyspacePrefix is the prefix of the CQL keyspace in which
// each Alternator table lives ('alternator_<table>').
const AlternatorKeyspacePrefix = "alternator_"

// AlternatorKeyspaceName returns the name of the CQL keyspace of the Alternator table.
func AlternatorKeyspaceName(table string) string {
	return AlternatorKeyspacePrefix + table
}

// IsLWTStateTable checks if table (possibly quoted) is a colocated LWT state table.
func IsLWTStateTable(table string) bool {
	return strings.HasSuffix(unquoteCQLName(table), LWTStateTableSuffix)
}

// SchemaSkipReason explains why schema restore doesn't apply a row of the CQL schema file
// (the output of DESCRIBE SCHEMA WITH INTERNALS). Empty reason means that the row is applied.
type SchemaSkipReason string

// SchemaSkipReason enumeration.
const (
	// SchemaNotSkipped means that the row is applied with CQL.
	SchemaNotSkipped SchemaSkipReason = ""
	// SchemaSkipRoleOrServiceLevel covers rows with empty keyspace.
	// Scylla 6.3 added roles and service levels to the output of
	// DESC SCHEMA WITH INTERNALS (https://github.com/scylladb/scylladb/pull/20168).
	// Those entities do not live in any particular keyspace and are skipped
	// until their restoration is properly supported.
	SchemaSkipRoleOrServiceLevel SchemaSkipReason = "role_or_service_level"
	// SchemaSkipReplicatedKeys covers the system_replicated_keys keyspace.
	SchemaSkipReplicatedKeys SchemaSkipReason = "replicated_keys"
	// SchemaSkipLWTStateTable covers colocated LWT state tables
	// (https://github.com/scylladb/scylla-manager/issues/4732).
	SchemaSkipLWTStateTable SchemaSkipReason = "lwt_state_table"
	// SchemaSkipAuditKeyspace covers the audit keyspace
	// (https://github.com/scylladb/scylla-manager/issues/4748).
	SchemaSkipAuditKeyspace SchemaSkipReason = "audit_keyspace"
	// SchemaSkipAlternator covers the CQL representation of Alternator tables.
	// Those tables are not skipped by schema restore - they are created with
	// the Alternator API from the Alternator schema file instead of with CQL.
	SchemaSkipAlternator SchemaSkipReason = "alternator"
)

// Skipped returns true if the row isn't applied with CQL.
func (r SchemaSkipReason) Skipped() bool {
	return r != SchemaNotSkipped
}

// SchemaRestoreFilter decides which rows of the CQL schema file are applied
// with CQL by schema restore. It's shared with other consumers of backups,
// so that they can tell what a schema restore creates.
type SchemaRestoreFilter struct {
	alternatorKeyspaces map[string]struct{}
}

// NewSchemaRestoreFilter returns SchemaRestoreFilter for a backup with
// the given Alternator schema file (zero value if the backup has none).
func NewSchemaRestoreFilter(alternator AlternatorSchema) SchemaRestoreFilter {
	ks := make(map[string]struct{})
	for _, t := range AlternatorTablesToRestore(alternator) {
		ks[AlternatorKeyspaceName(*t.Describe.TableName)] = struct{}{}
	}
	return SchemaRestoreFilter{alternatorKeyspaces: ks}
}

// AlternatorTablesToRestore returns tables from the Alternator schema file
// which are created with the Alternator API by schema restore.
// LWT state tables shouldn't be listed with Alternator API,
// but this bug exists in early 2025.4 versions (#4732), so they are left out.
func AlternatorTablesToRestore(alternator AlternatorSchema) []AlternatorTableSchema {
	var out []AlternatorTableSchema
	for _, t := range alternator.Tables {
		if t.Describe == nil || t.Describe.TableName == nil {
			continue
		}
		if IsLWTStateTable(*t.Describe.TableName) {
			continue
		}
		out = append(out, t)
	}
	return out
}

// Skip returns the reason why schema restore doesn't apply the CQL schema
// file row with the given keyspace, type and name (as returned by
// DESCRIBE SCHEMA WITH INTERNALS, possibly quoted), or SchemaNotSkipped.
func (f SchemaRestoreFilter) Skip(keyspace, rowType, name string) SchemaSkipReason {
	if keyspace == "" {
		return SchemaSkipRoleOrServiceLevel
	}
	ks := unquoteCQLName(keyspace)
	if ks == ReplicatedKeysKeyspace {
		return SchemaSkipReplicatedKeys
	}
	if rowType == "table" && IsLWTStateTable(name) {
		return SchemaSkipLWTStateTable
	}
	if ks == AuditKeyspace {
		return SchemaSkipAuditKeyspace
	}
	if _, ok := f.alternatorKeyspaces[ks]; ok {
		return SchemaSkipAlternator
	}
	return SchemaNotSkipped
}

func unquoteCQLName(name string) string {
	return strings.TrimPrefix(strings.TrimSuffix(name, `"`), `"`)
}
