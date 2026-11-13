// Copyright (C) 2026 ScyllaDB

// Package schemaskip lists the rows of DESCRIBE SCHEMA WITH INTERNALS that
// a schema restore does not apply. Scylla Manager's restore schema worker and
// ScyllaDB Cloud, which shows a snapshot's schema to the user, share this list,
// so both describe the same restore.
package schemaskip

import "strings"

const (
	// ReplicatedKeysKeyspace stores encryption at rest keys of the replicated key provider.
	// See https://github.com/scylladb/scylla-enterprise/issues/4168.
	ReplicatedKeysKeyspace = "system_replicated_keys"
	// AuditKeyspace stores audit data. It lacks the "system" prefix
	// and can be altered by users, but it's still an internal keyspace.
	// See https://github.com/scylladb/scylla-manager/issues/4748.
	AuditKeyspace = "audit"
	// LWTStateTableSuffix is the suffix of the colocated table storing
	// LWT state of a tablet table (ScyllaDB 2025.4+).
	// See https://github.com/scylladb/scylla-manager/issues/4732.
	LWTStateTableSuffix = "$paxos"
)

// Skip reports whether a schema restore skips the described schema row
// with the given keyspace, type and name.
// It does not cover Alternator tables, which are recognised by the
// Alternator schema file and restored through the Alternator API.
func Skip(keyspace, rowType, name string) bool {
	switch {
	case keyspace == "":
		// Scylla 6.3 added roles and service levels to the output of
		// DESC SCHEMA WITH INTERNALS (https://github.com/scylladb/scylladb/pull/20168).
		// Those entities do not live in any keyspace, so that's how we identify them.
		return true
	case keyspace == ReplicatedKeysKeyspace, keyspace == AuditKeyspace:
		return true
	case rowType == "table" && strings.HasSuffix(unquote(name), LWTStateTableSuffix):
		return true
	default:
		return false
	}
}

func unquote(name string) string {
	return strings.TrimPrefix(strings.TrimSuffix(name, `"`), `"`)
}
