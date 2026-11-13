// Copyright (C) 2026 ScyllaDB

package schemaskip

import "testing"

func TestSkip(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		keyspace string
		rowType  string
		rowName  string
		want     bool
	}{
		{name: "role has no keyspace", keyspace: "", rowType: "role", rowName: "r1", want: true},
		{name: "service level has no keyspace", keyspace: "", rowType: "service_level", rowName: "sl", want: true},
		{name: "replicated keys keyspace", keyspace: "system_replicated_keys", rowType: "keyspace", rowName: "system_replicated_keys", want: true},
		{name: "replicated keys table", keyspace: "system_replicated_keys", rowType: "table", rowName: "encrypted_keys", want: true},
		{name: "audit keyspace", keyspace: "audit", rowType: "table", rowName: "audit_log", want: true},
		{name: "LWT state table", keyspace: "ks", rowType: "table", rowName: "tab$paxos", want: true},
		{name: "quoted LWT state table", keyspace: "ks", rowType: "table", rowName: `"tab$paxos"`, want: true},
		// Only tables carry the suffix; a view with such a name is a user object.
		{name: "view with paxos suffix", keyspace: "ks", rowType: "view", rowName: "v$paxos", want: false},
		{name: "user keyspace", keyspace: "ks", rowType: "keyspace", rowName: "ks", want: false},
		{name: "user table", keyspace: "ks", rowType: "table", rowName: "tab", want: false},
		{name: "user type", keyspace: "ks", rowType: "type", rowName: "address", want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Skip(tc.keyspace, tc.rowType, tc.rowName); got != tc.want {
				t.Fatalf("Skip(%q, %q, %q) = %v, want %v", tc.keyspace, tc.rowType, tc.rowName, got, tc.want)
			}
		})
	}
}
