// Copyright (C) 2026 ScyllaDB

package backupspec

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func alternatorTable(name string) AlternatorTableSchema {
	return AlternatorTableSchema{Describe: &types.TableDescription{TableName: &name}}
}

func TestSchemaRestoreFilterSkip(t *testing.T) {
	t.Parallel()

	alternator := AlternatorSchema{
		Tables: []AlternatorTableSchema{
			alternatorTable("tab"),
			alternatorTable("lwt" + LWTStateTableSuffix),
			{}, // Missing description
		},
	}

	testCases := []struct {
		name       string
		alternator AlternatorSchema
		keyspace   string
		rowType    string
		rowName    string
		expected   SchemaSkipReason
	}{
		{
			name:     "user keyspace",
			keyspace: "ks",
			rowType:  "keyspace",
			rowName:  "ks",
			expected: SchemaNotSkipped,
		},
		{
			name:     "user table",
			keyspace: "ks",
			rowType:  "table",
			rowName:  "tab",
			expected: SchemaNotSkipped,
		},
		{
			name:     "role",
			keyspace: "",
			rowType:  "role",
			rowName:  "r1",
			expected: SchemaSkipRoleOrServiceLevel,
		},
		{
			name:     "service level",
			keyspace: "",
			rowType:  "service_level",
			rowName:  "sl1",
			expected: SchemaSkipRoleOrServiceLevel,
		},
		{
			name:     "replicated keys keyspace",
			keyspace: "system_replicated_keys",
			rowType:  "keyspace",
			rowName:  "system_replicated_keys",
			expected: SchemaSkipReplicatedKeys,
		},
		{
			name:     "replicated keys table",
			keyspace: "system_replicated_keys",
			rowType:  "table",
			rowName:  "encrypted_keys",
			expected: SchemaSkipReplicatedKeys,
		},
		{
			name:     "LWT state table",
			keyspace: "ks",
			rowType:  "table",
			rowName:  `"tab$paxos"`,
			expected: SchemaSkipLWTStateTable,
		},
		{
			name:     "LWT suffix on non table row",
			keyspace: "ks",
			rowType:  "index",
			rowName:  `"tab$paxos"`,
			expected: SchemaNotSkipped,
		},
		{
			name:     "audit keyspace",
			keyspace: "audit",
			rowType:  "keyspace",
			rowName:  "audit",
			expected: SchemaSkipAuditKeyspace,
		},
		{
			name:     "audit table",
			keyspace: "audit",
			rowType:  "table",
			rowName:  "audit_log",
			expected: SchemaSkipAuditKeyspace,
		},
		{
			name:       "alternator keyspace",
			alternator: alternator,
			keyspace:   `"alternator_tab"`,
			rowType:    "keyspace",
			rowName:    `"alternator_tab"`,
			expected:   SchemaSkipAlternator,
		},
		{
			name:       "alternator table",
			alternator: alternator,
			keyspace:   "alternator_tab",
			rowType:    "table",
			rowName:    "tab",
			expected:   SchemaSkipAlternator,
		},
		{
			name:       "alternator keyspace missing in alternator schema",
			alternator: alternator,
			keyspace:   "alternator_other",
			rowType:    "keyspace",
			rowName:    "alternator_other",
			expected:   SchemaNotSkipped,
		},
		{
			name:       "alternator keyspace without alternator schema",
			alternator: AlternatorSchema{},
			keyspace:   "alternator_tab",
			rowType:    "keyspace",
			rowName:    "alternator_tab",
			expected:   SchemaNotSkipped,
		},
		{
			name:       "alternator LWT state table listed in alternator schema",
			alternator: alternator,
			keyspace:   "alternator_lwt$paxos",
			rowType:    "keyspace",
			rowName:    "alternator_lwt$paxos",
			expected:   SchemaNotSkipped,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := NewSchemaRestoreFilter(tc.alternator)
			got := f.Skip(tc.keyspace, tc.rowType, tc.rowName)
			if got != tc.expected {
				t.Fatalf("Skip(%q, %q, %q) = %q, expected %q", tc.keyspace, tc.rowType, tc.rowName, got, tc.expected)
			}
			if got.Skipped() != (tc.expected != SchemaNotSkipped) {
				t.Fatalf("Skipped() = %v for %q", got.Skipped(), got)
			}
		})
	}
}

func TestAlternatorTablesToRestore(t *testing.T) {
	t.Parallel()

	schema := AlternatorSchema{
		Tables: []AlternatorTableSchema{
			alternatorTable("a"),
			alternatorTable("a" + LWTStateTableSuffix),
			{},
			{Describe: &types.TableDescription{}},
			alternatorTable("b"),
		},
	}
	got := AlternatorTablesToRestore(schema)
	if len(got) != 2 || *got[0].Describe.TableName != "a" || *got[1].Describe.TableName != "b" {
		t.Fatalf("AlternatorTablesToRestore() = %v, expected tables a and b", got)
	}
}
