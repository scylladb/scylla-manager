// Copyright (C) 2025 ScyllaDB

package backup_test

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestParseSchemaFileName(t *testing.T) {
	testCases := []struct {
		name       string
		schemaFile string
		taskID     uuid.UUID
		tag        string
		err        bool
	}{
		{
			name:       "valid cql schema file",
			schemaFile: "task_1702a53a-2ecd-44b2-9c79-c45653a49d31_tag_sm_20250527161648UTC_schema_with_internals.json.gz",
			taskID:     uuid.MustParse("1702a53a-2ecd-44b2-9c79-c45653a49d31"),
			tag:        "sm_20250527161648UTC",
		},
		{
			name:       "invalid format",
			schemaFile: "invalid_schema_file.json.gz",
			err:        true,
		},
		{
			name:       "invalid task ID",
			schemaFile: "task_notauuid_tag_sm_20250527161648UTC_schema_with_internals.json.gz",
			err:        true,
		},
		{
			name:       "invalid tag",
			schemaFile: "task_1702a53a-2ecd-44b2-9c79-c45653a49d31_tag_invalidtag_schema_with_internals.json.gz",
			err:        true,
		},
		{
			name:       "valid alternator schema file",
			schemaFile: "task_1702a53a-2ecd-44b2-9c79-c45653a49d31_tag_sm_20250527161648UTC_alternator_schema.json.gz",
			taskID:     uuid.MustParse("1702a53a-2ecd-44b2-9c79-c45653a49d31"),
			tag:        "sm_20250527161648UTC",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			taskID, tag, err := backup.ParseSchemaFileName(tc.schemaFile)
			if tc.err && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.taskID != taskID {
				t.Fatalf("got task ID %q, expected %q", taskID, tc.taskID)
			}
			if tc.tag != tag {
				t.Fatalf("got tag %q, expected %q", tag, tc.tag)
			}
		})
	}
}
