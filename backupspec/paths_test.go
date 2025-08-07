// Copyright (C) 2025 ScyllaDB

package backupspec

import (
	"fmt"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestAlternatorSchemaPaths(t *testing.T) {
	var (
		clusterID   = uuid.NewTime()
		taskID      = uuid.NewTime()
		snapshotTag = SnapshotTagAt(timeutc.Now())
	)

	t.Run("AlternatorSchemaFileSuffixWithTag", func(t *testing.T) {
		expected := fmt.Sprintf("tag_%s_alternator_schema.json.gz", snapshotTag)
		if got := AlternatorSchemaFileSuffixWithTag(snapshotTag); got != expected {
			t.Fatalf("Expected: %s, got: %s", expected, got)
		}
	})

	t.Run("AlternatorSchemaFile", func(t *testing.T) {
		expected := fmt.Sprintf("task_%s_tag_%s_alternator_schema.json.gz", taskID, snapshotTag)
		if got := AlternatorSchemaFile(taskID, snapshotTag); got != expected {
			t.Fatalf("Expected: %s, got: %s", expected, got)
		}
	})

	t.Run("AlternatorSchemaPath", func(t *testing.T) {
		expected := fmt.Sprintf("backup/schema/cluster/%s/task_%s_tag_%s_alternator_schema.json.gz", clusterID, taskID, snapshotTag)
		if got := AlternatorSchemaPath(clusterID, taskID, snapshotTag); got != expected {
			t.Fatalf("Expected: %s, got: %s", expected, got)
		}
	})
}
