// Copyright (C) 2025 ScyllaDB

package backup_test

import (
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func TestNewSnapshotTag(t *testing.T) {
	start := timeutc.Now()
	tag1 := backup.NewSnapshotTag()
	tag2 := backup.NewSnapshotTag()
	tag3 := backup.NewSnapshotTag()
	end := timeutc.Now()

	if tag1 == tag2 || tag1 == tag3 || tag2 == tag3 {
		t.Fatalf("Expected different snaphsot tags, got: %s, %s, %s", tag1, tag2, tag3)
	}
	d := end.Sub(start)
	if d < 2*time.Second || d >= 3*time.Second {
		t.Fatalf("Expected to sleep for 2 (#tags - 1) seconds, got: %d", d)
	}
}
