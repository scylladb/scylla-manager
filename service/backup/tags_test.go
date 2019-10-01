// Copyright (C) 2017 ScyllaDB

package backup

import "testing"

func TestSnapshotTag(t *testing.T) {
	tag := newSnapshotTag()
	t.Log(tag)
	if !isSnapshotTag(tag) {
		t.Fatalf("isSnapshotTag() did not claim newSnapshotTag() output %s", tag)
	}
}
