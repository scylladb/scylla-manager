// Copyright (C) 2017 ScyllaDB

package backup

import "testing"

func TestClaimTag(t *testing.T) {
	tag := newSnapshotTag()
	t.Log(tag)
	if !claimTag(tag) {
		t.Fatalf("claimTag() did not claim newSnapshotTag() output %s", tag)
	}
}
