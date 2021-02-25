// Copyright (C) 2017 ScyllaDB

package backupspec

import (
	"testing"
	"time"
)

func TestSnapshotTag(t *testing.T) {
	t.Parallel()

	tag := NewSnapshotTag()
	t.Log(tag)
	if !IsSnapshotTag(tag) {
		t.Fatalf("IsSnapshotTag(%s) = false, expected true", tag)
	}
}

func TestSnapshotTagChanges(t *testing.T) {
	t.Parallel()

	t0 := NewSnapshotTag()
	time.Sleep(time.Second)
	t1 := NewSnapshotTag()
	if t0 == t1 {
		t.Fatalf("NewSnapshotTag() = %s; NewSnapshotTag() = %s, expected to be different", t0, t1)
	}
}

func TestSnapshotTagTime(t *testing.T) {
	zero := time.Time{}
	times := []time.Time{
		zero.Add(time.Second),
		zero.Add(time.Minute),
		zero.Add(time.Hour),
		zero.Add(time.Second + time.Minute + time.Hour),
	}

	for _, test := range times {
		tag := SnapshotTagAt(test)
		v, err := SnapshotTagTime(tag)
		if err != nil {
			t.Errorf("SnapshotTagTime(%s) error %s", tag, err)
		}
		if v != test {
			t.Errorf("SnapshotTagTime(%s) = %s, expected %s", tag, v, test)
		}
	}
}
