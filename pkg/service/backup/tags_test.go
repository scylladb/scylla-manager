// Copyright (C) 2017 ScyllaDB

package backup

import (
	"testing"
	"time"
)

func TestSnapshotTag(t *testing.T) {
	t.Parallel()

	tag := newSnapshotTag()
	t.Log(tag)
	if !isSnapshotTag(tag) {
		t.Fatalf("isSnapshotTag(%s) = false, expected true", tag)
	}
}

func TestSnapshotTagChanges(t *testing.T) {
	t.Parallel()

	t0 := newSnapshotTag()
	time.Sleep(time.Second)
	t1 := newSnapshotTag()
	if t0 == t1 {
		t.Fatalf("newSnapshotTag() = %s; newSnapshotTag() = %s, expected to be different", t0, t1)
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
		tag := snapshotTagAt(test)
		v, err := snapshotTagTime(tag)
		if err != nil {
			t.Errorf("snapshotTagTime(%s) error %s", tag, err)
		}
		if v != test {
			t.Errorf("snapshotTagTime(%s) = %s, expected %s", tag, v, test)
		}
	}
}
