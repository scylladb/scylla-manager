// Copyright (C) 2017 ScyllaDB

package repair

import (
	"testing"

	"github.com/scylladb/mermaid/uuid"
)

func TestUnit(t *testing.T) {
	t.Run("GetIDNilTables", func(t *testing.T) {
		t.Parallel()
		u := Unit{Keyspace: "a", Tables: nil}
		if u.genID() == uuid.Nil {
			t.Fatal("empty uuid")
		}
	})
	t.Run("GetIDDuplicateTables", func(t *testing.T) {
		t.Parallel()
		u0 := Unit{Keyspace: "a", Tables: []string{"a"}}
		u1 := Unit{Keyspace: "a", Tables: []string{"a", "a"}}

		if u0.genID() != u1.genID() {
			t.Fatal("id mismatch")
		}
	})
}

func TestRunProgressPercentComplete(t *testing.T) {
	table := []struct {
		P RunProgress
		E int
	}{
		{
			P: RunProgress{},
			E: 0,
		},
		{
			P: RunProgress{SegmentSuccess: 100, SegmentCount: 200},
			E: 50,
		},
		{
			P: RunProgress{SegmentSuccess: 100, SegmentCount: 200, SegmentError: 100},
			E: 50,
		},
		{
			P: RunProgress{SegmentSuccess: 1, SegmentCount: 100},
			E: 1,
		},
	}

	for i, test := range table {
		if test.P.PercentComplete() != test.E {
			t.Error(i, "expected", test.E, "got", test.P.PercentComplete())
		}
	}
}
