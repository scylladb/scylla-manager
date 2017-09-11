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
