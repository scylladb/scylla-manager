// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestClusterMetrics(t *testing.T) {
	m := NewClusterMetrics()

	t.Run("SetName", func(t *testing.T) {
		c0 := uuid.MustParse("b703df56-c428-46a7-bfba-cfa6ee91b976")
		m.SetName(c0, "a")
		m.SetName(c0, "b")
		c1 := uuid.MustParse("0d61b791-cccc-42fb-a64f-b9c636bbd21c")
		m.SetName(c1, "c")
		m.SetName(c1, "d")

		text := Dump(t, m.name)

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	})
}
