// Copyright (C) 2017 ScyllaDB

package repair

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
)

func TestTableTokenRangeReplicaHash(t *testing.T) {
	ttr0 := tableTokenRange{
		Replicas: []string{"a", "b", "c"},
	}
	ttr1 := tableTokenRange{
		Replicas: []string{"c", "b", "a"},
	}
	ttr2 := tableTokenRange{
		Replicas: []string{"c", "b"},
	}

	if ttr0.ReplicaHash() != ttr1.ReplicaHash() {
		t.Error("Hash mismatch")
	}

	if ttr0.ReplicaHash() == ttr2.ReplicaHash() {
		t.Error("Unexpeced hash match")
	}
}

func TestDumpRanges(t *testing.T) {
	ranges := []*tableTokenRange{
		{
			StartToken: 1,
			EndToken:   2,
		},
		{
			StartToken: 3,
			EndToken:   4,
		},
		{
			StartToken: 500,
			EndToken:   4,
		},
	}

	if s := dumpRanges(ranges); s == "" {
		t.Fatalf("dumpRanges()=%s expected x", s)
	}
}

func TestTableTokenRangeBuilder(t *testing.T) {
	hostDC := map[string]string{
		"a": "dc1",
		"b": "dc1",
		"c": "dc1",
		"d": "dc2",
		"e": "dc2",
		"f": "dc2",
	}

	ranges := []scyllaclient.TokenRange{
		{
			StartToken: 1,
			EndToken:   2,
			Replicas:   []string{"b", "a", "c", "d", "f", "e"},
		},
		{
			StartToken: 3,
			EndToken:   4,
			Replicas:   []string{"f", "c", "a", "b", "d", "e"},
		},
		{
			StartToken: 5,
			EndToken:   6,
			Replicas:   []string{"c", "a", "b", "f", "d", "e"},
		},
	}

	table := []struct {
		Name   string
		Target Target
		Golden []*tableTokenRange
	}{
		{
			Name: "Basic",
			Target: Target{
				DC: []string{"dc1", "dc2"},
			},
			Golden: []*tableTokenRange{
				{
					Keyspace: "kn", Table: "tn", Pos: 0, StartToken: 1, EndToken: 2,
					Replicas: []string{"b", "a", "c", "d", "f", "e"},
				},
				{
					Keyspace: "kn", Table: "tn", Pos: 1, StartToken: 3, EndToken: 4,
					Replicas: []string{"f", "c", "a", "b", "d", "e"},
				},
				{
					Keyspace: "kn", Table: "tn", Pos: 2, StartToken: 5, EndToken: 6,
					Replicas: []string{"c", "a", "b", "f", "d", "e"},
				},
			},
		},
		{
			Name: "DC=dc1",
			Target: Target{
				DC: []string{"dc1"},
			},
			Golden: []*tableTokenRange{
				{
					Keyspace: "kn", Table: "tn", Pos: 0, StartToken: 1, EndToken: 2,
					Replicas: []string{"b", "a", "c"},
				},
				{
					Keyspace: "kn", Table: "tn", Pos: 1, StartToken: 3, EndToken: 4,
					Replicas: []string{"c", "a", "b"},
				},
				{
					Keyspace: "kn", Table: "tn", Pos: 2, StartToken: 5, EndToken: 6,
					Replicas: []string{"c", "a", "b"},
				},
			},
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			b := newTableTokenRangeBuilder(test.Target, hostDC)
			b.Add(ranges)
			v := b.Build(Unit{Keyspace: "kn", Tables: []string{"tn"}})

			if diff := cmp.Diff(v, test.Golden); diff != "" {
				t.Errorf("Build()=%+#v expected %+#v, diff %s", v, test.Golden, diff)
			}
		})
	}
}
