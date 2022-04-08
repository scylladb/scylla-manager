// Copyright (C) 2017 ScyllaDB

package repair

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
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
			Replicas:   []string{"b", "a", "f", "e"},
		},
		{
			StartToken: 3,
			EndToken:   4,
			Replicas:   []string{"f", "c", "a", "d"},
		},
		{
			StartToken: 5,
			EndToken:   6,
			Replicas:   []string{"c", "b", "d", "e"},
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
					Replicas: []string{"b", "a", "f", "e"},
				},
				{
					Keyspace: "kn", Table: "tn", Pos: 1, StartToken: 3, EndToken: 4,
					Replicas: []string{"f", "c", "a", "d"},
				},
				{
					Keyspace: "kn", Table: "tn", Pos: 2, StartToken: 5, EndToken: 6,
					Replicas: []string{"c", "b", "d", "e"},
				},
			},
		},
		{
			Name: "Host=a",
			Target: Target{
				DC:   []string{"dc1", "dc2"},
				Host: "a",
			},
			Golden: []*tableTokenRange{
				{
					Keyspace: "kn", Table: "tn", Pos: 0, StartToken: 1, EndToken: 2,
					Replicas: []string{"b", "a", "f", "e"},
				},
				{
					Keyspace: "kn", Table: "tn", Pos: 1, StartToken: 3, EndToken: 4,
					Replicas: []string{"f", "c", "a", "d"},
				},
			},
		},
		{
			Name: "IgnoreHosts=a",
			Target: Target{
				DC:          []string{"dc1", "dc2"},
				IgnoreHosts: []string{"a"},
			},
			Golden: []*tableTokenRange{
				{
					Keyspace: "kn", Table: "tn", Pos: 0, StartToken: 1, EndToken: 2,
					Replicas: []string{"b", "f", "e"},
				},
				{
					Keyspace: "kn", Table: "tn", Pos: 1, StartToken: 3, EndToken: 4,
					Replicas: []string{"f", "c", "d"},
				},
				{
					Keyspace: "kn", Table: "tn", Pos: 2, StartToken: 5, EndToken: 6,
					Replicas: []string{"c", "b", "d", "e"},
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
					Replicas: []string{"b", "a"},
				},
				{
					Keyspace: "kn", Table: "tn", Pos: 1, StartToken: 3, EndToken: 4,
					Replicas: []string{"c", "a"},
				},
				{
					Keyspace: "kn", Table: "tn", Pos: 2, StartToken: 5, EndToken: 6,
					Replicas: []string{"c", "b"},
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

			if diff := cmp.Diff(test.Golden, v); diff != "" {
				t.Errorf("Build()=%+#v expected %+#v, diff %s", v, test.Golden, diff)
			}
		})
	}
}

func TestTestTableTokenRangeBuilderMaxParallelRepairs(t *testing.T) {
	table := []struct {
		Name      string
		InputFile string
		Count     int
	}{
		{
			Name:      "RF=2",
			InputFile: "testdata/worker_count/simple_rf2.json",
			Count:     3,
		},
		{
			Name:      "RF=3",
			InputFile: "testdata/worker_count/simple_rf3.json",
			Count:     2,
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			var content []struct {
				Endpoints []string `json:"endpoints"`
			}
			data, err := os.ReadFile(test.InputFile)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &content); err != nil {
				t.Fatal(err)
			}
			var ranges []scyllaclient.TokenRange
			for i := range content {
				ranges = append(ranges, scyllaclient.TokenRange{Replicas: content[i].Endpoints})
			}

			target := Target{DC: []string{"dc1"}}
			hostDC := map[string]string{
				"192.168.100.11": "dc1",
				"192.168.100.12": "dc1",
				"192.168.100.13": "dc1",
				"192.168.100.21": "dc1",
				"192.168.100.22": "dc1",
				"192.168.100.23": "dc1",
			}
			b := newTableTokenRangeBuilder(target, hostDC).Add(ranges)

			if v := b.MaxParallelRepairs(); v != test.Count {
				t.Errorf("maxParallelRepairs()=%d expected %d", v, test.Count)
			}
		})
	}
}
