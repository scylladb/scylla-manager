// Copyright (C) 2017 ScyllaDB

package repair

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/mermaid/pkg/dht"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
)

func TestValidateHostsBelongToCluster(t *testing.T) {
	t.Parallel()

	table := []struct {
		DC map[string][]string
		H  []string
		E  string
	}{
		{},
		{
			H: []string{"foobar"},
			E: "no such hosts foobar",
		},
		{
			DC: map[string][]string{"dc1": {"172.16.1.3", "172.16.1.2", "172.16.1.10"}, "dc2": {"172.16.1.4", "172.16.1.20", "172.16.1.5"}},
			H:  []string{"172.16.1.3", "172.16.1.5"},
		},
		{
			DC: map[string][]string{"dc1": {"172.16.1.3", "172.16.1.2", "172.16.1.10"}, "dc2": {"172.16.1.4", "172.16.1.20", "172.16.1.5"}},
			H:  []string{"172.16.1.3", "172.16.1.5", "foo", "bar"},
			E:  "no such hosts foo, bar",
		},
	}

	for i, test := range table {
		msg := ""
		if err := validateHostsBelongToCluster(test.DC, test.H...); err != nil {
			msg = err.Error()
		}
		if diff := cmp.Diff(msg, test.E); diff != "" {
			t.Error(i, diff)
		}
	}
}

func TestGroupSegmentsByHost(t *testing.T) {
	t.Parallel()

	hostDC := map[string]string{
		"172.16.1.2":  "dc1",
		"172.16.1.3":  "dc1",
		"172.16.1.10": "dc1",
		"172.16.1.4":  "dc2",
		"172.16.1.5":  "dc2",
		"172.16.1.20": "dc2",
	}

	rf3 := scyllaclient.Ring{
		Tokens: []scyllaclient.TokenRange{
			{
				StartToken: 9165301526494284802,
				EndToken:   9190445181212206709,
				Replicas:   []string{"172.16.1.3", "172.16.1.2", "172.16.1.10", "172.16.1.4", "172.16.1.20", "172.16.1.5"},
			},
			{
				StartToken: 9142565851149460331,
				EndToken:   9143747749498840635,
				Replicas:   []string{"172.16.1.20", "172.16.1.10", "172.16.1.2", "172.16.1.3", "172.16.1.4", "172.16.1.5"},
			},
			// start - end replaced
			{
				StartToken: 9138850273782950336,
				EndToken:   9121190935171762434,
				Replicas:   []string{"172.16.1.10", "172.16.1.2", "172.16.1.3", "172.16.1.20", "172.16.1.4", "172.16.1.5"},
			},
		},
		HostDC: hostDC,
	}

	rf2 := scyllaclient.Ring{
		Tokens: []scyllaclient.TokenRange{
			{
				StartToken: 9165301526494284802,
				EndToken:   9190445181212206709,
				Replicas:   []string{"172.16.1.3", "172.16.1.2", "172.16.1.4", "172.16.1.20"},
			},
			{
				StartToken: 9142565851149460331,
				EndToken:   9143747749498840635,
				Replicas:   []string{"172.16.1.20", "172.16.1.10", "172.16.1.2", "172.16.1.4"},
			},
			// start - end replaced
			{
				StartToken: 9138850273782950336,
				EndToken:   9121190935171762434,
				Replicas:   []string{"172.16.1.10", "172.16.1.3", "172.16.1.20", "172.16.1.5"},
			},
		},
		HostDC: hostDC,
	}

	table := []struct {
		N  string
		R  scyllaclient.Ring
		DC string
		WH []string
		K  TokenRangesKind
		S  map[string]segments
	}{
		{
			N:  "DCPrimaryTokenRanges with RF3",
			R:  rf3,
			DC: "dc1",
			K:  DCPrimaryTokenRanges,
			S: map[string]segments{
				"172.16.1.3": {
					{9165301526494284802, 9190445181212206709},
				},
				"172.16.1.10": {
					{9142565851149460331, 9143747749498840635},
					{dht.Murmur3MinToken, 9121190935171762434},
					{9138850273782950336, dht.Murmur3MaxToken},
				},
			},
		},
		{
			N:  "DCPrimaryTokenRanges with RF2 replicated by 172.16.1.2",
			R:  rf2,
			DC: "dc1",
			WH: []string{"172.16.1.2"},
			K:  DCPrimaryTokenRanges,
			S: map[string]segments{
				"172.16.1.3": {
					{9165301526494284802, 9190445181212206709},
				},
				"172.16.1.10": {
					{9142565851149460331, 9143747749498840635},
				},
			},
		},
		{
			N:  "PrimaryTokenRanges with RF3",
			R:  rf3,
			DC: "dc1",
			K:  PrimaryTokenRanges,
			S: map[string]segments{
				"172.16.1.3": {
					{9165301526494284802, 9190445181212206709},
				},
				"172.16.1.10": {
					{dht.Murmur3MinToken, 9121190935171762434},
					{9138850273782950336, dht.Murmur3MaxToken},
				},
			},
		},
		{
			N:  "PrimaryTokenRanges with RF2 replicated by 172.16.1.2",
			R:  rf2,
			DC: "dc1",
			WH: []string{"172.16.1.2"},
			K:  PrimaryTokenRanges,
			S: map[string]segments{
				"172.16.1.3": {
					{9165301526494284802, 9190445181212206709},
				},
			},
		},
		{
			N:  "NonPrimaryTokenRanges with RF3",
			R:  rf3,
			DC: "dc1",
			K:  NonPrimaryTokenRanges,
			S: map[string]segments{
				"172.16.1.2": {
					{9165301526494284802, 9190445181212206709},
					{9142565851149460331, 9143747749498840635},
					{dht.Murmur3MinToken, 9121190935171762434},
					{9138850273782950336, dht.Murmur3MaxToken},
				},
				"172.16.1.3": {
					{9142565851149460331, 9143747749498840635},
					{dht.Murmur3MinToken, 9121190935171762434},
					{9138850273782950336, dht.Murmur3MaxToken},
				},
				"172.16.1.10": {
					{9165301526494284802, 9190445181212206709},
					{9142565851149460331, 9143747749498840635},
				},
			},
		},
		{
			N:  "NonPrimaryTokenRanges with RF3 replicated by 172.16.1.2",
			R:  rf3,
			DC: "dc1",
			WH: []string{"172.16.1.2"},
			K:  NonPrimaryTokenRanges,
			S: map[string]segments{
				"172.16.1.3": {
					{9142565851149460331, 9143747749498840635},
					{dht.Murmur3MinToken, 9121190935171762434},
					{9138850273782950336, dht.Murmur3MaxToken},
				},
				"172.16.1.10": {
					{9165301526494284802, 9190445181212206709},
					{9142565851149460331, 9143747749498840635},
				},
			},
		},
		{
			N:  "AllTokenRanges with RF3",
			R:  rf3,
			DC: "dc1",
			K:  AllTokenRanges,
			S: map[string]segments{
				"172.16.1.2": {
					{9165301526494284802, 9190445181212206709},
					{9142565851149460331, 9143747749498840635},
					{dht.Murmur3MinToken, 9121190935171762434},
					{9138850273782950336, dht.Murmur3MaxToken},
				},
				"172.16.1.3": {
					{9165301526494284802, 9190445181212206709},
					{9142565851149460331, 9143747749498840635},
					{dht.Murmur3MinToken, 9121190935171762434},
					{9138850273782950336, dht.Murmur3MaxToken},
				},
				"172.16.1.10": {
					{9165301526494284802, 9190445181212206709},
					{9142565851149460331, 9143747749498840635},
					{dht.Murmur3MinToken, 9121190935171762434},
					{9138850273782950336, dht.Murmur3MaxToken},
				},
			},
		},
		{
			N:  "AllTokenRanges with RF2 replicated by 172.16.1.2",
			R:  rf2,
			DC: "dc1",
			WH: []string{"172.16.1.2"},
			K:  AllTokenRanges,
			S: map[string]segments{
				"172.16.1.3": {
					{9165301526494284802, 9190445181212206709},
				},
				"172.16.1.10": {
					{9142565851149460331, 9143747749498840635},
				},
			},
		},
		{
			N:  "DCPrimaryTokenRanges with RF2 and host from dc2",
			R:  rf2,
			DC: "dc1",
			WH: []string{"172.16.1.4"},
			K:  DCPrimaryTokenRanges,
			S: map[string]segments{
				"172.16.1.3": {
					{9165301526494284802, 9190445181212206709},
				},
				"172.16.1.10": {
					{9142565851149460331, 9143747749498840635},
				},
			},
		},
		{
			N:  "PrimaryTokenRanges with RF3 from all DCs",
			R:  rf3,
			DC: "",
			K:  PrimaryTokenRanges,
			S: map[string]segments{
				"172.16.1.3": {
					{9165301526494284802, 9190445181212206709},
				},
				"172.16.1.20": {
					{9142565851149460331, 9143747749498840635},
				},
				"172.16.1.10": {
					{dht.Murmur3MinToken, 9121190935171762434},
					{9138850273782950336, dht.Murmur3MaxToken},
				},
			},
		},
	}

	for _, test := range table {
		hostSegments := groupSegmentsByHost(test.DC, "", test.WH, test.K, test.R)

		if diff := cmp.Diff(test.S, hostSegments); diff != "" {
			t.Error(test.N, diff, hostSegments)
		}
	}
}

func TestSplitSegmentsToShards(t *testing.T) {
	t.Parallel()

	for _, shardCount := range []uint{1, 2, 3, 5, 8} {
		p := dht.NewMurmur3Partitioner(shardCount, 12)
		s := segments{
			{
				StartToken: dht.Murmur3MinToken,
				EndToken:   dht.Murmur3MinToken + 1<<50,
			},
			{
				StartToken: 9165301526494284802,
				EndToken:   9190445181212206709,
			},
			{
				StartToken: 9142565851149460331,
				EndToken:   9143747749498840635,
			},
		}
		v := s.splitToShards(p)

		if err := s.validateShards(v, p); err != nil {
			t.Fatal(err)
		}
	}
}

func TestValidateShardProgress(t *testing.T) {
	t.Parallel()

	table := []struct {
		S   segments
		P   *RunProgress
		Err string
	}{
		{
			S:   segments{{0, 10}, {20, 30}},
			P:   &RunProgress{SegmentCount: 1},
			Err: "shard 0: segment count mismatch got 1 expected 2",
		},
		{
			S:   segments{{0, 10}, {20, 30}},
			P:   &RunProgress{SegmentCount: 2, LastStartToken: -1},
			Err: "shard 0: no segment for start token -1",
		},
		{
			S:   segments{{0, 10}, {20, 30}},
			P:   &RunProgress{SegmentCount: 2, LastStartToken: 20},
			Err: "",
		},
	}

	for _, test := range table {
		msg := ""
		if err := validateShardProgress([]segments{test.S}, []*RunProgress{test.P}); err != nil {
			msg = err.Error()
		}
		if diff := cmp.Diff(msg, test.Err); diff != "" {
			t.Error(diff)
		}
	}
}

func TestTopologyHash(t *testing.T) {
	t.Parallel()

	v := topologyHash([]int64{1})
	if v.String() != "17cb299f-0000-4000-9599-a4a200000000" {
		t.Fatal(v)
	}
}

func TestAggregateProgress(t *testing.T) {
	t.Parallel()

	units := []Unit{
		{Keyspace: "keyspace0"},
		{Keyspace: "keyspace1"},
		{Keyspace: "keyspace2"},
	}

	table := []struct {
		U []Unit
		P []*RunProgress
		E string
	}{
		// empty units list
		{
			E: "test-data/aggregate_progress_empty_units_list.json",
		},
		// empty progress list
		{
			U: units,
			E: "test-data/aggregate_progress_empty_progress_list.json",
		},
		// single unit, single host, single shard
		{
			U: units[0:1],
			P: []*RunProgress{
				{Host: "A", SegmentCount: 100, SegmentSuccess: 30},
			},
			E: "test-data/aggregate_progress_single_unit_host_shard.json",
		},
		// multiple units, single host, single shard
		{
			U: units,
			P: []*RunProgress{
				{Unit: 0, Host: "A", SegmentCount: 100, SegmentSuccess: 30},
				{Unit: 1, Host: "B", SegmentCount: 100, SegmentSuccess: 40},
				{Unit: 2, Host: "C", SegmentCount: 100, SegmentSuccess: 50},
			},
			E: "test-data/aggregate_progress_multiple_units_single_host_shard.json",
		},
	}

	opts := cmp.Options{
		cmp.AllowUnexported(Progress{}, Unit{}, UnitProgress{}, NodeProgress{}, ShardProgress{}),
		cmpopts.IgnoreUnexported(progress{}),
	}

	for i, test := range table {
		f, err := os.Open(test.E)
		if err != nil {
			t.Fatal(err)
		}
		var p Progress
		if err := json.NewDecoder(f).Decode(&p); err != nil {
			t.Fatal(err)
		}
		f.Close()

		r := &Run{
			Units:       test.U,
			TokenRanges: DCPrimaryTokenRanges,
		}

		if diff := cmp.Diff(p, aggregateProgress(r, test.P), opts); diff != "" {
			t.Error(i, diff)
		}
	}
}

func TestAggregateUnitProgress(t *testing.T) {
	t.Parallel()

	u := Unit{
		Keyspace: "keyspace",
	}

	table := []struct {
		P []*RunProgress
		E string
	}{
		// empty progress list
		{
			E: "test-data/aggregate_unit_progress_empty_progress_list.json",
		},
		// multiple hosts, single shard
		{
			P: []*RunProgress{
				{Host: "A", SegmentCount: 100, SegmentSuccess: 30},
				{Host: "B", SegmentCount: 100, SegmentSuccess: 50},
			},
			E: "test-data/aggregate_unit_progress_multiple_hosts_single_shard.json",
		},
		// multiple hosts, multiple shards
		{
			P: []*RunProgress{
				{Host: "A", SegmentCount: 100, SegmentSuccess: 30},
				{Host: "A", SegmentCount: 100, SegmentSuccess: 50},
				{Host: "B", SegmentCount: 100, SegmentSuccess: 60},
			},
			E: "test-data/aggregate_unit_progress_multiple_hosts_shards.json",
		},
	}

	opts := cmp.Options{
		cmp.AllowUnexported(Unit{}, UnitProgress{}, NodeProgress{}, ShardProgress{}),
		cmpopts.IgnoreUnexported(progress{}),
	}

	for i, test := range table {
		f, err := os.Open(test.E)
		if err != nil {
			t.Fatal(err)
		}
		var p UnitProgress
		if err := json.NewDecoder(f).Decode(&p); err != nil {
			t.Fatal(err)
		}
		f.Close()

		if diff := cmp.Diff(p, aggregateUnitProgress(u, test.P), opts); diff != "" {
			t.Error(i, diff)
		}
	}
}
