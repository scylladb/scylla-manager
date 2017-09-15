// Copyright (C) 2017 ScyllaDB

package repair

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/dht"
	"github.com/scylladb/mermaid/scylla"
	"github.com/scylladb/mermaid/uuid"
)

func TestMergeConfig(t *testing.T) {
	v := &Config{
		Enabled:              bptr(true),
		SegmentSizeLimit:     i64ptr(50),
		RetryLimit:           iptr(3),
		RetryBackoffSeconds:  iptr(60),
		ParallelShardPercent: fptr(1),
	}

	table := []struct {
		C []*Config
		S []ConfigSource
		E *ConfigInfo
	}{
		// empty
		{
			C: nil,
			S: nil,
			E: nil,
		},
		// missing fields
		{
			C: []*Config{},
			S: nil,
			E: nil,
		},
		// disable
		{
			C: []*Config{v, {Enabled: bptr(false)}},
			S: []ConfigSource{{ExternalID: "0"}, {ExternalID: "1"}},
			E: &ConfigInfo{
				Config: Config{
					Enabled:              bptr(false),
					SegmentSizeLimit:     v.SegmentSizeLimit,
					RetryLimit:           v.RetryLimit,
					RetryBackoffSeconds:  v.RetryBackoffSeconds,
					ParallelShardPercent: v.ParallelShardPercent,
				},
				EnabledSource:              ConfigSource{ExternalID: "1"},
				SegmentSizeLimitSource:     ConfigSource{ExternalID: "0"},
				RetryLimitSource:           ConfigSource{ExternalID: "0"},
				RetryBackoffSecondsSource:  ConfigSource{ExternalID: "0"},
				ParallelShardPercentSource: ConfigSource{ExternalID: "0"},
			},
		},
		// fallthrough
		{
			C: []*Config{{}, {}, v},
			S: []ConfigSource{{ExternalID: "0"}, {ExternalID: "1"}, {ExternalID: "2"}},
			E: &ConfigInfo{
				Config:                     *v,
				EnabledSource:              ConfigSource{ExternalID: "2"},
				SegmentSizeLimitSource:     ConfigSource{ExternalID: "2"},
				RetryLimitSource:           ConfigSource{ExternalID: "2"},
				RetryBackoffSecondsSource:  ConfigSource{ExternalID: "2"},
				ParallelShardPercentSource: ConfigSource{ExternalID: "2"},
			},
		},
		// merge
		{
			C: []*Config{
				{
					Enabled: bptr(true),
				},
				{
					SegmentSizeLimit:    i64ptr(50),
					RetryLimit:          iptr(3),
					RetryBackoffSeconds: iptr(60),
				},
				{
					RetryBackoffSeconds:  iptr(60),
					ParallelShardPercent: fptr(1),
				},
			},
			S: []ConfigSource{{ExternalID: "0"}, {ExternalID: "1"}, {ExternalID: "2"}},
			E: &ConfigInfo{
				Config:                     *v,
				EnabledSource:              ConfigSource{ExternalID: "0"},
				SegmentSizeLimitSource:     ConfigSource{ExternalID: "1"},
				RetryLimitSource:           ConfigSource{ExternalID: "1"},
				RetryBackoffSecondsSource:  ConfigSource{ExternalID: "1"},
				ParallelShardPercentSource: ConfigSource{ExternalID: "2"},
			},
		},
	}

	for i, test := range table {
		c, _ := mergeConfigs(test.C, test.S)
		if diff := cmp.Diff(c, test.E, cmp.AllowUnexported(uuid.UUID{})); diff != "" {
			t.Error(i, diff)
		}
	}
}

func bptr(b bool) *bool {
	return &b
}

func iptr(i int) *int {
	return &i
}

func i64ptr(i int64) *int64 {
	return &i
}

func fptr(f float32) *float32 {
	return &f
}

func TestGroupSegmentsByHost(t *testing.T) {
	t.Parallel()

	trs := []*scylla.TokenRange{
		{
			StartToken: 9165301526494284802,
			EndToken:   9190445181212206709,
			Hosts:      map[string][]string{"dc1": {"172.16.1.3", "172.16.1.2", "172.16.1.10"}, "dc2": {"172.16.1.4", "172.16.1.20", "172.16.1.5"}},
		},
		{
			StartToken: 9142565851149460331,
			EndToken:   9143747749498840635,
			Hosts:      map[string][]string{"dc1": {"172.16.1.10", "172.16.1.2", "172.16.1.3"}, "dc2": {"172.16.1.20", "172.16.1.4", "172.16.1.5"}},
		},
		// start - end replaced
		{
			StartToken: 9138850273782950336,
			EndToken:   9121190935171762434,
			Hosts:      map[string][]string{"dc1": {"172.16.1.10", "172.16.1.2", "172.16.1.3"}, "dc2": {"172.16.1.20", "172.16.1.4", "172.16.1.5"}},
		},
	}

	dc1 := map[string][]*Segment{
		"172.16.1.3": {
			{
				StartToken: 9165301526494284802,
				EndToken:   9190445181212206709,
			},
		},
		"172.16.1.10": {
			{
				StartToken: 9142565851149460331,
				EndToken:   9143747749498840635,
			},
			{
				StartToken: dht.Murmur3MinToken,
				EndToken:   9121190935171762434,
			},
			{
				StartToken: 9138850273782950336,
				EndToken:   dht.Murmur3MaxToken,
			},
		},
	}

	if diff := cmp.Diff(dc1, groupSegmentsByHost("dc1", trs)); diff != "" {
		t.Fatal(diff)
	}
}

func TestSplitSegmentsToShards(t *testing.T) {
	t.Parallel()

	for _, shardCount := range []uint{1, 2, 3, 5, 8} {
		p := dht.NewMurmur3Partitioner(shardCount, 12)
		s := []*Segment{
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
		v := splitSegmentsToShards(s, p)

		if err := validateShards(s, v, p); err != nil {
			t.Fatal(err)
		}
	}
}

func TestValidateTables(t *testing.T) {
	t.Parallel()

	table := []struct {
		T   []string
		A   []string
		Err string
	}{
		{},
		{
			A: []string{"A", "B", "C", "D"},
		},
		{
			T: []string{"A", "B"},
			A: []string{"A", "B", "C", "D"},
		},
		{
			T:   []string{"A", "B"},
			A:   []string{"A", "_", "C", "D"},
			Err: "unknown tables [B]",
		},
	}

	for _, test := range table {
		msg := ""
		if err := validateTables(test.T, test.A); err != nil {
			msg = err.Error()
		}
		if diff := cmp.Diff(msg, test.Err); diff != "" {
			t.Fatal(diff)
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
