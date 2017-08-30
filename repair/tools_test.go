package repair

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMergeConfig(t *testing.T) {
	v := &Config{
		Enabled:              bptr(true),
		SegmentsPerShard:     iptr(50),
		RetryLimit:           iptr(3),
		RetryBackoffSeconds:  iptr(60),
		ParallelNodeLimit:    iptr(0),
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
					SegmentsPerShard:     v.SegmentsPerShard,
					RetryLimit:           v.RetryLimit,
					RetryBackoffSeconds:  v.RetryBackoffSeconds,
					ParallelNodeLimit:    v.ParallelNodeLimit,
					ParallelShardPercent: v.ParallelShardPercent,
				},
				EnabledSource:              ConfigSource{ExternalID: "1"},
				SegmentsPerShardSource:     ConfigSource{ExternalID: "0"},
				RetryLimitSource:           ConfigSource{ExternalID: "0"},
				RetryBackoffSecondsSource:  ConfigSource{ExternalID: "0"},
				ParallelNodeLimitSource:    ConfigSource{ExternalID: "0"},
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
				SegmentsPerShardSource:     ConfigSource{ExternalID: "2"},
				RetryLimitSource:           ConfigSource{ExternalID: "2"},
				RetryBackoffSecondsSource:  ConfigSource{ExternalID: "2"},
				ParallelNodeLimitSource:    ConfigSource{ExternalID: "2"},
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
					SegmentsPerShard:    iptr(50),
					RetryLimit:          iptr(3),
					RetryBackoffSeconds: iptr(60),
				},
				{
					RetryBackoffSeconds:  iptr(60),
					ParallelNodeLimit:    iptr(0),
					ParallelShardPercent: fptr(1),
				},
			},
			S: []ConfigSource{{ExternalID: "0"}, {ExternalID: "1"}, {ExternalID: "2"}},
			E: &ConfigInfo{
				Config:                     *v,
				EnabledSource:              ConfigSource{ExternalID: "0"},
				SegmentsPerShardSource:     ConfigSource{ExternalID: "1"},
				RetryLimitSource:           ConfigSource{ExternalID: "1"},
				RetryBackoffSecondsSource:  ConfigSource{ExternalID: "1"},
				ParallelNodeLimitSource:    ConfigSource{ExternalID: "2"},
				ParallelShardPercentSource: ConfigSource{ExternalID: "2"},
			},
		},
	}

	for i, test := range table {
		c, _ := mergeConfigs(test.C, test.S)
		if diff := cmp.Diff(c, test.E); diff != "" {
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

func fptr(f float32) *float32 {
	return &f
}

func TestTopologyHash(t *testing.T) {
	t.Parallel()

	v, err := topologyHash([]int64{1})
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "17cb299f-0000-4000-9599-a4a200000000" {
		t.Fatal(v)
	}
}
