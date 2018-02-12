// Copyright (C) 2017 ScyllaDB

package repair

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/mermaidtest"
)

func TestMergeConfig(t *testing.T) {
	v := &LegacyConfig{
		Enabled:              bptr(true),
		SegmentSizeLimit:     i64ptr(50),
		RetryLimit:           iptr(3),
		RetryBackoffSeconds:  iptr(60),
		ParallelShardPercent: fptr(1),
	}

	table := []struct {
		C []*LegacyConfig
		S []LegacyConfigSource
		E *LegacyConfigInfo
	}{
		// empty
		{
			C: nil,
			S: nil,
			E: nil,
		},
		// missing fields
		{
			C: []*LegacyConfig{},
			S: nil,
			E: nil,
		},
		// disable
		{
			C: []*LegacyConfig{v, {Enabled: bptr(false)}},
			S: []LegacyConfigSource{{ExternalID: "0"}, {ExternalID: "1"}},
			E: &LegacyConfigInfo{
				LegacyConfig: LegacyConfig{
					Enabled:              bptr(false),
					SegmentSizeLimit:     v.SegmentSizeLimit,
					RetryLimit:           v.RetryLimit,
					RetryBackoffSeconds:  v.RetryBackoffSeconds,
					ParallelShardPercent: v.ParallelShardPercent,
				},
				EnabledSource:              LegacyConfigSource{ExternalID: "1"},
				SegmentSizeLimitSource:     LegacyConfigSource{ExternalID: "0"},
				RetryLimitSource:           LegacyConfigSource{ExternalID: "0"},
				RetryBackoffSecondsSource:  LegacyConfigSource{ExternalID: "0"},
				ParallelShardPercentSource: LegacyConfigSource{ExternalID: "0"},
			},
		},
		// fallthrough
		{
			C: []*LegacyConfig{{}, {}, v},
			S: []LegacyConfigSource{{ExternalID: "0"}, {ExternalID: "1"}, {ExternalID: "2"}},
			E: &LegacyConfigInfo{
				LegacyConfig:               *v,
				EnabledSource:              LegacyConfigSource{ExternalID: "2"},
				SegmentSizeLimitSource:     LegacyConfigSource{ExternalID: "2"},
				RetryLimitSource:           LegacyConfigSource{ExternalID: "2"},
				RetryBackoffSecondsSource:  LegacyConfigSource{ExternalID: "2"},
				ParallelShardPercentSource: LegacyConfigSource{ExternalID: "2"},
			},
		},
		// merge
		{
			C: []*LegacyConfig{
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
			S: []LegacyConfigSource{{ExternalID: "0"}, {ExternalID: "1"}, {ExternalID: "2"}},
			E: &LegacyConfigInfo{
				LegacyConfig:               *v,
				EnabledSource:              LegacyConfigSource{ExternalID: "0"},
				SegmentSizeLimitSource:     LegacyConfigSource{ExternalID: "1"},
				RetryLimitSource:           LegacyConfigSource{ExternalID: "1"},
				RetryBackoffSecondsSource:  LegacyConfigSource{ExternalID: "1"},
				ParallelShardPercentSource: LegacyConfigSource{ExternalID: "2"},
			},
		},
	}

	for i, test := range table {
		c, _ := mergeConfigs(test.C, test.S)
		if diff := cmp.Diff(c, test.E, mermaidtest.UUIDComparer()); diff != "" {
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
