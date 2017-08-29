package repair

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/cespare/xxhash"
	"github.com/scylladb/mermaid"
)

// mergeConfigs does the configuration merging for Service.GetMergedUnitConfig.
func mergeConfigs(all []*Config, src []ConfigSource) (*ConfigInfo, error) {
	if len(all) == 0 {
		return nil, errors.New("no matching configurations")
	}

	m := ConfigInfo{}

	// Enabled *bool
	for i, c := range all {
		if c.Enabled != nil {
			if m.Enabled == nil || !*c.Enabled {
				m.Enabled = c.Enabled
				m.EnabledSource = src[i]
			}
		}
	}
	if m.Enabled == nil {
		return nil, errors.New("no value for Enabled")
	}

	// SegmentsPerShard *int
	for i, c := range all {
		if c.SegmentsPerShard != nil {
			m.SegmentsPerShard = c.SegmentsPerShard
			m.SegmentsPerShardSource = src[i]
			break
		}
	}
	if m.SegmentsPerShard == nil {
		return nil, errors.New("no value for SegmentsPerShard")
	}

	// RetryLimit *int
	for i, c := range all {
		if c.RetryLimit != nil {
			m.RetryLimit = c.RetryLimit
			m.RetryLimitSource = src[i]
			break
		}
	}
	if m.RetryLimit == nil {
		return nil, errors.New("no value for RetryLimit")
	}

	// RetryBackoffSeconds *int
	for i, c := range all {
		if c.RetryBackoffSeconds != nil {
			m.RetryBackoffSeconds = c.RetryBackoffSeconds
			m.RetryBackoffSecondsSource = src[i]
			break
		}
	}
	if m.RetryBackoffSeconds == nil {
		return nil, errors.New("no value for RetryBackoffSeconds")
	}

	// ParallelNodeLimit *int
	for i, c := range all {
		if c.ParallelNodeLimit != nil {
			m.ParallelNodeLimit = c.ParallelNodeLimit
			m.ParallelNodeLimitSource = src[i]
			break
		}
	}
	if m.ParallelNodeLimit == nil {
		return nil, errors.New("no value for ParallelNodeLimit")
	}

	// ParallelShardPercent *float32
	for i, c := range all {
		if c.ParallelShardPercent != nil {
			m.ParallelShardPercent = c.ParallelShardPercent
			m.ParallelShardPercentSource = src[i]
			break
		}
	}
	if m.ParallelShardPercent == nil {
		return nil, errors.New("no value for ParallelShardPercent")
	}

	return &m, nil
}

// topologyHash returns hash of all the tokens.
func topologyHash(tokens []int64) (mermaid.UUID, error) {
	var (
		xx = xxhash.New()
		b  = make([]byte, 8)
		u  uint64
	)
	for _, t := range tokens {
		if t >= 0 {
			u = uint64(t)
		} else {
			u = uint64(math.MaxInt64 + t)
		}
		binary.LittleEndian.PutUint64(b, u)
		xx.Write(b)
	}
	h := xx.Sum64()

	return mermaid.UUIDFromUint64(uint64(h>>32), uint64(uint32(h))), nil
}
