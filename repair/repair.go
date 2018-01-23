// Copyright (C) 2017 ScyllaDB

package repair

import (
	"encoding/binary"
	"math"

	"github.com/cespare/xxhash"
	"github.com/fatih/set"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/dht"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
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

	// SegmentSizeLimit *int64
	for i, c := range all {
		if c.SegmentSizeLimit != nil {
			m.SegmentSizeLimit = c.SegmentSizeLimit
			m.SegmentSizeLimitSource = src[i]
			break
		}
	}
	if m.SegmentSizeLimit == nil {
		return nil, errors.New("no value for SegmentSizeLimit")
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

// groupSegmentsByHost extract list of primary segments (token ranges) for every
// host in a datacenter and returns a mapping from host to list of it's segments.
func groupSegmentsByHost(dc string, ring []*scyllaclient.TokenRange) (map[string][]*Segment, error) {
	m := make(map[string][]*Segment)

	for _, r := range ring {
		if len(r.Hosts[dc]) == 0 {
			return nil, errors.Errorf("token range %d:%d not present in dc %s", r.StartToken, r.EndToken, dc)
		}

		host := r.Hosts[dc][0]
		if r.StartToken > r.EndToken {
			m[host] = append(m[host],
				&Segment{StartToken: dht.Murmur3MinToken, EndToken: r.EndToken},
				&Segment{StartToken: r.StartToken, EndToken: dht.Murmur3MaxToken},
			)
		} else {
			m[host] = append(m[host], &Segment{StartToken: r.StartToken, EndToken: r.EndToken})
		}
	}

	return m, nil
}

// splitSegmentsToShards splits the segments into shards given the partitioner.
func splitSegmentsToShards(segments []*Segment, p *dht.Murmur3Partitioner) [][]*Segment {
	res := make([][]*Segment, p.ShardCount())

	for _, s := range segments {
		start := s.StartToken
		end := s.EndToken
		shard := p.ShardOf(end - 1)

		for start < end {
			prev := p.PrevShard(shard)
			token := p.TokenForPrevShard(end, shard)

			if token > start {
				res[shard] = append(res[shard], &Segment{StartToken: token, EndToken: end})
			} else {
				res[shard] = append(res[shard], &Segment{StartToken: start, EndToken: end})
			}

			end = token
			shard = prev
		}
	}

	return res
}

// validateShards checks that the shard split of segments is sound.
func validateShards(segments []*Segment, shards [][]*Segment, p *dht.Murmur3Partitioner) error {
	startTokens := set.NewNonTS()
	endTokens := set.NewNonTS()

	// check that the segments belong to the correct shards
	for shard, s := range shards {
		for _, r := range s {
			if p.ShardOf(r.StartToken) != uint(shard) {
				return errors.Errorf("wrong shard of a start token %d, expected %d, got %d", r.StartToken, p.ShardOf(r.StartToken), shard)
			}
			if p.ShardOf(r.EndToken-1) != uint(shard) {
				return errors.Errorf("wrong shard of an end token %d, expected %d, got %d", r.EndToken-1, p.ShardOf(r.EndToken-1), shard)
			}

			// extract tokens
			startTokens.Add(r.StartToken)
			endTokens.Add(r.EndToken)
		}
	}

	// check that shards contain the original start and end tokens
	for _, r := range segments {
		if !startTokens.Has(r.StartToken) {
			return errors.Errorf("no start token %d", r.StartToken)
		}
		if !endTokens.Has(r.EndToken) {
			return errors.Errorf("no end token %d", r.StartToken)
		}

		startTokens.Remove(r.StartToken)
		endTokens.Remove(r.EndToken)
	}

	// check that the range is continuous
	var err error

	startTokens.Each(func(item interface{}) bool {
		if !endTokens.Has(item) {
			err = errors.Errorf("missing end token for start token %d", item)
			return false
		}
		return true
	})
	if err != nil {
		return err
	}

	endTokens.Each(func(item interface{}) bool {
		if !startTokens.Has(item) {
			err = errors.Errorf("missing start token end token %d", item)
			return false
		}
		return true
	})

	return err
}

// validateShardProgress checks if run progress, possibly copied from a
// different run matches the shards.
func validateShardProgress(shards [][]*Segment, prog []*RunProgress) error {
	if len(prog) != len(shards) {
		return errors.New("length mismatch")
	}

	for i, p := range prog {
		if p.Shard != i {
			return errors.Errorf("shard %d: progress for shard %d", i, p.Shard)
		}
		if p.SegmentCount != len(shards[i]) {
			return errors.Errorf("shard %d: segment count mismatch got %d expected %d", p.Shard, p.SegmentCount, len(shards[i]))
		}
		if p.LastStartToken != 0 {
			if _, ok := segmentsContainStartToken(shards[i], p.LastStartToken); !ok {
				return errors.Errorf("shard %d: no segment for start token %d", p.Shard, p.LastStartToken)
			}
		}
		for _, token := range p.SegmentErrorStartTokens {
			if _, ok := segmentsContainStartToken(shards[i], token); !ok {
				return errors.Errorf("shard %d: no segment for (failed) start token %d", p.Shard, token)
			}
		}
	}

	return nil
}

// validateTables checks if tables are a subset of all the tables. Empty table
// list is always valid.
func validateTables(tables []string, all []string) error {
	if len(tables) == 0 {
		return nil
	}

	s := set.NewNonTS()
	for _, t := range tables {
		s.Add(t)
	}
	for _, t := range all {
		s.Remove(t)
	}
	if !s.IsEmpty() {
		return errors.Errorf("unknown tables %s", s)
	}

	return nil
}

// topologyHash returns hash of all the tokens.
func topologyHash(tokens []int64) uuid.UUID {
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

	return uuid.NewFromUint64(uint64(h>>32), uint64(uint32(h)))
}
