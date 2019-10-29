// Copyright (C) 2017 ScyllaDB

package repair

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/pkg/errors"
	"github.com/scylladb/go-set/i64set"
	"github.com/scylladb/mermaid/internal/dht"
)

// segment specifies token range: [StartToken, EndToken), StartToken is always
// less then EndToken.
type segment struct {
	StartToken int64
	EndToken   int64
}

// segments is a grouping type for []*segment to allow for easier and more
// type safe operations on a []*segment
type segments []*segment

// dump writes segments as a coma separated list of pairs.
func (s segments) dump() string {
	buf := bytes.Buffer{}

	first := true
	for _, s := range s {
		if first {
			first = false
		} else {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf("%d", s.StartToken))
		buf.WriteByte(':')
		buf.WriteString(fmt.Sprintf("%d", s.EndToken))
	}

	return buf.String()
}

// merge joins adjunct segments.
func (s segments) merge() segments {
	// handle empty segments
	if len(s) == 0 {
		return s
	}

	// Sort
	sort.Slice(s, func(i, j int) bool {
		return s[i].StartToken < s[j].StartToken
	})

	var res segments

	cur := s[0]
	for i := 1; i < len(s); i++ {
		// overlap
		if cur.EndToken >= s[i].StartToken {
			if cur.EndToken < s[i].EndToken {
				cur.EndToken = s[i].EndToken
			}
			// don't overlap
		} else {
			res = append(res, cur)
			cur = s[i]
		}
	}
	res = append(res, cur)

	return res
}

// containStartToken checks if there exists a segment starting with a
// given token.
func (s segments) containStartToken(token int64) (int, bool) {
	i := sort.Search(len(s), func(i int) bool {
		return s[i].StartToken >= token
	})
	if i < len(s) && s[i].StartToken == token {
		return i, true
	}

	return i, false
}

// splitToShards splits the segments into shards given the partitioner.
func (s segments) splitToShards(p *dht.Murmur3Partitioner) []segments {
	res := make([]segments, p.ShardCount())

	for _, seg := range s {
		start := seg.StartToken
		end := seg.EndToken
		shard := p.ShardOf(end - 1)

		for start < end {
			prev := p.PrevShard(shard)
			token := p.TokenForPrevShard(end, shard)

			if token > start {
				res[shard] = append(res[shard], &segment{StartToken: token, EndToken: end})
			} else {
				res[shard] = append(res[shard], &segment{StartToken: start, EndToken: end})
			}

			end = token
			shard = prev
		}
	}

	return res
}

// validateShards checks that the shard split of segments is sound.
func (s segments) validateShards(shards []segments, p *dht.Murmur3Partitioner) error {
	startTokens := i64set.New()
	endTokens := i64set.New()

	// Check that the s belong to the correct shards
	for shard, s := range shards {
		for _, r := range s {
			if p.ShardOf(r.StartToken) != uint(shard) {
				return errors.Errorf("wrong shard of a start token %d, expected %d, got %d", r.StartToken, p.ShardOf(r.StartToken), shard)
			}
			if p.ShardOf(r.EndToken-1) != uint(shard) {
				return errors.Errorf("wrong shard of an end token %d, expected %d, got %d", r.EndToken-1, p.ShardOf(r.EndToken-1), shard)
			}

			// Extract tokens
			startTokens.Add(r.StartToken)
			endTokens.Add(r.EndToken)
		}
	}

	// Check that shards contain the original start and end tokens
	for _, r := range s {
		if !startTokens.Has(r.StartToken) {
			return errors.Errorf("no start token %d", r.StartToken)
		}
		if !endTokens.Has(r.EndToken) {
			return errors.Errorf("no end token %d", r.StartToken)
		}

		startTokens.Remove(r.StartToken)
		endTokens.Remove(r.EndToken)
	}

	// Check that the range is continuous
	var err error

	startTokens.Each(func(item int64) bool {
		if !endTokens.Has(item) {
			err = errors.Errorf("missing end token for start token %d", item)
			return false
		}
		return true
	})
	if err != nil {
		return err
	}

	endTokens.Each(func(item int64) bool {
		if !startTokens.Has(item) {
			err = errors.Errorf("missing start token end token %d", item)
			return false
		}
		return true
	})

	return err
}
