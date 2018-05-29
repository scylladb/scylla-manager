// Copyright (C) 2017 ScyllaDB

package repair

import (
	"encoding/binary"
	"math"

	"github.com/cespare/xxhash"
	"github.com/fatih/set"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/internal/dht"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
)

// groupSegmentsByHost extract list of primary segments (token ranges) for every
// host in a datacenter and returns a mapping from host to list of it's segments.
func groupSegmentsByHost(dc string, ring []*scyllaclient.TokenRange) (map[string]segments, error) {
	m := make(map[string]segments)

	for _, r := range ring {
		if len(r.Hosts[dc]) == 0 {
			return nil, errors.Errorf("token range %d:%d not present in dc %s", r.StartToken, r.EndToken, dc)
		}

		host := r.Hosts[dc][0]
		if r.StartToken > r.EndToken {
			m[host] = append(m[host],
				&segment{StartToken: dht.Murmur3MinToken, EndToken: r.EndToken},
				&segment{StartToken: r.StartToken, EndToken: dht.Murmur3MaxToken},
			)
		} else {
			m[host] = append(m[host], &segment{StartToken: r.StartToken, EndToken: r.EndToken})
		}
	}

	return m, nil
}

// validateShardProgress checks if run progress, possibly copied from a
// different run matches the shards.
func validateShardProgress(shards []segments, prog []*RunProgress) error {
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
			if _, ok := shards[i].containStartToken(p.LastStartToken); !ok {
				return errors.Errorf("shard %d: no segment for start token %d", p.Shard, p.LastStartToken)
			}
		}
		for _, token := range p.SegmentErrorStartTokens {
			if _, ok := shards[i].containStartToken(token); !ok {
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

func hostsPercentComplete(prog []*RunProgress) map[string]float64 {
	if len(prog) == 0 {
		return nil
	}

	v := make(map[string]float64)

	total := 0.
	hosts := 0.

	ht := 0.
	hs := 0.
	lh := prog[0].Host
	for _, p := range prog {
		if lh != p.Host {
			v[lh] = ht / hs
			total += ht / hs
			hosts++

			ht = 0
			hs = 0
			lh = p.Host
		}

		ht += float64(p.PercentComplete())
		hs++
	}

	v[lh] = ht / hs
	total += ht / hs
	hosts++

	v[""] = total / hosts

	return v
}
