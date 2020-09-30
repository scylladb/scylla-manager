// Copyright (C) 2017 ScyllaDB

package repair

import (
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/i64set"
	"github.com/scylladb/mermaid/pkg/dht"
)

func splitToShardsAndValidate(ttrs []*tableTokenRange, p *dht.Murmur3Partitioner) ([][]*tableTokenRange, error) {
	ttrs = fixRanges(ttrs)
	shards := splitTokenRangesToShards(ttrs, p)
	return shards, validateTokenRangesSplitToShard(ttrs, shards, p)
}

func splitTokenRangesToShards(ttrs []*tableTokenRange, p *dht.Murmur3Partitioner) [][]*tableTokenRange {
	res := make([][]*tableTokenRange, p.ShardCount())

	for _, ttr := range ttrs {
		start := ttr.StartToken
		end := ttr.EndToken
		shard := p.ShardOf(end - 1)

		for start < end {
			prev := p.PrevShard(shard)
			token := p.TokenForPrevShard(end, shard)

			x := new(tableTokenRange)
			*x = *ttr
			if token > start {
				x.StartToken = token
				x.EndToken = end
			} else {
				x.StartToken = start
				x.EndToken = end
			}
			res[shard] = append(res[shard], x)

			end = token
			shard = prev
		}
	}

	return res
}

func validateTokenRangesSplitToShard(ttrs []*tableTokenRange, shards [][]*tableTokenRange, p *dht.Murmur3Partitioner) error {
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
	for _, ttr := range ttrs {
		if !startTokens.Has(ttr.StartToken) {
			return errors.Errorf("no start token %d", ttr.StartToken)
		}
		if !endTokens.Has(ttr.EndToken) {
			return errors.Errorf("no end token %d", ttr.StartToken)
		}

		startTokens.Remove(ttr.StartToken)
		endTokens.Remove(ttr.EndToken)
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
