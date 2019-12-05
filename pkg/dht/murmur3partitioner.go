// Copyright (C) 2017 ScyllaDB

package dht

import (
	"math"
	"math/big"
)

// Full token range
const (
	Murmur3MinToken = int64(math.MinInt64)
	Murmur3MaxToken = int64(math.MaxInt64)
)

// Murmur3Partitioner see
// https://github.com/scylladb/scylla/blob/master/dht/murmur3_partitioner.hh
// https://github.com/scylladb/scylla/blob/master/dht/murmur3_partitioner.cc
type Murmur3Partitioner struct {
	shardCount            uint
	shardingIgnoreMsbBits uint
	shardStart            []uint64
}

// NewMurmur3Partitioner creates a new Murmur3Partitioner instance.
func NewMurmur3Partitioner(shardCount, shardingIgnoreMsbBits uint) *Murmur3Partitioner {
	if shardCount <= 1 {
		shardingIgnoreMsbBits = 0
	}

	p := &Murmur3Partitioner{
		shardCount:            shardCount,
		shardingIgnoreMsbBits: shardingIgnoreMsbBits,
	}
	p.initZeroBasedShardStart()

	return p
}

func (p *Murmur3Partitioner) initZeroBasedShardStart() {
	p.shardStart = make([]uint64, p.shardCount)

	if p.shardCount == 1 {
		return
	}

	var (
		t      uint64
		token  = new(big.Int)
		shards = new(big.Int)
	)
	shards.SetUint64(uint64(p.shardCount))

	for s := uint(0); s < p.shardCount; s++ {
		// uint64_t token = (uint128_t(s) << 64) / shards;
		token.SetUint64(uint64(s))
		token.Lsh(token, 64)
		token.Div(token, shards)
		// token >>= sharding_ignore_msb_bits;
		t = token.Uint64()
		t >>= p.shardingIgnoreMsbBits

		// Token is the start of the next shard, and can be slightly before due
		// to rounding errors adjust.
		for p.zeroBasedShardOf(t) != s {
			t++
		}
		p.shardStart[s] = t
	}
}

func (p *Murmur3Partitioner) zeroBasedShardOf(t uint64) uint {
	var (
		token  = new(big.Int)
		shards = new(big.Int)
	)
	// token <<= sharding_ignore_msb_bits;
	token.SetUint64(t << p.shardingIgnoreMsbBits)

	// (uint128_t(token) * shards) >> 64;
	shards.SetUint64(uint64(p.shardCount))
	token.Mul(token, shards)
	token.Rsh(token, 64)

	return uint(token.Uint64())
}

// ShardCount returns the number of shards.
func (p *Murmur3Partitioner) ShardCount() uint {
	return p.shardCount
}

// ShardOf returns shard the token belongs to.
func (p *Murmur3Partitioner) ShardOf(t int64) uint {
	return p.zeroBasedShardOf(zeroBased(t))
}

// PrevShard returns id of a previous shard in a round robin fashion.
func (p *Murmur3Partitioner) PrevShard(shard uint) uint {
	prev := shard
	if prev == 0 {
		prev = p.ShardCount()
	}
	return prev - 1
}

// TokenForPrevShard returns the start token for the shard -1
func (p *Murmur3Partitioner) TokenForPrevShard(t int64, shard uint) int64 {
	z := zeroBased(t)
	s := p.zeroBasedShardOf(z)

	if p.shardingIgnoreMsbBits == 0 {
		if shard > s {
			return Murmur3MinToken
		}
		return unzeroBased(p.shardStart[shard])
	}

	l := z >> (64 - p.shardingIgnoreMsbBits)
	if shard > s {
		l--
	}
	l <<= 64 - p.shardingIgnoreMsbBits

	return unzeroBased(l | p.shardStart[shard])
}

func zeroBased(t int64) uint64 {
	mid := uint64(1 << 63)
	if t >= 0 {
		return mid + uint64(t)
	}
	return mid - uint64(-t)
}

func unzeroBased(z uint64) int64 {
	mid := uint64(1 << 63)
	if z >= mid {
		return int64(z - mid)
	}
	return -int64(mid - z)
}
