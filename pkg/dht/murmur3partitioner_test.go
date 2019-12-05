// Copyright (C) 2017 ScyllaDB

package dht

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestZeroBased(t *testing.T) {
	t.Parallel()

	f, err := os.Open("testdata/murmur3_tokens.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var tokens []int64
	if err := json.NewDecoder(f).Decode(&tokens); err != nil {
		t.Fatal(err)
	}

	for _, v := range tokens {
		if unzeroBased(zeroBased(v)) != v {
			t.Error("invalid value for token", v)
		}
	}
}

func TestMurmur3PartitionerInitZeroBasedShardStart(t *testing.T) {
	t.Parallel()

	f, err := os.Open("testdata/murmur3_token_zero_based_shard_start.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var table []struct {
		S []uint64 `json:"shard_start"`
		N uint     `json:"shard_count"`
		M uint     `json:"msb"`
	}
	if err := json.NewDecoder(f).Decode(&table); err != nil {
		t.Fatal(err)
	}

	for i, test := range table {
		p := NewMurmur3Partitioner(test.N, test.M)
		if diff := cmp.Diff(p.shardStart, test.S); diff != "" {
			t.Error("invalid shard start at pos", i, diff)
		}
	}
}

func TestMurmur3PartitionerShardOf(t *testing.T) {
	t.Parallel()

	f, err := os.Open("testdata/murmur3_token_shard.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var table []struct {
		S uint  `json:"shard"`
		T int64 `json:"token"`
		N uint  `json:"shard_count"`
		M uint  `json:"msb"`
	}
	if err := json.NewDecoder(f).Decode(&table); err != nil {
		t.Fatal(err)
	}

	for i, test := range table {
		p := NewMurmur3Partitioner(test.N, test.M)
		if p.ShardOf(test.T) != test.S {
			t.Error("invalid shard at pos", i, "expected", test.S, "got", p.ShardOf(test.T))
		}
	}
}

func TestMurmur3PartitionerPrevShard(t *testing.T) {
	t.Parallel()

	p := NewMurmur3Partitioner(8, 12)
	if p.PrevShard(0) != 7 {
		t.Fatal("wrong prev")
	}
	for i := uint(1); i < 8; i++ {
		if p.PrevShard(i) != i-1 {
			t.Fatal("wrong prev")
		}
	}
}

func TestMurmur3PartitionerTokenForPrevShard(t *testing.T) {
	t.Parallel()

	f, err := os.Open("testdata/murmur3_token_for_prev_shard.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var table []struct {
		T int64 `json:"token"`
		S uint  `json:"shard"`
		N uint  `json:"shard_count"`
		M uint  `json:"msb"`
		V int64 `json:"value"`
	}
	if err := json.NewDecoder(f).Decode(&table); err != nil {
		t.Fatal(err)
	}

	for i, test := range table {
		p := NewMurmur3Partitioner(test.N, test.M)
		// skip the bug in Asias code
		if test.T == Murmur3MinToken {
			continue
		}
		if p.TokenForPrevShard(test.T, test.S) != test.V {
			t.Error("invalid value at pos", i, test)
		}
	}
}
