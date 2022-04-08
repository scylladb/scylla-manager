// Copyright (C) 2017 ScyllaDB

package repair

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/dht"
)

func TestSplitSegmentsToShards(t *testing.T) {
	t.Parallel()

	for _, shardCount := range []uint{1, 2, 3, 5, 8} {
		p := dht.NewMurmur3Partitioner(shardCount, 12)
		ttrs := []*tableTokenRange{
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
		if _, err := splitToShardsAndValidate(ttrs, p); err != nil {
			t.Fatal(err)
		}
	}
}
