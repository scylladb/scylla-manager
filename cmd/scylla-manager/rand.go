// Copyright (C) 2017 ScyllaDB

package main

import (
	crand "crypto/rand"
	"math"
	"math/big"
	"math/rand"

	"github.com/scylladb/mermaid/timeutc"
)

// seedMathRand provides weak, but guaranteed seeding, which is better than
// running with Go's default seed of 1. A call to SeedMathRand() is expected
// to be called via init(), but never a second time.
func seedMathRand() {
	n, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		rand.Seed(timeutc.Now().UnixNano())
		return
	}
	rand.Seed(n.Int64())
}
