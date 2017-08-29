package repair

import (
	"encoding/binary"
	"math"

	"github.com/cespare/xxhash"
	"github.com/scylladb/mermaid"
)

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
