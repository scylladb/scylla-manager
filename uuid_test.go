package mermaid

import (
	"encoding/binary"
	"testing"
)

func TestUUIDFromUint64(t *testing.T) {
	l := uint64(11400714785074694791)&(uint64(0x0F)<<48) | (uint64(0x40) << 48)
	h := uint64(14029467366897019727)&uint64(0x3F) | uint64(0x80)
	u := UUIDFromUint64(l, h)

	if l != binary.LittleEndian.Uint64(u[0:8]) {
		t.Fatal("wrong lower bits")
	}
	if h != binary.LittleEndian.Uint64(u[8:16]) {
		t.Fatal("wrong higher bits")
	}
}
