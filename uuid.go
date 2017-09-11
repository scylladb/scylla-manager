// Copyright (C) 2017 ScyllaDB

package mermaid

import (
	"github.com/gocql/gocql"
)

type UUID = gocql.UUID

// UUIDFromUint64 creates UUID from uint64 pair.
func UUIDFromUint64(l, h uint64) UUID {
	var u UUID

	u[0] = byte(l)
	u[1] = byte(l >> 8)
	u[2] = byte(l >> 16)
	u[3] = byte(l >> 24)
	u[4] = byte(l >> 32)
	u[5] = byte(l >> 40)
	u[6] = byte(l >> 48)
	u[7] = byte(l >> 56)

	u[8] = byte(h)
	u[9] = byte(h >> 8)
	u[10] = byte(h >> 16)
	u[11] = byte(h >> 24)
	u[12] = byte(h >> 32)
	u[13] = byte(h >> 40)
	u[14] = byte(h >> 48)
	u[15] = byte(h >> 56)

	u[6] &= 0x0F // clear version
	u[6] |= 0x40 // set version to 4 (random uuid)
	u[8] &= 0x3F // clear variant
	u[8] |= 0x80 // set to IETF variant

	return u
}
