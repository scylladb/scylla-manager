// Copyright (C) 2017 ScyllaDB

package uuid

import (
	"encoding/binary"

	"github.com/gocql/gocql"
)

// Nil UUID is special form of UUID that is specified to have all
// 128 bits set to zero.
// https://tools.ietf.org/html/rfc4122#section-4.1.7
var Nil UUID

// UUID is a wrapper for a UUID type, currently "github.com/gocql/gocql".UUID.
type UUID struct {
	gocql.UUID
}

// NewRandom returns a random (Version 4) UUID Nil and error if it fails to read from it's random source.
func NewRandom() (UUID, error) {
	u, err := gocql.RandomUUID()
	if err != nil {
		return Nil, err
	}
	return UUID{UUID: u}, nil
}

// NewFromUint64 creates a UUID from a uint64 pair.
func NewFromUint64(l, h uint64) UUID {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[:8], l)
	binary.LittleEndian.PutUint64(b[8:], h)

	b[6] &= 0x0F // clear version
	b[6] |= 0x40 // set version to 4 (random uuid)
	b[8] &= 0x3F // clear variant
	b[8] |= 0x80 // set to IETF variant

	var u gocql.UUID
	u, err := gocql.UUIDFromBytes(b[:])
	if err != nil {
		panic(err)
	}
	return UUID{UUID: u}
}

// MarshalCQL implements gocql.Marshaler.
func (u UUID) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	return u.UUID[:], nil
}

// UnmarshalCQL implements gocql.Unmarshaler.
func (u *UUID) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	if len(data) == 0 {
		*u = Nil
		return nil
	}

	var err error
	u.UUID, err = gocql.UUIDFromBytes(data)
	if err != nil {
		return err
	}
	return nil
}
