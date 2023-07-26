// Copyright (C) 2023 ScyllaDB

package sstable

import (
	"crypto/rand"
	"encoding/binary"
	"io"
	"math/bits"
	mathrand "math/rand"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"go.uber.org/atomic"
)

var (
	regexNewLaMx = regexp.MustCompile(`(la|m[cde])-([^-]+)-(\w+)-(.*)`)
	regexLaMx    = regexp.MustCompile(`(la|m[cde])-(\d+)-(\w+)-(.*)`)
	regexKa      = regexp.MustCompile(`(\w+)-(\w+)-ka-(\d+)-(.*)`)

	letters      = "abcdefghijklmnopqrstuvwxyz0123456789"
	pseudoRandom = newTimeBasedRandom()
)

const (
	incLow  = 1442695040888963407
	incHigh = 6364136223846793005
	mulLow  = 4865540595714422341
	mulHigh = 2549297995355413924
)

// ExtractID returns ID from SSTable name in a form string integer or UUID:
// me-3g7k_098r_4wtqo2asamoc1i8h9n-big-CRC.db -> 3g7k_098r_4wtqo2asamoc1i8h9n
// me-7-big-TOC.txt -> 7
// Supported SSTable format versions are: "mc", "md", "me", "la", "ka".
// Scylla code validating SSTable format can be found here:
// https://github.com/scylladb/scylladb/blob/decbc841b749d8dd3e2ddd4be4817c57d905eff2/sstables/sstables.cc#L2115-L2117
func ExtractID(sstable string) string {
	parts := strings.Split(sstable, "-")

	if regexLaMx.MatchString(sstable) || regexNewLaMx.MatchString(sstable) {
		return parts[1]
	}
	if regexKa.MatchString(sstable) {
		return parts[3]
	}

	panic(unknownSSTableError(sstable))
}

func replaceID(sstable, newID string) string {
	parts := strings.Split(sstable, "-")

	switch {
	case regexLaMx.MatchString(sstable) || regexNewLaMx.MatchString(sstable):
		parts[1] = newID
	case regexKa.MatchString(sstable):
		parts[3] = newID
	default:
		panic(unknownSSTableError(sstable))
	}

	return strings.Join(parts, "-")
}

func unknownSSTableError(sstable string) error {
	return errors.Errorf("unknown SSTable format version: %s. Supported versions are: 'mc', 'md', 'me', 'la', 'ka'", sstable)
}

// RenameToIDs reformat sstables ids to ID format keeping id consistency and uniqueness.
func RenameToIDs(sstables []string, globalCounter *atomic.Int64) map[string]string {
	return RenameSStables(sstables, func(_ string) string {
		return strconv.Itoa(int(globalCounter.Inc()))
	})
}

// RenameToUUIDs reformat sstables ids to UUID format keeping id consistency.
func RenameToUUIDs(sstables []string) map[string]string {
	return RenameSStables(sstables, func(id string) string {
		if !regexLaMx.MatchString(id) && regexNewLaMx.MatchString(id) {
			return id
		}
		return RandomSSTableUUID()
	})
}

// RenameSStables resolves sstable file name conflicts replacing id in the names with unique id value.
// SSTables originating from different nodes could have identical names, leading to the conflict in which one overwrites another during download.
// To avoid that we need to remap them to unique names.
func RenameSStables(sstables []string, nameGen func(id string) string) map[string]string {
	idMapping := make(map[string]string)
	out := make(map[string]string)

	for _, sst := range sstables {
		id := ExtractID(sst)
		newID, ok := idMapping[id]
		if !ok {
			newID = nameGen(id)
			idMapping[id] = newID
		}

		out[sst] = replaceID(sst, newID)
	}

	return out
}

// RandomSSTableUUID generates random sstable uuid in a form of `497z_213k_3zpaqrwk2na921344z`.
func RandomSSTableUUID() string {
	var buff [26]byte
	_, err := io.ReadFull(rand.Reader, buff[:])
	if err != nil {
		pseudoRandom.Read(buff[:])
	}
	var out [26]byte
	for id, val := range buff {
		out[id] = letters[val%36]
	}
	// related scylla code, that does the same is here:
	// https://github.com/scylladb/scylladb/blob/f014ccf36962135889a84cff15b0478d711b2306/sstables/generation_type.hh#L258-L262
	return string(out[0:4]) + "_" + string(out[4:8]) + "_" + string(out[8:])
}

func newTimeBasedRandom() *mathrand.Rand {
	return mathrand.New(mathrand.NewSource(int64(getUint64FromTime())))
}

// getUint64FromTime returns pseudorandom value based on time.
// It partially follows simple fast statistically good algorithm https://www.pcg-random.org/pdf/toms-oneill-pcg-family-v1.02.pdf
// to make it more random.
func getUint64FromTime() uint64 {
	//nolint:errcheck
	binTime, _ := timeutc.Now().MarshalBinary()
	high := binary.BigEndian.Uint64(binTime[5:13])
	low := binary.LittleEndian.Uint64(binTime[5:13])

	hi, lo := bits.Mul64(low, mulLow)
	hi += high * mulLow
	hi += low * mulHigh
	low = lo
	high = hi

	low, carry := bits.Add64(low, incLow, 0)
	high, _ = bits.Add64(high, incHigh, carry)

	return bits.RotateLeft64(high^low, -int(high>>58))
}
