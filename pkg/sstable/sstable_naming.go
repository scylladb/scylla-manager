// Copyright (C) 2023 ScyllaDB

package sstable

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
)

var (
	regexNewLaMx = regexp.MustCompile(`(la|m[cde])-([^-]+)-(\w+)-(.*)`)
	regexLaMx    = regexp.MustCompile(`(la|m[cde])-(\d+)-(\w+)-(.*)`)
	regexKa      = regexp.MustCompile(`(\w+)-(\w+)-ka-(\d+)-(.*)`)
)

const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"

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

func encodeBase36(input uint64) string {
	// math.Log(math.MaxUint64) / math.Log(36) < 16, so we are safe
	var output [16]byte
	var i int
	for i = len(output) - 1; ; i-- {
		output[i] = alphabet[input%36]
		input /= 36
		if input == 0 {
			break
		}
	}
	return string(output[i:])
}

// RandomSSTableUUID generates random sstable uuid in a form of `497z_213k_3zpaqrwk2na921344z`.
func RandomSSTableUUID() string {
	// "time" package does not define Day, so..
	const Day = 24 * time.Hour
	// sstable uses UUID v1
	uuidv1 := uuid.Must(uuid.NewUUID())

	secs, nsecs := uuidv1.Time().UnixTime()
	seconds := time.Duration(secs) * time.Second
	days := seconds.Truncate(Day)
	seconds -= days
	// Cassandra's UUID representation encodes the higher 8 bytes as a single
	// 64-bit number, so let's keep this way.
	msb := binary.BigEndian.Uint64(uuidv1[8:])
	// related scylla code, that does thvle same is here:
	// https://github.com/scylladb/scylladb/blob/f014ccf36962135889a84cff15b0478d711b2306/sstables/generation_type.hh#L258-L262
	return fmt.Sprintf("%04s_%04s_%05s%013s",
		encodeBase36(uint64(days.Hours()/24)),
		encodeBase36(uint64(seconds.Seconds())),
		// the timestamp of UUID v1 is measured in units of 100 nanoseconds
		encodeBase36(uint64(nsecs/100)),
		encodeBase36(msb))
}
