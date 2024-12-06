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
func ExtractID(sstable string) (string, error) {
	parts := strings.Split(sstable, "-")

	if regexLaMx.MatchString(sstable) || regexNewLaMx.MatchString(sstable) {
		return parts[1], nil
	}
	if regexKa.MatchString(sstable) {
		return parts[3], nil
	}

	return "", unknownSSTableError(sstable)
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
		id, err := ExtractID(sst)
		if err != nil {
			panic(err)
		}
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

// Component defines SSTable components by their suffixes.
// Definitions and documentation were taken from:
// https://github.com/scylladb/scylladb/blob/master/docs/dev/sstables-directory-structure.md#sstable-files.
//
// Note that SSTables of different format version might consist
// of different set of component files.
type Component string

const (
	// ComponentData is the SSTable data file,
	// containing a part of the actual data stored in the database.
	ComponentData Component = "Data.db"

	// ComponentIndex of the row keys with pointers to their positions in the data file.
	ComponentIndex Component = "Index.db"

	// ComponentFilter is a structure stored in memory that checks
	// if row data exists in the memtable before accessing SSTables on disk.
	ComponentFilter Component = "Filter.db"

	// ComponentCompressionInfo is a file holding information about
	// uncompressed data length, chunk offsets and other compression information.
	ComponentCompressionInfo Component = "CompressionInfo.db"

	// ComponentStatistics is a statistical metadata about the content of the SSTable
	// and encoding statistics for the data file, starting with the mc format.
	ComponentStatistics Component = "Statistics.db"

	// ComponentSummary holds a sample of the partition index stored in memory.
	ComponentSummary Component = "Summary.db"

	// ComponentTOC is a file that stores the list of all components for the SSTable TOC.
	// See details below regarding the use of a temporary TOC name during creation and deletion of SSTables.
	ComponentTOC Component = "TOC.txt"

	// ComponentScylla holds scylla-specific metadata about the SSTable,
	// such as sharding information, extended features support, and sstabe-run identifier.
	ComponentScylla Component = "Scylla.db"

	// ComponentCRC holds the CRC32 for chunks in an uncompressed file.
	ComponentCRC Component = "CRC.db"

	// ComponentDigest are files holding the checksum of the data file.
	// The method used for checksum is specific to the SSTable format version.

	// ComponentDigestCRC holds crc32 checksum.
	ComponentDigestCRC Component = "Digest.crc32"
	// ComponentDigestADLER holds adler32 checksum.
	ComponentDigestADLER Component = "Digest.adler32"
	// ComponentDigestSHA1 holds sha1 checksum.
	ComponentDigestSHA1 Component = "Digest.sha1"
)
