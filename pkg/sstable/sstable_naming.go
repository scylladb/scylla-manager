// Copyright (C) 2026 ScyllaDB

package sstable

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var (
	regexNewLaMx = regexp.MustCompile(`(la|m[cdest])-([^-]+)-(\w+)-(.*)`)
	regexLaMx    = regexp.MustCompile(`(la|m[cdest])-(\d+)-(\w+)-(.*)`)
	regexAnyLaMx = regexp.MustCompile(`([A-Za-z]{2})-((?P<days>[0-9a-z]{4})_(?P<seconds>[0-9a-z]{4})_(?P<decimicrosecs>[0-9a-z]{5})(?P<msb>[0-9a-z]{13}))-(\w+)-(.*)`)
	regexKa      = regexp.MustCompile(`(\w+)-(\w+)-ka-(\d+)-(.*)`)
)

const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"

// ExtractID returns ID from SSTable name in a form string integer or UUID:
// me-3g7k_098r_4wtqo2asamoc1i8h9n-big-CRC.db -> 3g7k_098r_4wtqo2asamoc1i8h9n
// me-7-big-TOC.txt -> 7
// Supported SSTable format versions are: "mc", "md", "me", "ms", "mt", "la", "ka".
// Scylla code validating SSTable format can be found here:
// https://github.com/scylladb/scylladb/blob/decbc841b749d8dd3e2ddd4be4817c57d905eff2/sstables/sstables.cc#L2115-L2117
func ExtractID(sstable string) (string, error) {
	if matches := regexNewLaMx.FindStringSubmatch(sstable); matches != nil {
		return matches[2], nil
	}
	if matches := regexLaMx.FindStringSubmatch(sstable); matches != nil {
		return matches[2], nil
	}
	if matches := regexKa.FindStringSubmatch(sstable); matches != nil {
		return matches[3], nil
	}
	if id := extractAnySSTableUUID(sstable); id != "" {
		return id, nil
	}

	return "", unknownSSTableError(sstable)
}

func replaceID(sstable, newID string) string {
	parts := strings.Split(sstable, "-")

	switch {
	case regexLaMx.MatchString(sstable), regexNewLaMx.MatchString(sstable), extractAnySSTableUUID(sstable) != "":
		parts[1] = newID
	case regexKa.MatchString(sstable):
		parts[3] = newID
	default:
		panic(unknownSSTableError(sstable))
	}

	return strings.Join(parts, "-")
}

func unknownSSTableError(sstable string) error {
	return errors.Errorf("unknown SSTable format version: %s. Supported versions are: 'mc', 'md', 'me', 'ms', 'mt', 'la', 'ka'", sstable)
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

func extractAnySSTableUUID(sstable string) string {
	matches := regexAnyLaMx.FindStringSubmatch(sstable)
	if matches == nil {
		return ""
	}
	for _, name := range []string{"days", "seconds", "decimicrosecs", "msb"} {
		if _, err := decodeBase36(matches[regexAnyLaMx.SubexpIndex(name)]); err != nil {
			return ""
		}
	}
	return matches[2]
}

func decodeBase36(input string) (uint64, error) {
	if len(input) > 13 {
		return 0, errors.Errorf("out of range: %s", input)
	}
	output := uint64(0)
	for _, v := range input {
		output *= uint64(len(alphabet))
		index := strings.IndexByte(alphabet, byte(v))
		if index < 0 {
			return 0, errors.Errorf("malformatted base36 string: %s", input)
		}
		output += uint64(index)
	}

	return output, nil
}

// ComponentSuffix defines SSTable components by their suffixes.
// Definitions and documentation were taken from:
// https://github.com/scylladb/scylladb/blob/master/docs/dev/sstables-directory-structure.md#sstable-files.
//
// Note that SSTables of different format version might consist
// of different set of component files.
type ComponentSuffix string

const (
	// ComponentData is the SSTable data file,
	// containing a part of the actual data stored in the database.
	ComponentData ComponentSuffix = "Data.db"

	// ComponentIndex of the row keys with pointers to their positions in the data file.
	ComponentIndex ComponentSuffix = "Index.db"

	// ComponentPartitions is the inter-partition part of trie-based SSTable indexes.
	// Always used in conjunction with Rows.db.
	// Used in some newer format versions as a replacement for Index.db.
	ComponentPartitions ComponentSuffix = "Partitions.db"

	// ComponentRows is the intra-partition part of trie-based SSTable indexes.
	// Always used in conjunction with Partitions.db.
	// Used in some newer format versions as a replacement for Index.db.
	ComponentRows ComponentSuffix = "Rows.db"

	// ComponentFilter is a structure stored in memory that checks
	// if row data exists in the memtable before accessing SSTables on disk.
	ComponentFilter ComponentSuffix = "Filter.db"

	// ComponentCompressionInfo is a file holding information about
	// uncompressed data length, chunk offsets and other compression information.
	ComponentCompressionInfo ComponentSuffix = "CompressionInfo.db"

	// ComponentStatistics is a statistical metadata about the content of the SSTable
	// and encoding statistics for the data file, starting with the mc format.
	ComponentStatistics ComponentSuffix = "Statistics.db"

	// ComponentSummary holds a sample of the partition index stored in memory.
	ComponentSummary ComponentSuffix = "Summary.db"

	// ComponentTOC is a file that stores the list of all components for the SSTable TOC.
	// See details below regarding the use of a temporary TOC name during creation and deletion of SSTables.
	ComponentTOC ComponentSuffix = "TOC.txt"

	// ComponentScylla holds scylla-specific metadata about the SSTable,
	// such as sharding information, extended features support, and sstabe-run identifier.
	ComponentScylla ComponentSuffix = "Scylla.db"

	// ComponentCRC holds the CRC32 for chunks in an uncompressed file.
	ComponentCRC ComponentSuffix = "CRC.db"

	// ComponentDigest are files holding the checksum of the data file.
	// The method used for checksum is specific to the SSTable format version.

	// ComponentDigestCRC holds crc32 checksum.
	ComponentDigestCRC ComponentSuffix = "Digest.crc32"
	// ComponentDigestADLER holds adler32 checksum.
	ComponentDigestADLER ComponentSuffix = "Digest.adler32"
	// ComponentDigestSHA1 holds sha1 checksum.
	ComponentDigestSHA1 ComponentSuffix = "Digest.sha1"
)

// IDType describes possible formats of SSTable ID.
type IDType int

const (
	// IntegerID describes integer based ID.
	// It used to be the only IDType supported by the older Scylla version.
	// Note that cluster with newer Scylla versions might still contain
	// SSTables with this type of ID, as they need to be compacted
	// in order to be re-written with the UUID.
	IntegerID IDType = iota
	// UUID describes UUID based ID.
	// This is the default IDType for the newer Scylla versions.
	UUID
)

// GetIDType returns the IDType of provided ID.
func GetIDType(id string) IDType {
	if _, err := strconv.Atoi(id); err == nil {
		return IntegerID
	}
	return UUID
}

// ID describes SSTable ID.
type ID struct {
	Type IDType
	ID   string
}

// ParseID returns the ID of provided SSTable component.
func ParseID(component string) (ID, error) {
	id, err := ExtractID(component)
	if err != nil {
		return ID{}, err
	}
	return ID{
		Type: GetIDType(id),
		ID:   id,
	}, nil
}
