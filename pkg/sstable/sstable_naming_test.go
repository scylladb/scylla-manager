// Copyright (C) 2023 ScyllaDB

package sstable

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"go.uber.org/atomic"
)

func TestSExtractID(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		in       string
		expected string
	}{
		{
			in:       "me-3g7k_098r_4wtqo2asamoc1i8h9n-big-CRC.db",
			expected: "3g7k_098r_4wtqo2asamoc1i8h9n",
		},
		{
			in:       "md-7-big-Statistics.db",
			expected: "7",
		},
		{
			in:       "mc-7-big-Statistics.db",
			expected: "7",
		},
		{
			in:       "me-7-big-TOC.txt",
			expected: "7",
		},
		{
			in:       "me-17-big-Digest.crc32",
			expected: "17",
		},
		{
			in:       "la-7-big-TOC.txt",
			expected: "7",
		},
		{
			in:       "keyspace1-standard1-ka-1-CRC.db",
			expected: "1",
		},
	}

	for id := range testCases {
		tcase := testCases[id]
		t.Run(tcase.in, func(t *testing.T) {
			t.Parallel()
			val := ExtractID(tcase.in)
			if tcase.expected != val {
				t.Fatalf("expected %s, received %s", tcase.expected, val)
			}
		})
	}
}

func TestReplaceID(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		in       string
		expected string
	}{
		{
			in:       "me-3g7k_098r_4wtqo2asamoc1i8h9n-big-CRC.db",
			expected: "me-1-big-CRC.db",
		},
		{
			in:       "md-7-big-Statistics.db",
			expected: "md-1-big-Statistics.db",
		},
		{
			in:       "mc-7-big-Statistics.db",
			expected: "mc-1-big-Statistics.db",
		},
		{
			in:       "me-7-big-TOC.txt",
			expected: "me-1-big-TOC.txt",
		},
		{
			in:       "la-7-big-TOC.txt",
			expected: "la-1-big-TOC.txt",
		},
		{
			in:       "me-17-big-Digest.crc32",
			expected: "me-1-big-Digest.crc32",
		},
		{
			in:       "keyspace1-standard1-ka-7-CRC.db",
			expected: "keyspace1-standard1-ka-1-CRC.db",
		},
	}

	for id := range testCases {
		tcase := testCases[id]
		t.Run(tcase.in, func(t *testing.T) {
			t.Parallel()
			val := replaceID(tcase.in, "1")
			if tcase.expected != val {
				t.Fatalf("expected %s, received %s", tcase.expected, val)
			}
		})
	}
}

func TestRenameSStables(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		in       []string
		expected map[string]string
	}{
		{
			in: []string{
				"me-3g7k_098r_4wtqo2asamoc1i8h9n-big-CompressionInfo.db",
				"me-3g7k_098r_4wtqo2asamoc1i8h9n-big-Data.db",
				"me-3g7k_098r_4wtqo2asamoc1i8h9n-big-Digest.crc32",
				"me-3g7k_098r_4wtqo2asamoc1i8h9n-big-Filter.db",
				"me-3g7k_098r_4wtqo2asamoc1i8h9n-big-TOC.txt",
				"me-497z_213k_3zpaqrwk2na921344z-big-CompressionInfo.db",
				"me-497z_213k_3zpaqrwk2na921344z-big-Data.db",
				"me-497z_213k_3zpaqrwk2na921344z-big-Digest.crc32",
				"me-497z_213k_3zpaqrwk2na921344z-big-Filter.db",
				"me-497z_213k_3zpaqrwk2na921344z-big-TOC.txt",
			},
			expected: map[string]string{
				"me-3g7k_098r_4wtqo2asamoc1i8h9n-big-CompressionInfo.db": "me-1-big-CompressionInfo.db",
				"me-3g7k_098r_4wtqo2asamoc1i8h9n-big-Data.db":            "me-1-big-Data.db",
				"me-3g7k_098r_4wtqo2asamoc1i8h9n-big-Digest.crc32":       "me-1-big-Digest.crc32",
				"me-3g7k_098r_4wtqo2asamoc1i8h9n-big-Filter.db":          "me-1-big-Filter.db",
				"me-3g7k_098r_4wtqo2asamoc1i8h9n-big-TOC.txt":            "me-1-big-TOC.txt",
				"me-497z_213k_3zpaqrwk2na921344z-big-CompressionInfo.db": "me-2-big-CompressionInfo.db",
				"me-497z_213k_3zpaqrwk2na921344z-big-Data.db":            "me-2-big-Data.db",
				"me-497z_213k_3zpaqrwk2na921344z-big-Digest.crc32":       "me-2-big-Digest.crc32",
				"me-497z_213k_3zpaqrwk2na921344z-big-Filter.db":          "me-2-big-Filter.db",
				"me-497z_213k_3zpaqrwk2na921344z-big-TOC.txt":            "me-2-big-TOC.txt",
			},
		},
		{
			in: []string{
				"me-7-big-CompressionInfo.db",
				"me-7-big-Data.db",
				"me-7-big-Digest.crc32",
				"me-7-big-Filter.db",
				"me-7-big-TOC.txt",
				"me-1-big-CompressionInfo.db",
				"me-1-big-Data.db",
				"me-1-big-Digest.crc32",
				"me-1-big-Filter.db",
				"me-1-big-TOC.txt",
			},
			expected: map[string]string{
				"me-7-big-CompressionInfo.db": "me-1-big-CompressionInfo.db",
				"me-7-big-Data.db":            "me-1-big-Data.db",
				"me-7-big-Digest.crc32":       "me-1-big-Digest.crc32",
				"me-7-big-Filter.db":          "me-1-big-Filter.db",
				"me-7-big-TOC.txt":            "me-1-big-TOC.txt",
				"me-1-big-CompressionInfo.db": "me-2-big-CompressionInfo.db",
				"me-1-big-Data.db":            "me-2-big-Data.db",
				"me-1-big-Digest.crc32":       "me-2-big-Digest.crc32",
				"me-1-big-Filter.db":          "me-2-big-Filter.db",
				"me-1-big-TOC.txt":            "me-2-big-TOC.txt",
			},
		},
	}

	for id := range testCases {
		tcase := testCases[id]
		t.Run(strconv.Itoa(id), func(t *testing.T) {
			t.Parallel()
			counter := atomic.Uint64{}
			val := RenameSStables(tcase.in, func(id string) string {
				return strconv.Itoa(int(counter.Add(1)))
			})
			if diff := cmp.Diff(tcase.expected, val); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func decodeBase36(input string, t *testing.T) uint64 {
	if len(input) > 13 {
		t.Fatalf("out of range: %s", input)
	}
	output := uint64(0)
	for _, v := range input {
		output *= uint64(len(alphabet))
		index := strings.IndexByte(alphabet, byte(v))
		if index < 0 {
			t.Fatalf("malformatted base36 string: %s", input)
		}
		output += uint64(index)
	}
	return output
}

const Decimicrosecond = 100 * time.Nanosecond

func decodeUUID(input string, t *testing.T) (uuid.Time, uint64) {
	re := regexp.MustCompile(`(?P<days>\w{4})_(?P<seconds>\w{4})_(?P<decimicrosecs>\w{5})(?P<msb>\w{13})`)
	matches := re.FindStringSubmatch(input)
	if matches == nil {
		t.Errorf("RandomSSTableUUID generated value '%s' that does not match regexp", input)
	}
	days := decodeBase36(matches[re.SubexpIndex("days")], t)
	seconds := decodeBase36(matches[re.SubexpIndex("seconds")], t)
	decimicrosecs := decodeBase36(matches[re.SubexpIndex("decimicrosecs")], t)
	msb := decodeBase36(matches[re.SubexpIndex("msb")], t)

	timestamp := (time.Duration(days*24)*time.Hour +
		time.Duration(seconds)*time.Second +
		time.Duration(decimicrosecs)*Decimicrosecond)
	return uuid.Time(timestamp.Nanoseconds() / 100), msb
}

func TestRandomSSTableUUID(t *testing.T) {
	val1 := RandomSSTableUUID()
	timestamp1, _ := decodeUUID(val1, t)

	val2 := RandomSSTableUUID()
	timestamp2, _ := decodeUUID(val2, t)

	// Assume it does not take more than 1 second to generate and decode a UUID
	elapsed_ns100 := timestamp2 - timestamp1
	elapsed := time.Duration(elapsed_ns100) * Decimicrosecond
	if elapsed.Seconds() < 0 || elapsed.Seconds() > 1 {
		t.Fatal("time screw?")
	}
}
