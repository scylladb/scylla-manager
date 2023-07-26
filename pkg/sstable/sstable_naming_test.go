// Copyright (C) 2023 ScyllaDB

package sstable

import (
	"regexp"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
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

func TestRandomSSTableUUID(t *testing.T) {
	re := regexp.MustCompile(`(\w{4})_(\w{4})_(\w+)`)
	val := RandomSSTableUUID()
	if !re.MatchString(val) {
		t.Errorf("RandomSSTableUUID generated value '%s' that does not match regexp", val)
	}
}
