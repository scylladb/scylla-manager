// Copyright (C) 2026 ScyllaDB

package scyllaclient_test

import (
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/dht"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

// TestMaxRepairRangesFitQueryParam verifies that MaxRepairRanges ranges
// with the longest encoding fit the ranges query param limit.
func TestMaxRepairRangesFitQueryParam(t *testing.T) {
	// Tokens with the longest string representation
	ranges := make([]scyllaclient.TokenRange, scyllaclient.MaxRepairRanges)
	last := len(ranges) - 1
	for i := range last {
		ranges[i] = scyllaclient.TokenRange{
			StartToken: dht.Murmur3MinToken + 1 + int64(i),
			EndToken:   dht.Murmur3MinToken + 2 + int64(i),
		}
	}
	// Wraparound range closing the ring is sent as two ranges
	ranges[last] = scyllaclient.TokenRange{
		StartToken: dht.Murmur3MinToken + 1 + int64(last),
		EndToken:   dht.Murmur3MinToken + 1,
	}

	q := url.Values{"ranges": {scyllaclient.DumpRanges(ranges)}}.Encode()
	if size := len(q) - len("ranges="); size > scyllaclient.MaxListQueryParamSize {
		t.Fatalf("Ranges query param size %d exceeds limit %d", size, scyllaclient.MaxListQueryParamSize)
	}
}

// TestBatchListQueryParam verifies that list query param items are split
// into batches fitting the size limit, without reordering or losing items.
func TestBatchListQueryParam(t *testing.T) {
	const maxTableNameLen = 192

	sepLen := len(url.QueryEscape(","))
	longName := strings.Repeat("t", maxTableNameLen)
	longNamesPerBatch := scyllaclient.MaxListQueryParamSize / (maxTableNameLen + sepLen)
	// Limit is a multiple of 128B, so names taking 128B with separator fill it exactly
	const fillingItemSize = 128
	fillingName := strings.Repeat("t", fillingItemSize-sepLen)
	fillingNameCnt := scyllaclient.MaxListQueryParamSize / fillingItemSize

	testCases := []struct {
		name       string
		items      []string
		batchSizes []int
	}{
		{
			name: "no items",
		},
		{
			name:       "single batch",
			items:      []string{"t1", "t2", "t3"},
			batchSizes: []int{3},
		},
		{
			name:       "long names",
			items:      slices.Repeat([]string{longName}, 2*longNamesPerBatch+1),
			batchSizes: []int{longNamesPerBatch, longNamesPerBatch, 1},
		},
		{
			name:       "names exactly filling limit",
			items:      slices.Repeat([]string{fillingName}, fillingNameCnt),
			batchSizes: []int{fillingNameCnt},
		},
		{
			name:       "names exceeding limit by one",
			items:      slices.Repeat([]string{fillingName}, fillingNameCnt+1),
			batchSizes: []int{fillingNameCnt, 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batches := scyllaclient.BatchListQueryParam(tc.items)

			var batchSizes []int
			for _, b := range batches {
				if len(b) == 0 {
					t.Error("Unexpected empty batch")
				}
				batchSizes = append(batchSizes, len(b))
				size := len(url.Values{"cf": {strings.Join(b, ",")}}.Encode()) - len("cf=")
				if size > scyllaclient.MaxListQueryParamSize {
					t.Errorf("Batch of %d items exceeds limit: %d", len(b), size)
				}
			}
			if !slices.Equal(batchSizes, tc.batchSizes) {
				t.Errorf("Expected batch sizes %v, got %v", tc.batchSizes, batchSizes)
			}
			if flat := slices.Concat(batches...); !slices.Equal(flat, tc.items) {
				t.Errorf("Expected items %v, got %v", tc.items, flat)
			}
		})
	}
}
