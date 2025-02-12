// Copyright (C) 2025 ScyllaDB

package one2onerestore

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
)

func TestNodesMappingSet(t *testing.T) {
	testCases := []struct {
		name         string
		nodesMapping string

		expectedErr bool
	}{
		{
			name:         "one line",
			nodesMapping: "./testdata/nodesmapping_oneline.txt",
		},
		{
			name:         "multi line",
			nodesMapping: "./testdata/nodesmapping_multiline.txt",
		},
		{
			name:         "whitespace",
			nodesMapping: "./testdata/nodesmapping_whitespace.txt",
		},
		{
			name:         "without =",
			nodesMapping: "./testdata/nodesmapping_no=.txt",
			expectedErr:  true,
		},
		{
			name:         "without dc in source",
			nodesMapping: "./testdata/nodesmapping_no_dc.txt",
			expectedErr:  true,
		},
		{
			name:         "without dc in target",
			nodesMapping: "./testdata/nodesmapping_no_dc2.txt",
			expectedErr:  true,
		},
		{
			name:         "file not exists",
			nodesMapping: "./testdata/not_found.404",
			expectedErr:  true,
		},
		{
			name:         "empty file",
			nodesMapping: "./testdata/empty.txt",
			expectedErr:  true,
		},
		{
			name:         "dc count mismatch, 1 in source, but 2 in target",
			nodesMapping: "./testdata/nodesmapping_dc_mismatch_source.txt",
			expectedErr:  true,
		},
		{
			name:         "dc count mismatch, 2 in source, but 1 in target",
			nodesMapping: "./testdata/nodesmapping_dc_mismatch_target.txt",
			expectedErr:  true,
		},
		{
			name:         "rack count mismatch, 1 in source, but 2 in target",
			nodesMapping: "./testdata/nodesmapping_rack_mismatch_source.txt",
			expectedErr:  true,
		},
		{
			name:         "rack count mismatch, 2 in source, but 1 in target",
			nodesMapping: "./testdata/nodesmapping_rack_mismatch_target.txt",
			expectedErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var mapping nodesMapping
			err := mapping.Set(tc.nodesMapping)
			if tc.expectedErr && err == nil {
				t.Fatalf("Expected err, got nil")
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected err, got %v", err)
			}

			testutils.SaveGoldenJSONFileIfNeeded(t, &mapping)
			var expectedMapping nodesMapping
			testutils.LoadGoldenJSONFile(t, &expectedMapping)
			if diff := cmp.Diff(expectedMapping, mapping); diff != "" {
				t.Fatalf("Unexpected mappings: \n%s", diff)
			}
		})
	}
}
