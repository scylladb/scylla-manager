// Copyright (C) 2025 ScyllaDB
package restore

import (
	"fmt"
	"slices"
	"testing"
)

func TestSetDCMapping(t *testing.T) {
	testCases := []struct {
		input            string
		expectedErr      bool
		expectedMappings dcMappings
	}{
		{
			input: "dc1=>dc2",
			expectedMappings: dcMappings{
				{Source: "dc1", Target: "dc2"},
			},
		},
		{
			input: " dc1 => dc1 ",
			expectedMappings: dcMappings{
				{Source: "dc1", Target: "dc1"},
			},
		},
		{
			input: "dc1=>dc3;dc2=>dc4",
			expectedMappings: dcMappings{
				{Source: "dc1", Target: "dc3"},
				{Source: "dc2", Target: "dc4"},
			},
		},
		{
			input:            "dc1=>dc3=>dc2=>dc4",
			expectedMappings: dcMappings{},
			expectedErr:      true,
		},
		{
			input:            ";",
			expectedMappings: dcMappings{},
			expectedErr:      true,
		},
		{
			input:            "=>",
			expectedMappings: dcMappings{},
			expectedErr:      true,
		},
		{
			input:            "dc1=>",
			expectedMappings: dcMappings{},
			expectedErr:      true,
		},
		{
			input:            "dc1=>;",
			expectedMappings: dcMappings{},
			expectedErr:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			var mappings dcMappings

			err := mappings.Set(tc.input)
			if tc.expectedErr && err == nil {
				t.Fatal("Expected err, but got nil")
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected err: %v", err)
			}
			if !slices.Equal(mappings, tc.expectedMappings) {
				t.Fatalf("Expected %v, but got %v", tc.expectedMappings, mappings)
			}
		})
	}

}

func TestDCMappingString(t *testing.T) {
	testCases := []struct {
		mappings dcMappings
		expected string
	}{
		{
			mappings: dcMappings{
				{Source: "dc1", Target: "dc2"},
			},
			expected: "dc1=>dc2",
		},
		{
			mappings: dcMappings{
				{Source: "dc1", Target: "dc2"},
				{Source: "dc3", Target: "dc4"},
			},
			expected: "dc1=>dc2;dc3=>dc4",
		},
		{},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			actual := tc.mappings.String()
			if actual != tc.expected {
				t.Fatalf("Expected %q, but got %q", tc.expected, actual)
			}
		})
	}
}
