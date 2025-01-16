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
				{Source: []string{"dc1"}, Target: []string{"dc2"}},
			},
		},
		{
			input: "dc1, dc2=>dc1, dc2",
			expectedMappings: dcMappings{
				{Source: []string{"dc1", "dc2"}, Target: []string{"dc1", "dc2"}},
			},
		},
		{
			input: "dc1=>dc3;dc2=>dc4",
			expectedMappings: dcMappings{
				{Source: []string{"dc1"}, Target: []string{"dc3"}},
				{Source: []string{"dc2"}, Target: []string{"dc4"}},
			},
		},
		{
			input: "dc1,dc2=>dc3",
			expectedMappings: dcMappings{
				{Source: []string{"dc1", "dc2"}, Target: []string{"dc3"}},
			},
		},
		{
			input: "dc1,!dc2=>dc3",
			expectedMappings: dcMappings{
				{
					Source:       []string{"dc1"},
					Target:       []string{"dc3"},
					IgnoreSource: []string{"dc2"},
				},
			},
		},
		{
			input: "dc1,!dc2=>dc3,!dc4",
			expectedMappings: dcMappings{
				{
					Source:       []string{"dc1"},
					Target:       []string{"dc3"},
					IgnoreSource: []string{"dc2"},
					IgnoreTarget: []string{"dc4"},
				},
			},
		},
		{
			input: "dc1,!dc2=>dc3,!dc4",
			expectedMappings: dcMappings{
				{
					Source: []string{"dc1"},
					Target: []string{"dc3"},

					IgnoreSource: []string{"dc2"},
					IgnoreTarget: []string{"dc4"},
				},
			},
		},
		{
			input: "!dc1,dc2=>dc3,!dc4",
			expectedMappings: dcMappings{
				{
					Source: []string{"dc2"},
					Target: []string{"dc3"},

					IgnoreSource: []string{"dc1"},
					IgnoreTarget: []string{"dc4"},
				},
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
			slices.EqualFunc(tc.expectedMappings, mappings, func(a, b dcMapping) bool {
				return slices.Equal(a.Source, b.Source) &&
					slices.Equal(a.Target, b.Target) &&
					slices.Equal(a.IgnoreSource, b.IgnoreSource) &&
					slices.Equal(a.IgnoreTarget, b.IgnoreTarget)
			})
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
				{Source: []string{"dc1"}, Target: []string{"dc2"}},
			},
			expected: "dc1=>dc2",
		},
		{
			mappings: dcMappings{
				{Source: []string{"dc1"}, Target: []string{"dc2"}},
				{Source: []string{"dc3"}, Target: []string{"dc4"}},
			},
			expected: "dc1=>dc2;dc3=>dc4",
		},
		{
			mappings: dcMappings{
				{
					Source:       []string{"dc1"},
					Target:       []string{"dc2"},
					IgnoreSource: []string{"dc2"},
				},
			},
			expected: "dc1,!dc2=>dc2",
		},
		{
			mappings: dcMappings{
				{
					Source:       []string{"dc1"},
					Target:       []string{"dc2"},
					IgnoreSource: []string{"dc2"},
					IgnoreTarget: []string{"dc3"},
				},
			},
			expected: "dc1,!dc2=>dc2,!dc3",
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
