// Copyright (C) 2025 ScyllaDB
package restore

import (
	"maps"
	"slices"
	"testing"
)

func TestCalculateMappings(t *testing.T) {
	testCases := []struct {
		name string

		mappings             DCMappings
		expectedTargetMap    map[string][]string
		expectedIgnoreSource []string
		expectedIgnoreTarget []string
	}{
		{
			name: "dc1=>dc2",
			mappings: []DCMapping{
				{
					Source: []string{"dc1"},
					Target: []string{"dc2"},
				},
			},
			expectedTargetMap: map[string][]string{
				"dc2": {"dc1"},
			},
		},
		{
			name: "dc1=>dc1,dc2",
			mappings: []DCMapping{
				{
					Source: []string{"dc1"},
					Target: []string{"dc1", "dc2"},
				},
			},
			expectedTargetMap: map[string][]string{
				"dc1": {"dc1"},
				"dc2": {"dc1"},
			},
		},
		{
			name: "dc1,dc2=>dc3",
			mappings: []DCMapping{
				{
					Source: []string{"dc1", "dc2"},
					Target: []string{"dc3"},
				},
			},
			expectedTargetMap: map[string][]string{
				"dc3": {"dc1", "dc2"},
			},
		},
		{
			name: "dc1,dc2=>dc2",
			mappings: []DCMapping{
				{
					Source: []string{"dc1", "dc2"},
					Target: []string{"dc2"},
				},
			},
			expectedTargetMap: map[string][]string{
				"dc2": {"dc1", "dc2"},
			},
		},
		{
			name: "empty Source",
			mappings: []DCMapping{
				{
					Source: []string{},
					Target: []string{"dc2"},
				},
			},
			expectedTargetMap: map[string][]string{},
		},
		{
			name: "empty Target",
			mappings: []DCMapping{
				{
					Source: []string{"dc1"},
					Target: []string{},
				},
			},
			expectedTargetMap: map[string][]string{},
		},
		{
			name: "dc1,dc2,dc3=>dc1,dc2",
			mappings: []DCMapping{
				{
					Source: []string{"dc1", "dc2", "dc3"},
					Target: []string{"dc1", "dc2"},
				},
			},
			expectedTargetMap: map[string][]string{
				"dc1": {"dc1"},
				"dc2": {"dc2", "dc3"},
			},
		},
		{
			name: "dc1,dc2=>dc1,dc2;dc2=>dc3",
			mappings: []DCMapping{
				{
					Source: []string{"dc1", "dc2"},
					Target: []string{"dc1", "dc2"},
				},
				{
					Source: []string{"dc2"},
					Target: []string{"dc3"},
				},
			},
			expectedTargetMap: map[string][]string{
				"dc1": {"dc1"},
				"dc2": {"dc2"},
				"dc3": {"dc2"},
			},
		},
		{
			name: "dc1,!dc2=>dc1,dc2",
			mappings: []DCMapping{
				{
					Source:       []string{"dc1"},
					Target:       []string{"dc1", "dc2"},
					IgnoreSource: []string{"dc2"},
				},
			},
			expectedTargetMap: map[string][]string{
				"dc1": {"dc1"},
				"dc2": {"dc1"},
			},
			expectedIgnoreSource: []string{"dc2"},
		},
		{
			name: "dc1,dc2=>dc1,!dc2",
			mappings: []DCMapping{
				{
					Source:       []string{"dc1", "dc2"},
					Target:       []string{"dc1"},
					IgnoreTarget: []string{"dc2"},
				},
			},
			expectedTargetMap: map[string][]string{
				"dc1": {"dc1", "dc2"},
			},
			expectedIgnoreTarget: []string{"dc2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			targetMap, ignoreSource, ignoreTarget := tc.mappings.calculateMappings()

			if !maps.EqualFunc(targetMap, tc.expectedTargetMap, slices.Equal) {
				t.Fatalf("Expected %v, but got %v", tc.expectedTargetMap, targetMap)
			}

			if !slices.Equal(ignoreSource, tc.expectedIgnoreSource) {
				t.Fatalf("Expected %v, but got %v\n", tc.expectedIgnoreSource, ignoreSource)
			}

			if !slices.Equal(ignoreTarget, tc.expectedIgnoreTarget) {
				t.Fatalf("Expected %v, but got %v\n", tc.expectedIgnoreTarget, ignoreTarget)
			}
		})
	}
}
