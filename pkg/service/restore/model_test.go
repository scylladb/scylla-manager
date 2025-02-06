// Copyright (C) 2025 ScyllaDB
package restore

import (
	"maps"
	"testing"
)

func TestCalculateMappings(t *testing.T) {
	testCases := []struct {
		name string

		mappings          DCMappings
		expectedSourceMap map[string]string
		expectedTargetMap map[string]string
	}{
		{
			name: "dc1=>dc2",
			mappings: []DCMapping{
				{
					Source: "dc1",
					Target: "dc2",
				},
			},
			expectedSourceMap: map[string]string{
				"dc1": "dc2",
			},
			expectedTargetMap: map[string]string{
				"dc2": "dc1",
			},
		},
		{
			name: "dc1=>dc2;dc3=>dc4",
			mappings: []DCMapping{
				{
					Source: "dc1",
					Target: "dc2",
				},
				{
					Source: "dc3",
					Target: "dc4",
				},
			},
			expectedSourceMap: map[string]string{
				"dc1": "dc2",
				"dc3": "dc4",
			},
			expectedTargetMap: map[string]string{
				"dc2": "dc1",
				"dc4": "dc3",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sourceDC2TargetDCMap, targetDC2SourceDCMap := tc.mappings.calculateMappings()

			if !maps.Equal(sourceDC2TargetDCMap, tc.expectedSourceMap) {
				t.Fatalf("Expected %v, but got %v", tc.expectedSourceMap, sourceDC2TargetDCMap)
			}

			if !maps.Equal(targetDC2SourceDCMap, tc.expectedTargetMap) {
				t.Fatalf("Expected %v, but got %v", tc.expectedTargetMap, targetDC2SourceDCMap)
			}
		})
	}
}
