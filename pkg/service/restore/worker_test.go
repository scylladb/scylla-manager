// Copyright (C) 2025 ScyllaDB
package restore

import "testing"

func TestValidateDCMappings(t *testing.T) {
	testCases := []struct {
		name       string
		sourceDC   []string
		targetDC   []string
		dcMappings DCMappings

		expectedErr bool
	}{
		{
			name:     "sourceDC != targetDC, but with full mapping",
			sourceDC: []string{"dc1"},
			targetDC: []string{"dc2"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc2"},
			},
			expectedErr: false,
		},
		{
			name:     "source != target, but will full mapping, two dcs per cluster",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc3", "dc4"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc3"},
				{Source: "dc2", Target: "dc4"},
			},
			expectedErr: false,
		},
		{
			name:     "DC mappings has unknown source dc",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc3", "dc4"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc3"},
				{Source: "dc0", Target: "dc4"},
			},
			expectedErr: true,
		},
		{
			name:     "DC mappings has unknown target dc",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc3", "dc4"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc3"},
				{Source: "dc2", Target: "dc5"},
			},
			expectedErr: true,
		},
		{
			name:     "Squeezing DCs is not supported",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc1"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc1"},
				{Source: "dc2", Target: "dc1"},
			},
			expectedErr: true,
		},
		{
			name:     "Expanding DCs is not supported",
			sourceDC: []string{"dc1"},
			targetDC: []string{"dc1", "dc2"},
			dcMappings: []DCMapping{
				{Source: "dc1", Target: "dc1"},
				{Source: "dc1", Target: "dc2"},
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := &worker{}
			err := w.validateDCMappings(tc.dcMappings, tc.sourceDC, tc.targetDC)
			if tc.expectedErr && err == nil {
				t.Fatalf("Expected err, but got nil")
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected err: %v", err)
			}
		})
	}
}
