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
				{Source: []string{"dc1"}, Target: []string{"dc2"}},
			},
			expectedErr: false,
		},
		{
			name:     "source != target, but will full mapping, two dcs per cluster",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc3", "dc4"},
			dcMappings: []DCMapping{
				{Source: []string{"dc1", "dc2"}, Target: []string{"dc3", "dc4"}},
			},
			expectedErr: false,
		},
		{
			name:        "sourceDCs == targetDCs, no mapping",
			sourceDC:    []string{"dc1"},
			targetDC:    []string{"dc1"},
			expectedErr: false,
		},
		{
			name:        "sourceDCs == targetDCs, no mapping, two dcs per cluster",
			sourceDC:    []string{"dc1", "dc2"},
			targetDC:    []string{"dc1", "dc2"},
			expectedErr: false,
		},
		{
			name:        "sourceDCs != targetDCs, no mapping",
			sourceDC:    []string{"dc1"},
			targetDC:    []string{"dc2"},
			expectedErr: true,
		},
		{
			name:     "sourceDCs != targetDCs, but with full mapping",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc2"},
			dcMappings: []DCMapping{
				{Source: []string{"dc1", "dc2"}, Target: []string{"dc2"}},
			},
			expectedErr: false,
		},
		{
			name:     "sourceDCs != targetDCs, but with partial mapping",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc2"},
			dcMappings: []DCMapping{
				{Source: []string{"dc1"}, Target: []string{"dc2"}},
			},
			expectedErr: true,
		},
		{
			name:     "sourceDCs != targetDCs, with deletion in mapping",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc2"},
			dcMappings: []DCMapping{
				{
					Source:       []string{"dc1"},
					Target:       []string{"dc2"},
					IgnoreSource: []string{"dc2"},
				},
			},
			expectedErr: false,
		},
		{
			name:     "sourceDCs != targetDCs, with deletion in mapping",
			sourceDC: []string{"dc1"},
			targetDC: []string{"dc1", "dc2"},
			dcMappings: []DCMapping{
				{
					Source:       []string{"dc1"},
					Target:       []string{"dc1"},
					IgnoreTarget: []string{"dc2"},
				},
			},
			expectedErr: false,
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
