// Copyright (C) 2025 ScyllaDB

package featuregate_test

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/featuregate"
)

func TestRepairSmallTableOptimization(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		scyllaVer string
		expected  bool
	}{
		{
			scyllaVer: "2024.1.4",
			expected:  false,
		},
		{
			scyllaVer: "2024.2.5",
			expected:  true,
		},
		{
			scyllaVer: "2024.2.6",
			expected:  true,
		},
		{
			scyllaVer: "5.4.9",
			expected:  false,
		},
		{
			scyllaVer: "5.5.0",
			expected:  false,
		},
		{
			scyllaVer: "6.0.0",
			expected:  true,
		},
		{
			scyllaVer: "6.0.1",
			expected:  true,
		},
		{
			scyllaVer: "6.1.0",
			expected:  true,
		},
	}

	fg := featuregate.ScyllaFeatureGate{}
	for _, tc := range testCases {
		result, err := fg.RepairSmallTableOptimization(tc.scyllaVer)
		if err != nil {
			t.Fatal(err)
		}
		if result != tc.expected {
			t.Fatalf("expected {%v}, but got {%v}, version = {%s}", tc.expected, result, tc.scyllaVer)
		}
	}
}

func TestSafeDescribeMethodReadBarrierAPI(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		scyllaVersion   string
		expectedSupport bool
		expectedError   bool
	}{
		{
			name:            "when scylla >= 2025.1, then it is expected to support read barrier api",
			scyllaVersion:   "2025.1.0-candidate-20241106103631",
			expectedSupport: true,
			expectedError:   false,
		},
		{
			name:            "when scylla >= 6.1, then it is expected to support read barrier api",
			scyllaVersion:   "6.2.1-candidate-20241106103631",
			expectedSupport: true,
			expectedError:   false,
		},
		{
			name:            "when scylla >= 2024.2, then it is expected to support read barrier cql",
			scyllaVersion:   "2024.2",
			expectedSupport: false,
			expectedError:   false,
		},
		{
			name:            "when scylla >= 6.0, then it is expected to support read barrier cql",
			scyllaVersion:   "6.0.1",
			expectedSupport: false,
			expectedError:   false,
		},
		{
			name:            "when scylla < 6.0, then it is expected to not support any safe method",
			scyllaVersion:   "5.9.9",
			expectedSupport: false,
			expectedError:   false,
		},
		{
			name:            "when scylla < 2024.2, then it is expected to not support any safe method",
			scyllaVersion:   "2024.1",
			expectedSupport: false,
			expectedError:   false,
		},
		{
			name:            "when scylla version is not a semver, then it is expected to return an error",
			scyllaVersion:   "main",
			expectedSupport: false,
			expectedError:   true,
		},
	}

	fg := featuregate.ScyllaFeatureGate{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			support, err := fg.SafeDescribeMethodReadBarrierAPI(tc.scyllaVersion)
			if tc.expectedError != (err != nil) {
				t.Fatalf("Expected error to be %v, got %v", tc.expectedError, err != nil)
			}
			if tc.expectedSupport != support {
				t.Fatalf("Expected support to be %v, got %v", tc.expectedSupport, support)
			}
		})
	}
}

func TestSafeDescribeMethodReadBarrierCQL(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		scyllaVersion   string
		expectedSupport bool
		expectedError   bool
	}{
		{
			name:            "when scylla >= 2025.1, then it is expected to support read barrier api",
			scyllaVersion:   "2025.1.0-candidate-20241106103631",
			expectedSupport: true,
			expectedError:   false,
		},
		{
			name:            "when scylla >= 6.1, then it is expected to support read barrier api",
			scyllaVersion:   "6.2.1-candidate-20241106103631",
			expectedSupport: true,
			expectedError:   false,
		},
		{
			name:            "when scylla >= 2024.2, then it is expected to support read barrier cql",
			scyllaVersion:   "2024.2",
			expectedSupport: true,
			expectedError:   false,
		},
		{
			name:            "when scylla >= 6.0, then it is expected to support read barrier cql",
			scyllaVersion:   "6.0.1",
			expectedSupport: true,
			expectedError:   false,
		},
		{
			name:            "when scylla < 6.0, then it is expected to not support any safe method",
			scyllaVersion:   "5.9.9",
			expectedSupport: false,
			expectedError:   false,
		},
		{
			name:            "when scylla < 2024.2, then it is expected to not support any safe method",
			scyllaVersion:   "2024.1",
			expectedSupport: false,
			expectedError:   false,
		},
		{
			name:            "when scylla version is not a semver, then it is expected to return an error",
			scyllaVersion:   "main",
			expectedSupport: false,
			expectedError:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fg := featuregate.ScyllaFeatureGate{}
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					support, err := fg.SafeDescribeMethodReadBarrierCQL(tc.scyllaVersion)
					if tc.expectedError != (err != nil) {
						t.Fatalf("Expected error to be %v, got %v", tc.expectedError, err != nil)
					}
					if tc.expectedSupport != support {
						t.Fatalf("Expected support to be %v, got %v", tc.expectedSupport, support)
					}
				})
			}
		})
	}
}

func TestSkipCleanupAndSkipReshape(t *testing.T) {
	testCases := []struct {
		name          string
		scyllaVersion string
		expected      bool
	}{
		{
			name:          "2025.3.0 is supported",
			scyllaVersion: "2025.3.0",
			expected:      true,
		},
		{
			name:          "2025.2.1 is supported",
			scyllaVersion: "2025.2.1",
			expected:      true,
		},
		{
			name:          "2025.2.0 is supported",
			scyllaVersion: "2025.2.0",
			expected:      true,
		},
		{
			name:          "2025.1.0 is not supported",
			scyllaVersion: "2025.1.0",
			expected:      false,
		},
	}

	fg := featuregate.ScyllaFeatureGate{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			supported, err := fg.SkipCleanupAndSkipReshape(tc.scyllaVersion)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if supported != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, supported)
			}
		})
	}
}
