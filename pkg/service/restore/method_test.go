// Copyright (C) 2026 ScyllaDB

package restore

import (
	"slices"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func TestMethodListParse(t *testing.T) {
	testCases := []struct {
		name        string
		method      MethodList
		expected    []Method
		expectedErr string
	}{
		{
			name:     "empty resolves to default method",
			method:   "",
			expected: []Method{MethodRclone},
		},
		{
			name:     "auto resolves to all supported methods",
			method:   MethodAuto,
			expected: []Method{MethodTablet, MethodNative, MethodRclone},
		},
		{
			name:     "single rclone",
			method:   "rclone",
			expected: []Method{MethodRclone},
		},
		{
			name:     "single native",
			method:   "native",
			expected: []Method{MethodNative},
		},
		{
			name:     "single tablet",
			method:   "tablet",
			expected: []Method{MethodTablet},
		},
		{
			name:     "list is sorted by preference",
			method:   "rclone,native",
			expected: []Method{MethodNative, MethodRclone},
		},
		{
			name:     "full list is sorted by preference",
			method:   "rclone,tablet,native",
			expected: []Method{MethodTablet, MethodNative, MethodRclone},
		},
		{
			name:     "list with whitespace",
			method:   " native , rclone ",
			expected: []Method{MethodNative, MethodRclone},
		},
		{
			name:        "unknown method",
			method:      "foo",
			expectedErr: "unknown method",
		},
		{
			name:        "unknown method in list",
			method:      "native,foo",
			expectedErr: "unknown method",
		},
		{
			name:        "empty method in list",
			method:      "native,,rclone",
			expectedErr: "unknown method",
		},
		{
			name:        "trailing comma",
			method:      "native,",
			expectedErr: "unknown method",
		},
		{
			name:        "duplicated method",
			method:      "native,rclone,native",
			expectedErr: "specified multiple times",
		},
		{
			name:        "auto combined with other method",
			method:      "auto,native",
			expectedErr: "can't be combined",
		},
		{
			name:        "auto combined with other method as last",
			method:      "native,auto",
			expectedErr: "can't be combined",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			methods, err := tc.method.parse()
			if tc.expectedErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.expectedErr) {
					t.Fatalf("Expected error containing %q, got %v", tc.expectedErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !slices.Equal(methods, tc.expected) {
				t.Fatalf("Expected methods %v, got %v", tc.expected, methods)
			}
		})
	}
}

func TestMethodListQueries(t *testing.T) {
	testCases := []struct {
		method         MethodList
		allowsTablet   bool
		allowsNative   bool
		allowsRclone   bool
		explicitTablet bool
		explicitNative bool
	}{
		{
			method:       "",
			allowsRclone: true,
		},
		{
			method:       MethodAuto,
			allowsTablet: true,
			allowsNative: true,
			allowsRclone: true,
		},
		{
			method:       "rclone",
			allowsRclone: true,
		},
		{
			method:         "native",
			allowsNative:   true,
			explicitNative: true,
		},
		{
			method:         "native,rclone",
			allowsNative:   true,
			allowsRclone:   true,
			explicitNative: true,
		},
		{
			method:         "tablet",
			allowsTablet:   true,
			explicitTablet: true,
		},
		{
			method:         "tablet,native",
			allowsTablet:   true,
			allowsNative:   true,
			explicitTablet: true,
			explicitNative: true,
		},
	}

	for _, tc := range testCases {
		t.Run(string(tc.method), func(t *testing.T) {
			if got := tc.method.allows(MethodTablet); got != tc.allowsTablet {
				t.Errorf("allows(tablet): expected %v, got %v", tc.allowsTablet, got)
			}
			if got := tc.method.allows(MethodNative); got != tc.allowsNative {
				t.Errorf("allows(native): expected %v, got %v", tc.allowsNative, got)
			}
			if got := tc.method.allows(MethodRclone); got != tc.allowsRclone {
				t.Errorf("allows(rclone): expected %v, got %v", tc.allowsRclone, got)
			}
			if got := tc.method.explicit(MethodTablet); got != tc.explicitTablet {
				t.Errorf("explicit(tablet): expected %v, got %v", tc.explicitTablet, got)
			}
			if got := tc.method.explicit(MethodNative); got != tc.explicitNative {
				t.Errorf("explicit(native): expected %v, got %v", tc.explicitNative, got)
			}
		})
	}
}

func TestValidatePropertiesMethod(t *testing.T) {
	testCases := []struct {
		name          string
		method        MethodList
		restoreSchema bool
		expectedErr   string
	}{
		{
			name:   "restore tables with default method",
			method: "",
		},
		{
			name:   "restore tables with method list",
			method: "native,rclone",
		},
		{
			name:        "restore tables with unknown method",
			method:      "foo",
			expectedErr: "unknown method",
		},
		{
			name:          "restore schema with default method",
			method:        "",
			restoreSchema: true,
		},
		{
			name:          "restore schema with explicit default method",
			method:        "rclone",
			restoreSchema: true,
		},
		{
			name:          "restore schema with non-default method",
			method:        "native",
			restoreSchema: true,
			expectedErr:   "restore schema does not support '--method' flag",
		},
		{
			name:          "restore schema with method list",
			method:        "rclone,native",
			restoreSchema: true,
			expectedErr:   "restore schema does not support '--method' flag",
		},
		{
			name:          "restore schema with unknown method",
			method:        "foo",
			restoreSchema: true,
			expectedErr:   "unknown method",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			target := defaultTarget()
			target.Location = []backupspec.Location{{Provider: backupspec.S3, Path: "bucket"}}
			target.SnapshotTag = backupspec.SnapshotTagAt(timeutc.Now())
			target.Method = tc.method
			target.RestoreSchema = tc.restoreSchema
			target.RestoreTables = !tc.restoreSchema

			err := target.validateProperties()
			if tc.expectedErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.expectedErr) {
					t.Fatalf("Expected error containing %q, got %v", tc.expectedErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}
