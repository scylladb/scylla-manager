// Copyright (C) 2023 ScyllaDB

package repair

import (
	"fmt"
	"testing"
)

func TestInternalTablePreferenceKLess(t *testing.T) {
	testCases := []struct {
		ks1      string
		ks2      string
		expected bool
	}{
		// The same keyspace
		{ks1: "system_distributed", ks2: "system_distributed", expected: false},
		{ks1: "ks1", ks2: "ks1", expected: false},
		// User vs user
		{ks1: "ks1", ks2: "ks2", expected: false},
		{ks1: "ks2", ks2: "ks1", expected: false},
		// System vs system
		{ks1: "system_auth", ks2: "system_distributed", expected: true},
		{ks1: "system_auth", ks2: "system_distributed_everywhere", expected: true},
		{ks1: "system_auth", ks2: "system_traces", expected: true},
		{ks1: "system_distributed", ks2: "system_distributed_everywhere", expected: true},
		{ks1: "system_distributed_everywhere", ks2: "system_traces", expected: true},
		// System vs user
		{ks1: "system_auth", ks2: "ks1", expected: true},
		{ks1: "system_traces", ks2: "ks1", expected: true},
		// Unknown system keyspace
		{ks1: "system_auth", ks2: "system_unknown", expected: true},
		{ks1: "system_distributed", ks2: "system_unknown", expected: true},
		{ks1: "system_unknown", ks2: "ks1", expected: true},
		{ks1: "system_unknown1", ks2: "system_unknown2", expected: false},
	}

	tp := NewInternalTablePreference()

	for i := range testCases {
		tc := testCases[i]

		t.Run(fmt.Sprintf("%s vs %s", tc.ks1, tc.ks2), func(t *testing.T) {
			t.Parallel()

			if got := tp.KSLess(tc.ks1, tc.ks2); got != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}

			if tc.expected {
				if tp.KSLess(tc.ks2, tc.ks1) {
					t.Fatal("symmetric relation")
				}
			}
		})
	}
}

func TestInternalTablePreferenceTLess(t *testing.T) {
	testCases := []struct {
		ks       string
		t1       string
		t2       string
		expected bool
	}{
		// The same table
		{ks: "system_distributed", t1: "view_build_status", t2: "view_build_status", expected: false},
		{ks: "ks", t1: "t1", t2: "t1", expected: false},
		// User vs user
		{ks: "ks", t1: "t1", t2: "t2", expected: false},
		{ks: "ks", t1: "t2", t2: "t1", expected: false},
		// System auth tables
		{ks: "system_auth", t1: "role_attributes", t2: "role_members", expected: true},
		{ks: "system_not_auth", t1: "role_attributes", t2: "role_members", expected: false},
		{ks: "system_auth", t1: "role_attributes", t2: "roles", expected: true},
		{ks: "system_auth", t1: "role_members", t2: "roles", expected: true},
		{ks: "system_auth", t1: "role_members", t2: "t1", expected: true},
	}

	tp := NewInternalTablePreference()

	for i := range testCases {
		tc := testCases[i]

		t.Run(fmt.Sprintf("%s: %s vs %s", tc.ks, tc.t1, tc.t2), func(t *testing.T) {
			t.Parallel()

			if got := tp.TLess(tc.ks, tc.t1, tc.t2); got != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}

			if tc.expected {
				if tp.TLess(tc.ks, tc.t2, tc.t1) {
					t.Fatal("symmetric relation")
				}
			}
		})
	}
}
