// Copyright (C) 2025 ScyllaDB
package restore

import (
	"testing"

	gocmp "github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

func TestValidateDCMappings(t *testing.T) {
	testCases := []struct {
		name       string
		sourceDC   []string
		targetDC   []string
		dcMappings map[string]string

		expectedErr bool
	}{
		{
			name:     "sourceDC != targetDC, but with full mapping",
			sourceDC: []string{"dc1"},
			targetDC: []string{"dc2"},
			dcMappings: map[string]string{
				"dc1": "dc2",
			},
			expectedErr: false,
		},
		{
			name:     "source != target, but with full mapping, two dcs per cluster",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc3", "dc4"},
			dcMappings: map[string]string{
				"dc1": "dc3",
				"dc2": "dc4",
			},
			expectedErr: false,
		},
		{
			name:     "DC mappings has unknown source dc",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc3", "dc4"},
			dcMappings: map[string]string{
				"dc1": "dc3",
				"dc0": "dc4",
			},
			expectedErr: true,
		},
		{
			name:     "DC mappings has unknown target dc",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc3", "dc4"},
			dcMappings: map[string]string{
				"dc1": "dc3",
				"dc2": "dc5",
			},
			expectedErr: true,
		},
		{
			name:     "Squeezing DCs is not supported",
			sourceDC: []string{"dc1", "dc2"},
			targetDC: []string{"dc1"},
			dcMappings: map[string]string{
				"dc1": "dc1",
				"dc2": "dc1",
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

func TestHostsByDC(t *testing.T) {
	testCases := []struct {
		name                 string
		nodes                scyllaclient.NodeStatusInfoSlice
		targetDC2SourceDCMap map[string]string
		locationDCs          []string

		expected map[string][]string
	}{
		{
			name: "When --dc-mapping is provided with all DCs",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Addr: "n1", Datacenter: "target_dc1"},
				{Addr: "n2", Datacenter: "target_dc1"},
				{Addr: "n3", Datacenter: "target_dc2"},
				{Addr: "n4", Datacenter: "target_dc2"},
			},
			targetDC2SourceDCMap: map[string]string{
				"target_dc1": "source_dc1",
				"target_dc2": "source_dc2",
			},
			locationDCs: []string{"source_dc1", "source_dc2"},

			expected: map[string][]string{
				"source_dc1": {"n1", "n2"},
				"source_dc2": {"n3", "n4"},
			},
		},
		{
			name: "When --dc-mapping is provided with some DCs",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Addr: "n1", Datacenter: "target_dc1"},
				{Addr: "n2", Datacenter: "target_dc1"},
				{Addr: "n3", Datacenter: "target_dc2"},
				{Addr: "n4", Datacenter: "target_dc2"},
			},
			targetDC2SourceDCMap: map[string]string{
				"target_dc1": "source_dc1",
			},
			locationDCs: []string{"source_dc1", "source_dc2"},

			expected: map[string][]string{
				"source_dc1": {"n1", "n2"},
			},
		},
		{
			name: "When --dc-mapping is empty and location contains one DC",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Addr: "n1", Datacenter: "target_dc1"},
				{Addr: "n2", Datacenter: "target_dc1"},
				{Addr: "n3", Datacenter: "target_dc2"},
				{Addr: "n4", Datacenter: "target_dc2"},
			},
			targetDC2SourceDCMap: map[string]string{},
			locationDCs:          []string{"source_dc1"},

			expected: map[string][]string{
				"source_dc1": {"n1", "n2", "n3", "n4"},
			},
		},
		{
			name: "When --dc-mapping is empty and location contains multiple DCs",
			nodes: scyllaclient.NodeStatusInfoSlice{
				{Addr: "n1", Datacenter: "target_dc1"},
				{Addr: "n2", Datacenter: "target_dc1"},
				{Addr: "n3", Datacenter: "target_dc2"},
				{Addr: "n4", Datacenter: "target_dc2"},
			},
			targetDC2SourceDCMap: map[string]string{},
			locationDCs:          []string{"source_dc1", "source_dc2"},

			expected: map[string][]string{
				"source_dc1": {"n1", "n2", "n3", "n4"},
				"source_dc2": {"n1", "n2", "n3", "n4"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := hostsByDC(tc.nodes, tc.targetDC2SourceDCMap, tc.locationDCs)
			if diff := gocmp.Diff(actual, tc.expected); diff != "" {
				t.Fatalf("Unexpected result: %v", diff)
			}
		})
	}
}

func TestValidateProperties(t *testing.T) {
	validLoc, err := backupspec.NewLocation("s3:my-bucket")
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name        string
		target      Target
		expectedErr bool
	}{
		{
			name: "restore tables without keyspace mapping passes validation",
			target: Target{
				Location:      []backupspec.Location{validLoc},
				SnapshotTag:   "sm_20240101000000UTC",
				RestoreTables: true,
				Transfers:     0,
				Method:        MethodRclone,
			},
			expectedErr: false,
		},
		{
			name: "restore tables with nil keyspace mapping passes validation",
			target: Target{
				Location:         []backupspec.Location{validLoc},
				SnapshotTag:      "sm_20240101000000UTC",
				RestoreTables:    true,
				Transfers:        0,
				Method:           MethodRclone,
				KeyspaceMappings: nil,
			},
			expectedErr: false,
		},
		{
			name: "restore tables with empty keyspace mapping passes validation",
			target: Target{
				Location:         []backupspec.Location{validLoc},
				SnapshotTag:      "sm_20240101000000UTC",
				RestoreTables:    true,
				Transfers:        0,
				Method:           MethodRclone,
				KeyspaceMappings: map[string]string{},
			},
			expectedErr: false,
		},
		{
			name: "restore schema without keyspace mapping passes validation",
			target: Target{
				Location:    []backupspec.Location{validLoc},
				SnapshotTag: "sm_20240101000000UTC",
				RestoreSchema: true,
				Transfers:   0,
				Method:      MethodRclone,
			},
			expectedErr: false,
		},
		{
			name: "restore schema with keyspace mapping is forbidden",
			target: Target{
				Location:    []backupspec.Location{validLoc},
				SnapshotTag: "sm_20240101000000UTC",
				RestoreSchema: true,
				Transfers:   0,
				Method:      MethodRclone,
				KeyspaceMappings: map[string]string{
					"ks_old": "ks_new",
				},
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.target.validateProperties()
			if tc.expectedErr && err == nil {
				t.Fatalf("Expected err, but got nil")
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected err: %v", err)
			}
		})
	}
}

func TestValidateKeyspaceMappings(t *testing.T) {
	testCases := []struct {
		name             string
		targetKeyspaces  []string
		ksMappings       map[string]string
		expectedErr      bool
	}{
		{
			name:            "valid single mapping",
			targetKeyspaces: []string{"ks_new"},
			ksMappings: map[string]string{
				"ks_old": "ks_new",
			},
			expectedErr: false,
		},
		{
			name:            "valid multiple mappings",
			targetKeyspaces: []string{"ks_new1", "ks_new2"},
			ksMappings: map[string]string{
				"ks_old1": "ks_new1",
				"ks_old2": "ks_new2",
			},
			expectedErr: false,
		},
		{
			name:            "target keyspace does not exist in cluster",
			targetKeyspaces: []string{"ks_other"},
			ksMappings: map[string]string{
				"ks_old": "ks_new",
			},
			expectedErr: true,
		},
		{
			name:            "duplicate target keyspace in mappings",
			targetKeyspaces: []string{"ks_new"},
			ksMappings: map[string]string{
				"ks_old1": "ks_new",
				"ks_old2": "ks_new",
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateKeyspaceMappings(tc.ksMappings, tc.targetKeyspaces)
			if tc.expectedErr && err == nil {
				t.Fatalf("Expected err, but got nil")
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("Unexpected err: %v", err)
			}
		})
	}
}

func TestTargetKeyspace(t *testing.T) {
	testCases := []struct {
		name             string
		ksMappings       map[string]string
		sourceKs         string
		expectedTargetKs string
	}{
		{
			name: "mapped keyspace",
			ksMappings: map[string]string{
				"ks_old": "ks_new",
			},
			sourceKs:         "ks_old",
			expectedTargetKs: "ks_new",
		},
		{
			name: "unmapped keyspace returns source",
			ksMappings: map[string]string{
				"ks_old": "ks_new",
			},
			sourceKs:         "ks_other",
			expectedTargetKs: "ks_other",
		},
		{
			name:             "no mappings returns source",
			ksMappings:       nil,
			sourceKs:         "ks1",
			expectedTargetKs: "ks1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			target := Target{KeyspaceMappings: tc.ksMappings}
			actual := target.TargetKeyspace(tc.sourceKs)
			if actual != tc.expectedTargetKs {
				t.Fatalf("Expected %q, got %q", tc.expectedTargetKs, actual)
			}
		})
	}
}
