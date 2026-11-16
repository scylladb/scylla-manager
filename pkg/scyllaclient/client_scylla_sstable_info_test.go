// Copyright (C) 2026 ScyllaDB

package scyllaclient_test

import (
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// TestClientSSTableInfo tests that SSTableInfo queries the given keyspace and returns its SSTables mapped by table.
func TestClientSSTableInfo(t *testing.T) {
	t.Parallel()

	check := func(t *testing.T, r *http.Request) {
		if v := r.FormValue("keyspace"); v != "ks" {
			t.Errorf("keyspace = %q, expected ks", v)
		}
	}
	client, closeServer := scyllaclienttest.NewFakeScyllaServerRequestChecker(t, "testdata/scylla_api/storage_service_sstable_info.json", check)
	defer closeServer()

	v, err := client.SSTableInfo(t.Context(), scyllaclienttest.TestHost, "ks")
	if err != nil {
		t.Fatal(err)
	}

	generations := make(map[string][]string)
	for table, sstables := range v {
		for _, s := range sstables {
			generations[table] = append(generations[table], s.Generation)
		}
	}
	expected := map[string][]string{
		"t1": {"3h5k_19y7_3hdw024exwhsck4uqe", "3h5k_154g_4q6jk24exwhsck4uqe"},
		"t2": {"3h5k_19y7_4abcd24exwhsck4uqe"},
	}
	if diff := cmp.Diff(generations, expected); diff != "" {
		t.Fatal(diff)
	}
}

// TestClientSSTableInfoNotFound tests that SSTableInfo fails when sstable_info is not found.
func TestClientSSTableInfoNotFound(t *testing.T) {
	t.Parallel()

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/storage_service_sstable_info.404.json")
	defer closeServer()

	if _, err := client.SSTableInfo(t.Context(), scyllaclienttest.TestHost, "ks"); err == nil {
		t.Fatal("SSTableInfo() expected error")
	}
}

// TestSSTableExtendedProperties tests that SSTableExtendedProperties returns the attributes of the given group,
// or nil when the SSTable doesn't report it.
func TestSSTableExtendedProperties(t *testing.T) {
	t.Parallel()

	compression := &models.NamedMaps{
		Group: "compression_parameters",
		Attributes: []*models.Mapper{
			{Key: "sstable_compression", Value: "org.apache.cassandra.io.compress.LZ4Compressor"},
		},
	}

	testCases := []struct {
		name     string
		sstable  *models.Sstable
		expected map[string]string
	}{
		{
			name: "group present",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{
				compression,
				{
					Group: "encryption_info",
					Attributes: []*models.Mapper{
						{Key: "key_provider", Value: "KmsKeyProviderFactory"},
						{Key: "kms_host", Value: "key-kms-test"},
					},
				},
			}},
			expected: map[string]string{
				"key_provider": "KmsKeyProviderFactory",
				"kms_host":     "key-kms-test",
			},
		},
		{
			name:    "group absent",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{compression}},
		},
		{
			name:    "no groups",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{}},
		},
		{
			name:    "no extended properties",
			sstable: &models.Sstable{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v := scyllaclient.SSTableExtendedProperties(tc.sstable, "encryption_info")
			if diff := cmp.Diff(v, tc.expected); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
