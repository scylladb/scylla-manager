// Copyright (C) 2026 ScyllaDB

package scyllaclient_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
)

func TestClientSSTableInfo(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		file       string
		table      string
		wantQuery  map[string]string
		wantErr    bool
		wantGroups []map[string]string
	}{
		{
			name:      "keyspace only",
			file:      "testdata/scylla_api/storage_service_sstable_info.json",
			wantQuery: map[string]string{"keyspace": "ks", "cf": ""},
			wantGroups: []map[string]string{
				{
					"cipher_algorithm":    "AES/CBC/PKCS5Padding",
					"components":          "Data",
					"key_id":              "8cc2b2e4b9a2e1f3a3c55d8f3b1e7f0a",
					"key_provider":        "KmsKeyProviderFactory",
					"kms_host":            "key-jqUXFjnQukM30ja7",
					"secret_key_strength": "128",
				},
				nil,
			},
		},
		{
			name:      "keyspace and table",
			file:      "testdata/scylla_api/storage_service_sstable_info.json",
			table:     "t1",
			wantQuery: map[string]string{"keyspace": "ks", "cf": "t1"},
			wantGroups: []map[string]string{
				{
					"cipher_algorithm":    "AES/CBC/PKCS5Padding",
					"components":          "Data",
					"key_id":              "8cc2b2e4b9a2e1f3a3c55d8f3b1e7f0a",
					"key_provider":        "KmsKeyProviderFactory",
					"kms_host":            "key-jqUXFjnQukM30ja7",
					"secret_key_strength": "128",
				},
				nil,
			},
		},
		{
			// Scylla versions without the endpoint.
			name:      "not found",
			file:      "testdata/scylla_api/storage_service_sstable_info.404.json",
			wantQuery: map[string]string{"keyspace": "ks", "cf": ""},
			wantErr:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, closeServer := scyllaclienttest.NewFakeScyllaServerRequestChecker(t, tc.file, func(t *testing.T, r *http.Request) {
				if r.URL.Path != "/storage_service/sstable_info" {
					t.Errorf("path = %s", r.URL.Path)
				}
				for k, v := range tc.wantQuery {
					if got := r.URL.Query().Get(k); got != v {
						t.Errorf("query %s = %q, want %q", k, got, v)
					}
				}
			})
			defer closeServer()

			tables, err := client.SSTableInfo(context.Background(), scyllaclienttest.TestHost, "ks", tc.table)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "Not found") {
					t.Fatalf("expected not found error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(tables) != 1 || tables[0].Keyspace != "ks" || tables[0].Table != "t1" {
				t.Fatalf("unexpected tables %+v", tables)
			}
			var groups []map[string]string
			for _, s := range tables[0].SSTables {
				groups = append(groups, s.PropertyGroup(scyllaclient.SSTableEncryptionInfoGroup))
			}
			if diff := cmp.Diff(tc.wantGroups, groups); diff != "" {
				t.Fatalf("encryption info mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
