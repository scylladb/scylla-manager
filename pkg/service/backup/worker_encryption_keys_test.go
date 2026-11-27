// Copyright (C) 2026 ScyllaDB

package backup

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// TestSSTableEncryptionKey tests extracting the SSTable encryption key from sstable_info
// extended properties reported for different key providers. The provider is taken as reported,
// so it stays qualified with a Java package, or empty when left out.
func TestSSTableEncryptionKey(t *testing.T) {
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
		expected backupspec.EncryptionKey
		// Whether the SSTable is not encrypted.
		notEncrypted bool
	}{
		{
			name: "kms",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{
				compression,
				{
					Group: "encryption_info",
					Attributes: []*models.Mapper{
						{Key: "key_id", Value: "00"},
						{Key: "key_provider", Value: "KmsKeyProviderFactory"},
						{Key: "kms_host", Value: "key-kms-test"},
					},
				},
			}},
			expected: backupspec.EncryptionKey{Provider: "KmsKeyProviderFactory", Host: "key-kms-test"},
		},
		{
			name: "gcp",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{
				compression,
				{
					Group: "encryption_info",
					Attributes: []*models.Mapper{
						{Key: "gcp_host", Value: "key-gcp-test"},
						{Key: "key_id", Value: "00"},
						{Key: "key_provider", Value: "GcpKeyProviderFactory"},
					},
				},
			}},
			expected: backupspec.EncryptionKey{Provider: "GcpKeyProviderFactory", Host: "key-gcp-test"},
		},
		{
			name: "azure",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{
				compression,
				{
					Group: "encryption_info",
					Attributes: []*models.Mapper{
						{Key: "azure_host", Value: "key-azure-test"},
						{Key: "key_id", Value: "00"},
						{Key: "key_provider", Value: "AzureKeyProviderFactory"},
					},
				},
			}},
			expected: backupspec.EncryptionKey{Provider: "AzureKeyProviderFactory", Host: "key-azure-test"},
		},
		{
			name: "kmip",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{
				compression,
				{
					Group: "encryption_info",
					Attributes: []*models.Mapper{
						{Key: "key_id", Value: "00"},
						{Key: "key_provider", Value: "KmipKeyProviderFactory"},
						{Key: "kmip_host", Value: "key-kmip-test"},
					},
				},
			}},
			expected: backupspec.EncryptionKey{Provider: "KmipKeyProviderFactory", Host: "key-kmip-test"},
		},
		{
			name: "local file",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{
				compression,
				{
					Group: "encryption_info",
					Attributes: []*models.Mapper{
						{Key: "cipher_algorithm", Value: "AES/CBC/PKCS5Padding"},
						{Key: "key_provider", Value: "LocalFileSystemKeyProviderFactory"},
						{Key: "secret_key_strength", Value: "128"},
					},
				},
			}},
			expected: backupspec.EncryptionKey{Provider: "LocalFileSystemKeyProviderFactory"},
		},
		{
			name: "replicated",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{
				compression,
				{
					Group: "encryption_info",
					Attributes: []*models.Mapper{
						{Key: "cipher_algorithm", Value: "AES/CBC/PKCS5Padding"},
						{Key: "key_provider", Value: "ReplicatedKeyProviderFactory"},
						{Key: "secret_key_strength", Value: "128"},
					},
				},
			}},
			expected: backupspec.EncryptionKey{Provider: "ReplicatedKeyProviderFactory"},
		},
		{
			name: "kms qualified with a Java package",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{
				compression,
				{
					Group: "encryption_info",
					Attributes: []*models.Mapper{
						{Key: "key_provider", Value: "com.datastax.bdp.cassandra.crypto.KmsKeyProviderFactory"},
						{Key: "kms_host", Value: "key-kms-test"},
					},
				},
			}},
			expected: backupspec.EncryptionKey{Provider: "com.datastax.bdp.cassandra.crypto.KmsKeyProviderFactory", Host: "key-kms-test"},
		},
		{
			name: "azure qualified with a Java package",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{
				compression,
				{
					Group: "encryption_info",
					Attributes: []*models.Mapper{
						{Key: "azure_host", Value: "key-azure-test"},
						{Key: "key_provider", Value: "com.datastax.bdp.cassandra.crypto.AzureKeyProviderFactory"},
					},
				},
			}},
			expected: backupspec.EncryptionKey{Provider: "com.datastax.bdp.cassandra.crypto.AzureKeyProviderFactory", Host: "key-azure-test"},
		},
		{
			name: "host without provider",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{
				compression,
				{
					Group: "encryption_info",
					Attributes: []*models.Mapper{
						{Key: "cipher_algorithm", Value: "AES/CBC/PKCS5Padding"},
						{Key: "kms_host", Value: "key-kms-test"},
					},
				},
			}},
			expected: backupspec.EncryptionKey{Host: "key-kms-test"},
		},
		{
			name: "no provider and no host",
			sstable: &models.Sstable{ExtendedProperties: []*models.NamedMaps{
				compression,
				{
					Group: "encryption_info",
					Attributes: []*models.Mapper{
						{Key: "cipher_algorithm", Value: "AES/CBC/PKCS5Padding"},
						{Key: "secret_key_file", Value: "/etc/scylla/key"},
					},
				},
			}},
			// Encrypted SSTable is listed even when Scylla reports neither provider nor host
			expected: backupspec.EncryptionKey{},
		},
		{
			name:         "not encrypted",
			sstable:      &models.Sstable{ExtendedProperties: []*models.NamedMaps{compression}},
			notEncrypted: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v, ok := sstableEncryptionKey(tc.sstable)
			if v != tc.expected || ok == tc.notEncrypted {
				t.Fatalf("sstableEncryptionKey() = %#v, %v, expected %#v, %v", v, ok, tc.expected, !tc.notEncrypted)
			}
		})
	}
}

// TestTakeSnapshotEncryptionKeys tests that taking the snapshot keeps the union of the backed up tables' encryption keys
// reported by sstable_info before and after the snapshot of each keyspace, and no keys when any sstable_info call fails.
func TestTakeSnapshotEncryptionKeys(t *testing.T) {
	t.Parallel()

	var (
		keyA = backupspec.EncryptionKey{Provider: "KmsKeyProviderFactory", Host: "key-A"}
		keyB = backupspec.EncryptionKey{Provider: "KmsKeyProviderFactory", Host: "key-B"}
		keyC = backupspec.EncryptionKey{Provider: "KmsKeyProviderFactory", Host: "key-C"}
	)
	encryptedSSTable := func(k backupspec.EncryptionKey, keyID string) *models.Sstable {
		return &models.Sstable{ExtendedProperties: []*models.NamedMaps{{
			Group: encryptionInfoGroup,
			Attributes: []*models.Mapper{
				{Key: "key_id", Value: keyID},
				{Key: "key_provider", Value: k.Provider},
				{Key: "kms_host", Value: k.Host},
			},
		}}}
	}
	encryptedTable := func(table string, k backupspec.EncryptionKey) *models.TableSstables {
		return &models.TableSstables{Table: table, Sstables: []*models.Sstable{encryptedSSTable(k, "00")}}
	}
	unencryptedTable := func(table string) *models.TableSstables {
		return &models.TableSstables{Table: table, Sstables: []*models.Sstable{{}}}
	}

	// Tables reported by sstable_info mapped by keyspace.
	// sstable_info fails for keyspaces not in the map.
	type sstableInfo map[string][]*models.TableSstables

	testCases := []struct {
		name          string
		units         []Unit
		before, after sstableInfo
		expected      map[backupspec.EncryptionKey]struct{}
	}{
		{
			name:     "same key before and after snapshot",
			units:    []Unit{{Keyspace: "ks1", Tables: []string{"tab"}, AllTables: true}},
			before:   sstableInfo{"ks1": {encryptedTable("tab", keyA)}},
			after:    sstableInfo{"ks1": {encryptedTable("tab", keyA)}},
			expected: map[backupspec.EncryptionKey]struct{}{keyA: {}},
		},
		{
			name:     "SSTables rewritten with a new key after snapshot",
			units:    []Unit{{Keyspace: "ks1", Tables: []string{"tab"}, AllTables: true}},
			before:   sstableInfo{"ks1": {encryptedTable("tab", keyA)}},
			after:    sstableInfo{"ks1": {encryptedTable("tab", keyB)}},
			expected: map[backupspec.EncryptionKey]struct{}{keyA: {}, keyB: {}},
		},
		{
			name:     "SSTables flushed by snapshot",
			units:    []Unit{{Keyspace: "ks1", Tables: []string{"tab"}, AllTables: true}},
			before:   sstableInfo{"ks1": {{Table: "tab"}}},
			after:    sstableInfo{"ks1": {encryptedTable("tab", keyA)}},
			expected: map[backupspec.EncryptionKey]struct{}{keyA: {}},
		},
		{
			name:     "empty sstable_info response for one of keyspaces",
			units:    []Unit{{Keyspace: "ks1", Tables: []string{"tab"}, AllTables: true}, {Keyspace: "ks2", Tables: []string{"tab"}, AllTables: true}},
			before:   sstableInfo{"ks1": {}, "ks2": {encryptedTable("tab", keyB)}},
			after:    sstableInfo{"ks1": {}, "ks2": {encryptedTable("tab", keyB)}},
			expected: map[backupspec.EncryptionKey]struct{}{keyB: {}},
		},
		{
			name:   "unencrypted SSTables",
			units:  []Unit{{Keyspace: "ks1", Tables: []string{"tab"}, AllTables: true}},
			before: sstableInfo{"ks1": {unencryptedTable("tab")}},
			after:  sstableInfo{"ks1": {unencryptedTable("tab")}},
		},
		{
			name:     "table not included in backup",
			units:    []Unit{{Keyspace: "ks1", Tables: []string{"tab"}}},
			before:   sstableInfo{"ks1": {encryptedTable("tab", keyA), encryptedTable("other", keyC)}},
			after:    sstableInfo{"ks1": {encryptedTable("tab", keyA), encryptedTable("other", keyC)}},
			expected: map[backupspec.EncryptionKey]struct{}{keyA: {}},
		},
		{
			name:  "some tables included in backup",
			units: []Unit{{Keyspace: "ks1", Tables: []string{"tab", "tab2"}}},
			before: sstableInfo{"ks1": {
				encryptedTable("tab", keyA), encryptedTable("tab2", keyA), encryptedTable("other", keyC),
			}},
			after: sstableInfo{"ks1": {
				encryptedTable("tab", keyA), encryptedTable("tab2", keyB), encryptedTable("other", keyC),
			}},
			expected: map[backupspec.EncryptionKey]struct{}{keyA: {}, keyB: {}},
		},
		{
			name:     "all tables included in backup",
			units:    []Unit{{Keyspace: "ks1", Tables: []string{"tab", "other"}, AllTables: true}},
			before:   sstableInfo{"ks1": {encryptedTable("tab", keyA), encryptedTable("other", keyC)}},
			after:    sstableInfo{"ks1": {encryptedTable("tab", keyA), encryptedTable("other", keyC)}},
			expected: map[backupspec.EncryptionKey]struct{}{keyA: {}, keyC: {}},
		},
		{
			name:  "same key with different data key IDs",
			units: []Unit{{Keyspace: "ks1", Tables: []string{"tab"}, AllTables: true}},
			before: sstableInfo{"ks1": {{Table: "tab", Sstables: []*models.Sstable{
				encryptedSSTable(keyA, "01"), encryptedSSTable(keyA, "02"),
			}}}},
			after: sstableInfo{"ks1": {{Table: "tab", Sstables: []*models.Sstable{
				encryptedSSTable(keyA, "01"), encryptedSSTable(keyA, "02"),
			}}}},
			expected: map[backupspec.EncryptionKey]struct{}{keyA: {}},
		},
		{
			name:     "keyspaces with different keys",
			units:    []Unit{{Keyspace: "ks1", Tables: []string{"tab"}, AllTables: true}, {Keyspace: "ks2", Tables: []string{"tab"}, AllTables: true}},
			before:   sstableInfo{"ks1": {encryptedTable("tab", keyA)}, "ks2": {encryptedTable("tab", keyB)}},
			after:    sstableInfo{"ks1": {encryptedTable("tab", keyA)}, "ks2": {encryptedTable("tab", keyB)}},
			expected: map[backupspec.EncryptionKey]struct{}{keyA: {}, keyB: {}},
		},
		{
			name:  "sstable_info fails before snapshot",
			units: []Unit{{Keyspace: "ks1", Tables: []string{"tab"}, AllTables: true}},
			after: sstableInfo{"ks1": {encryptedTable("tab", keyA)}},
		},
		{
			name:   "sstable_info fails after snapshot",
			units:  []Unit{{Keyspace: "ks1", Tables: []string{"tab"}, AllTables: true}},
			before: sstableInfo{"ks1": {encryptedTable("tab", keyA)}},
		},
		{
			name:   "sstable_info fails for one of keyspaces",
			units:  []Unit{{Keyspace: "ks1", Tables: []string{"tab"}, AllTables: true}, {Keyspace: "ks2", Tables: []string{"tab"}, AllTables: true}},
			before: sstableInfo{"ks1": {encryptedTable("tab", keyA)}},
			after:  sstableInfo{"ks1": {encryptedTable("tab", keyA)}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Responses depend on whether the keyspace's snapshot was taken, not on the number of calls,
			// so that retries of a failed sstable_info call get the same response.
			var (
				mu          sync.Mutex
				snapshotted = make(map[string]bool)
			)
			client, closeServer := scyllaclienttest.NewFakeScyllaServerWithHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.URL.Path == "/storage_service/snapshots" {
					snapshotted[r.FormValue("kn")] = true
					return
				}
				ks := r.FormValue("keyspace")
				resp := tc.before
				if snapshotted[ks] {
					resp = tc.after
				}
				tables, ok := resp[ks]
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if err := json.NewEncoder(w).Encode(tables); err != nil {
					t.Error(err)
				}
			}))
			defer closeServer()

			w := &worker{
				workerTools: workerTools{Client: client, Logger: log.NewDevelopment()},
				Metrics:     metrics.NewBackupMetrics(),
				Units:       tc.units,
			}
			h := hostInfo{IP: scyllaclienttest.TestHost, ID: "host-id"}

			if err := w.takeSnapshot(t.Context(), h); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.expected, w.hostEncryptionKeys(h), cmpopts.EquateEmpty()); diff != "" {
				t.Fatalf("Encryption keys diff (-expected +got):\n%s", diff)
			}
		})
	}
}
