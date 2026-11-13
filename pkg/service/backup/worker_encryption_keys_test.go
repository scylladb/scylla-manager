// Copyright (C) 2026 ScyllaDB

package backup

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

func TestEncryptionKeys(t *testing.T) {
	t.Parallel()

	sstable := func(opts map[string]string) scyllaclient.SSTableInfo {
		s := scyllaclient.SSTableInfo{
			ExtendedProperties: []scyllaclient.SSTableNamedProperties{{
				Group:      "compression_parameters",
				Attributes: []scyllaclient.SSTableProperty{{Key: "sstable_compression", Value: "LZ4Compressor"}},
			}},
		}
		if opts == nil {
			return s
		}
		g := scyllaclient.SSTableNamedProperties{Group: scyllaclient.SSTableEncryptionInfoGroup}
		for k, v := range opts {
			g.Attributes = append(g.Attributes, scyllaclient.SSTableProperty{Key: k, Value: v})
		}
		s.ExtendedProperties = append(s.ExtendedProperties, g)
		return s
	}
	// Shapes as on lab: AWS cluster 9 and GCP cluster 8, Scylla 2026.3.5.
	kmsA := map[string]string{
		"components": "Data", "enabled": "true", "key_id": "61726e3a01",
		"key_provider": "KmsKeyProviderFactory", "kms_host": "key-a",
	}
	// Same key and data key in another SSTable, with other components.
	kmsASameDataKey := map[string]string{
		"components": "Data, Index", "enabled": "true", "key_id": "61726e3a01",
		"key_provider": "KmsKeyProviderFactory", "kms_host": "key-a",
	}
	// Same key, a newer data key after Scylla's key cache expiry.
	kmsANewDataKey := map[string]string{
		"components": "Data", "enabled": "true", "key_id": "61726e3a02",
		"key_provider": "KmsKeyProviderFactory", "kms_host": "key-a",
	}
	gcpB := map[string]string{"key_provider": "GcpKeyProviderFactory", "gcp_host": "key-b", "key_id": "0a24"}

	wantKmsA := backupspec.EncryptionKey{Provider: "KmsKeyProviderFactory", KmsHost: "key-a", KeyID: "61726e3a01"}
	wantKmsANew := backupspec.EncryptionKey{Provider: "KmsKeyProviderFactory", KmsHost: "key-a", KeyID: "61726e3a02"}
	wantGcpB := backupspec.EncryptionKey{Provider: "GcpKeyProviderFactory", KmsHost: "key-b", KeyID: "0a24"}

	backedUp := map[string]map[string]struct{}{"ks": {"t1": {}, "t2": {}}}

	testCases := []struct {
		name   string
		infos  []scyllaclient.TableSSTables
		tables map[string]map[string]struct{}
		want   []backupspec.EncryptionKey
	}{
		{
			name:   "unencrypted",
			infos:  []scyllaclient.TableSSTables{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{sstable(nil)}}},
			tables: backedUp,
			want:   nil,
		},
		{
			name: "one data key across SSTables and tables",
			infos: []scyllaclient.TableSSTables{
				{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{sstable(kmsA), sstable(kmsASameDataKey)}},
				{Keyspace: "ks", Table: "t2", SSTables: []scyllaclient.SSTableInfo{sstable(kmsA)}},
			},
			tables: backedUp,
			want:   []backupspec.EncryptionKey{wantKmsA},
		},
		{
			name: "two data keys of one key",
			infos: []scyllaclient.TableSSTables{
				{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{sstable(kmsANewDataKey), sstable(kmsA)}},
			},
			tables: backedUp,
			want:   []backupspec.EncryptionKey{wantKmsA, wantKmsANew},
		},
		{
			// SSTables written before a key change keep the old key until
			// compaction rewrites them. The GCP host is stored as kms_host.
			name: "two keys, sorted",
			infos: []scyllaclient.TableSSTables{
				{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{sstable(kmsA), sstable(gcpB), sstable(nil)}},
			},
			tables: backedUp,
			want:   []backupspec.EncryptionKey{wantGcpB, wantKmsA},
		},
		{
			name: "tables not in the snapshot are ignored",
			infos: []scyllaclient.TableSSTables{
				{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{sstable(kmsA)}},
				{Keyspace: "ks", Table: "skipped", SSTables: []scyllaclient.SSTableInfo{sstable(gcpB)}},
				{Keyspace: "other", Table: "t1", SSTables: []scyllaclient.SSTableInfo{sstable(gcpB)}},
			},
			tables: backedUp,
			want:   []backupspec.EncryptionKey{wantKmsA},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := encryptionKeys(tc.infos, tc.tables)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("encryptionKeys() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
