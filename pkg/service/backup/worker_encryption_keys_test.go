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
	kmsA := map[string]string{
		"key_provider": "KmsKeyProviderFactory", "kms_host": "key-a", "key_id": "id-1",
		"cipher_algorithm": "AES/CBC/PKCS5Padding", "secret_key_strength": "128", "components": "Data",
	}
	// Same key, another SSTable data key and component set.
	kmsASecondSSTable := map[string]string{
		"key_provider": "KmsKeyProviderFactory", "kms_host": "key-a", "key_id": "id-2",
		"cipher_algorithm": "AES/CBC/PKCS5Padding", "secret_key_strength": "128", "components": "Data, Index",
	}
	gcpB := map[string]string{"key_provider": "GcpKeyProviderFactory", "gcp_host": "key-b", "key_id": "id-3"}

	wantKmsA := backupspec.EncryptionKey{
		Provider: "KmsKeyProviderFactory", KmsHost: "key-a",
		CipherAlgorithm: "AES/CBC/PKCS5Padding", SecretKeyStrength: "128",
	}
	wantGcpB := backupspec.EncryptionKey{Provider: "GcpKeyProviderFactory", GcpHost: "key-b"}

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
			name: "one key across SSTables and tables",
			infos: []scyllaclient.TableSSTables{
				{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{sstable(kmsA), sstable(kmsASecondSSTable)}},
				{Keyspace: "ks", Table: "t2", SSTables: []scyllaclient.SSTableInfo{sstable(kmsA)}},
			},
			tables: backedUp,
			want:   []backupspec.EncryptionKey{wantKmsA},
		},
		{
			// SSTables written before a key change keep the old key until compaction rewrites them.
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
