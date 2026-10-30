// Copyright (C) 2026 ScyllaDB

package backup

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

func TestEncryptionKeysFromSSTableInfo(t *testing.T) {
	t.Parallel()

	compression := scyllaclient.SSTableNamedAttrs{
		Group:      "compression_parameters",
		Attributes: []scyllaclient.SSTableAttribute{{Key: "sstable_compression", Value: "LZ4Compressor"}},
	}
	encryption := func(host, keyID string) scyllaclient.SSTableNamedAttrs {
		return scyllaclient.SSTableNamedAttrs{
			Group: "encryption_info",
			Attributes: []scyllaclient.SSTableAttribute{
				{Key: "cipher_algorithm", Value: "AES/CBC/PKCS5Padding"},
				{Key: "components", Value: "Data, Index"},
				{Key: "key_id", Value: keyID},
				{Key: "key_provider", Value: "KmsKeyProviderFactory"},
				{Key: "kms_host", Value: host},
				{Key: "secret_key_strength", Value: "128"},
			},
		}
	}
	key := func(host string) backupspec.EncryptionKey {
		return backupspec.EncryptionKey{
			Provider:          "KmsKeyProviderFactory",
			KmsHost:           host,
			CipherAlgorithm:   "AES/CBC/PKCS5Padding",
			SecretKeyStrength: "128",
		}
	}
	sstables := func(groups ...[]scyllaclient.SSTableNamedAttrs) []scyllaclient.TableSSTables {
		t := scyllaclient.TableSSTables{Keyspace: "ks", Table: "tb"}
		for _, g := range groups {
			t.SSTables = append(t.SSTables, scyllaclient.SSTableInfo{ExtendedProperties: g})
		}
		return []scyllaclient.TableSSTables{t}
	}

	testCases := []struct {
		name string
		in   []scyllaclient.TableSSTables
		want []backupspec.EncryptionKey
	}{
		{
			name: "unencrypted sstables contribute nothing",
			in:   sstables([]scyllaclient.SSTableNamedAttrs{compression}, nil),
			want: nil,
		},
		{
			// Data key IDs differ per SSTable, so they must not split one key into many.
			name: "same key with different data key IDs is listed once",
			in: sstables(
				[]scyllaclient.SSTableNamedAttrs{compression, encryption("key-a", "01")},
				[]scyllaclient.SSTableNamedAttrs{encryption("key-a", "02")},
			),
			want: []backupspec.EncryptionKey{key("key-a")},
		},
		{
			name: "SSTables written before and after a key change list both keys sorted",
			in: sstables(
				[]scyllaclient.SSTableNamedAttrs{encryption("key-b", "01")},
				[]scyllaclient.SSTableNamedAttrs{compression},
				[]scyllaclient.SSTableNamedAttrs{encryption("key-a", "02")},
			),
			want: []backupspec.EncryptionKey{key("key-a"), key("key-b")},
		},
		{
			name: "GCP host is kept in its own field",
			in: sstables([]scyllaclient.SSTableNamedAttrs{{
				Group: "encryption_info",
				Attributes: []scyllaclient.SSTableAttribute{
					{Key: "key_provider", Value: "GcpKeyProviderFactory"},
					{Key: "gcp_host", Value: "key-g"},
				},
			}}),
			want: []backupspec.EncryptionKey{{Provider: "GcpKeyProviderFactory", GcpHost: "key-g"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := encryptionKeysFromSSTableInfo(tc.in)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("encryptionKeysFromSSTableInfo() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
