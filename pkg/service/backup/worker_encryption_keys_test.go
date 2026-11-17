// Copyright (C) 2026 ScyllaDB

package backup

import (
	"testing"

	"github.com/scylladb/scylla-manager/backupspec"
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
