// Copyright (C) 2026 ScyllaDB

package backup

import (
	"cmp"
	"context"
	"slices"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

// sstableEncryptionGroup is the sstable_info extended properties group
// filled by Scylla encryption-at-rest extension for encrypted SSTables.
const sstableEncryptionGroup = "encryption_info"

// hostEncryptionKeys returns the distinct encryption keys of the SSTables
// of tables backed up on the host.
// It never fails the backup: on any error it logs it and returns nil,
// so that the manifest doesn't claim an incomplete set of keys.
func (w *worker) hostEncryptionKeys(ctx context.Context, h hostInfo) []backupspec.EncryptionKey {
	type table struct{ keyspace, name string }
	var (
		seen = make(map[table]struct{})
		keys []backupspec.EncryptionKey
	)
	for _, d := range w.hostSnapshotDirs(h) {
		t := table{keyspace: d.Keyspace, name: d.Table}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}

		info, err := w.Client.SSTableInfo(ctx, h.IP, t.keyspace, t.name)
		if err != nil {
			w.Logger.Error(ctx, "Couldn't read SSTable encryption info, manifest won't list encryption keys",
				"host", h.IP,
				"keyspace", t.keyspace,
				"table", t.name,
				"error", err,
			)
			return nil
		}
		keys = append(keys, encryptionKeysFromSSTableInfo(info)...)
	}
	return uniqueEncryptionKeys(keys)
}

// encryptionKeysFromSSTableInfo extracts encryption keys from sstable_info response.
// Unencrypted SSTables don't contribute any key.
func encryptionKeysFromSSTableInfo(tables []scyllaclient.TableSSTables) []backupspec.EncryptionKey {
	var out []backupspec.EncryptionKey
	for _, t := range tables {
		for _, sst := range t.SSTables {
			for _, g := range sst.ExtendedProperties {
				if g.Group != sstableEncryptionGroup {
					continue
				}
				var k backupspec.EncryptionKey
				for _, a := range g.Attributes {
					switch a.Key {
					case "key_provider":
						k.Provider = a.Value
					case "kms_host":
						k.KmsHost = a.Value
					case "gcp_host":
						k.GcpHost = a.Value
					case "azure_host":
						k.AzureHost = a.Value
					case "kmip_host":
						k.KmipHost = a.Value
					case "master_key":
						k.MasterKey = a.Value
					case "secret_key_file":
						k.SecretKeyFile = a.Value
					case "cipher_algorithm":
						k.CipherAlgorithm = a.Value
					case "secret_key_strength":
						k.SecretKeyStrength = a.Value
					}
				}
				if k != (backupspec.EncryptionKey{}) {
					out = append(out, k)
				}
			}
		}
	}
	return uniqueEncryptionKeys(out)
}

// uniqueEncryptionKeys returns sorted keys without duplicates.
func uniqueEncryptionKeys(keys []backupspec.EncryptionKey) []backupspec.EncryptionKey {
	slices.SortFunc(keys, func(a, b backupspec.EncryptionKey) int {
		return cmp.Or(
			cmp.Compare(a.Provider, b.Provider),
			cmp.Compare(a.KmsHost, b.KmsHost),
			cmp.Compare(a.GcpHost, b.GcpHost),
			cmp.Compare(a.AzureHost, b.AzureHost),
			cmp.Compare(a.KmipHost, b.KmipHost),
			cmp.Compare(a.MasterKey, b.MasterKey),
			cmp.Compare(a.SecretKeyFile, b.SecretKeyFile),
			cmp.Compare(a.CipherAlgorithm, b.CipherAlgorithm),
			cmp.Compare(a.SecretKeyStrength, b.SecretKeyStrength),
		)
	})
	return slices.Compact(keys)
}
