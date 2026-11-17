// Copyright (C) 2026 ScyllaDB

package backup

import (
	"cmp"
	"context"
	"maps"
	"slices"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

const (
	encryptionInfoGroup = "encryption_info"
)

// encryptionKeyHostAttrs are the attributes naming the key, each used by a different key provider.
var encryptionKeyHostAttrs = []string{"kms_host", "gcp_host", "azure_host", "kmip_host"}

func (w *worker) setEncryptionKeys(h hostInfo, encryptionKeys map[backupspec.EncryptionKey]struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.encryptionKeysByHost == nil {
		w.encryptionKeysByHost = make(map[string]map[backupspec.EncryptionKey]struct{})
	}

	w.encryptionKeysByHost[h.ID] = encryptionKeys
}

func (w *worker) hostEncryptionKeys(h hostInfo) map[backupspec.EncryptionKey]struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.encryptionKeysByHost[h.ID]
}

// sortedEncryptionKeys returns the keys sorted, so that the manifest doesn't depend on map iteration order.
func sortedEncryptionKeys(keys map[backupspec.EncryptionKey]struct{}) []backupspec.EncryptionKey {
	return slices.SortedFunc(maps.Keys(keys), func(a, b backupspec.EncryptionKey) int {
		return cmp.Or(cmp.Compare(a.Provider, b.Provider), cmp.Compare(a.Host, b.Host))
	})
}

// collectSSTablesEncryptionKeys returns the keys of the unit's encrypted SSTables reported by sstable_info.
func (w *worker) collectSSTablesEncryptionKeys(ctx context.Context, h hostInfo, u Unit) (map[backupspec.EncryptionKey]struct{}, error) {
	tables, err := w.Client.SSTableInfo(scyllaclient.Interactive(ctx), h.IP, u.Keyspace)
	if err != nil {
		return nil, err
	}
	ret := make(map[backupspec.EncryptionKey]struct{})
	for t, sstables := range tables {
		if !u.AllTables && !slices.Contains(u.Tables, t) {
			continue
		}
		for _, s := range sstables {
			if k, ok := sstableEncryptionKey(s); ok {
				ret[k] = struct{}{}
			}
		}
	}
	return ret, nil
}

// sstableEncryptionKey returns the key used for encrypting the SSTable as reported by Scylla,
// or false if it's not encrypted.
func sstableEncryptionKey(s *models.Sstable) (backupspec.EncryptionKey, bool) {
	attrs := scyllaclient.SSTableExtendedProperties(s, encryptionInfoGroup)
	if attrs == nil {
		return backupspec.EncryptionKey{}, false
	}
	encryptionKey := backupspec.EncryptionKey{
		Provider: attrs["key_provider"],
	}
	for _, a := range encryptionKeyHostAttrs {
		if h, ok := attrs[a]; ok {
			encryptionKey.Host = h
			break
		}
	}
	return encryptionKey, true
}
