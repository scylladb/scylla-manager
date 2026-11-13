// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"
	"sort"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

// hostEncryptionKeys returns the distinct encryption keys of the SSTables
// the host backs up in this snapshot, read from Scylla's sstable_info.
// The keys are informative: when they can't be collected, for example on
// a Scylla version without sstable_info, the manifest is written without
// them and the backup continues.
func (w *worker) hostEncryptionKeys(ctx context.Context, h hostInfo, dirs []snapshotDir) []backupspec.EncryptionKey {
	tables := make(map[string]map[string]struct{})
	for i := range dirs {
		d := &dirs[i]
		if tables[d.Keyspace] == nil {
			tables[d.Keyspace] = make(map[string]struct{})
		}
		tables[d.Keyspace][d.Table] = struct{}{}
	}

	var infos []scyllaclient.TableSSTables
	for ks := range tables {
		ksInfos, err := w.Client.SSTableInfo(ctx, h.IP, ks, "")
		if err != nil {
			w.Logger.Info(ctx, "Couldn't read SSTable encryption keys, manifest won't list them",
				"host", h.IP,
				"keyspace", ks,
				"error", err,
			)
			return nil
		}
		infos = append(infos, ksInfos...)
	}
	return encryptionKeys(infos, tables)
}

// encryptionKeys returns the distinct keys (provider, host) of the SSTables of the given
// tables (keyspace -> table set), sorted for a stable manifest.
func encryptionKeys(infos []scyllaclient.TableSSTables, tables map[string]map[string]struct{}) []backupspec.EncryptionKey {
	set := make(map[backupspec.EncryptionKey]struct{})
	for _, t := range infos {
		if _, ok := tables[t.Keyspace][t.Table]; !ok {
			continue
		}
		for _, s := range t.SSTables {
			opts := s.PropertyGroup(scyllaclient.SSTableEncryptionInfoGroup)
			if opts == nil {
				continue
			}
			set[encryptionKeyFromOptions(opts)] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}

	out := make([]backupspec.EncryptionKey, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		return encryptionKeySortKey(out[i]) < encryptionKeySortKey(out[j])
	})
	return out
}

// encryptionKeyHostOptions are the scylla_encryption_options naming the key's
// host, in the order they are looked up.
var encryptionKeyHostOptions = []string{"kms_host", "gcp_host", "azure_host", "kmip_host"}

// encryptionKeyFromOptions maps scylla_encryption_options to a manifest key:
// the provider and the first named host it reports. Data key IDs, key
// material paths and cipher options are dropped.
func encryptionKeyFromOptions(opts map[string]string) backupspec.EncryptionKey {
	k := backupspec.EncryptionKey{Provider: opts["key_provider"]}
	for _, o := range encryptionKeyHostOptions {
		if h := opts[o]; h != "" {
			k.Host = h
			break
		}
	}
	return k
}

func encryptionKeySortKey(k backupspec.EncryptionKey) string {
	return k.Provider + "\x00" + k.Host
}
