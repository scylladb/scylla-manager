// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
)

// The manifest's encryption_keys must describe the snapshot's SSTables, not the
// live table's. sstable_info reports the live SSTables, which compaction keeps
// replacing, so the keys are read around taking the snapshot:
//   - the snapshot stage calls sstable_info right before and right after
//     taking the snapshot of each keyspace and records the key of every
//     SSTable from both answers (sstableKeys),
//   - the index stage checks that every SSTable of the snapshot is in the
//     record and keeps exactly the keys of the snapshot's SSTables,
//   - the manifest stage writes them to the node's manifest.
//
// An SSTable of the snapshot can be in neither answer: taking the snapshot
// flushes memtables, and compaction can rewrite the flushed SSTable before
// the second call (seen on system_schema tables right after schema changes).
// Its key can't be read, so the manifest gets the keys of all the table's
// SSTables from both answers: the SSTable was written between the calls with
// the key the table used then, which is among them even when the key changed
// between the calls. An extra key is harmless for a restore, a missing one is
// not. Only when the table has no SSTable in either answer the index stage
// fails. Snapshot and index stages are not resumable, so the run's snapshots
// are deleted and the next attempt takes a new snapshot.
//
// The keys are informative when sstable_info is not available, for example
// on Scylla versions without it, or a call fails: the manifest is written
// without them and the backup continues.
//
// A resumed run never rewrites the manifest: resumable stages come after the
// manifest stage, so the uploaded temporary manifest already holds the keys.

// noEncryptionKey marks an unencrypted SSTable in sstableKeys.
const noEncryptionKey = -1

// sstableKeys records the encryption key of every SSTable reported by
// sstable_info on a host.
type sstableKeys struct {
	// keys are the distinct keys of recorded SSTables.
	keys []backupspec.EncryptionKey
	// tables maps keyspace -> table -> recorded SSTables of the table.
	tables map[string]map[string]*tableSSTableKeys
}

// tableSSTableKeys records the SSTables of a table.
type tableSSTableKeys struct {
	// gens maps SSTable generation -> index in keys, or noEncryptionKey
	// for an unencrypted SSTable.
	gens map[string]int
}

func newSSTableKeys() *sstableKeys {
	return &sstableKeys{tables: make(map[string]map[string]*tableSSTableKeys)}
}

// add records the SSTables of the given sstable_info answer. Tables not
// accepted by filter are skipped.
func (k *sstableKeys) add(infos []scyllaclient.TableSSTables, filter func(keyspace, table string) bool) {
	for _, t := range infos {
		if !filter(t.Keyspace, t.Table) {
			continue
		}
		ks := k.tables[t.Keyspace]
		if ks == nil {
			ks = make(map[string]*tableSSTableKeys)
			k.tables[t.Keyspace] = ks
		}
		tk := ks[t.Table]
		if tk == nil {
			tk = &tableSSTableKeys{gens: make(map[string]int, len(t.SSTables))}
			ks[t.Table] = tk
		}
		for _, s := range t.SSTables {
			idx := noEncryptionKey
			if opts := s.PropertyGroup(scyllaclient.SSTableEncryptionInfoGroup); opts != nil {
				idx = k.keyIndex(encryptionKeyFromOptions(opts))
			}
			tk.gens[string(s.Generation)] = idx
		}
	}
}

func (k *sstableKeys) keyIndex(key backupspec.EncryptionKey) int {
	for i := range k.keys {
		if k.keys[i] == key {
			return i
		}
	}
	k.keys = append(k.keys, key)
	return len(k.keys) - 1
}

// maxReportedMissingSSTables limits the SSTables named in the mismatch error.
const maxReportedMissingSSTables = 5

// snapshotKeys returns the distinct keys of the snapshot's SSTables listed
// in dirs, sorted for a stable manifest, and the snapshot's SSTables in
// neither answer, for which all keys of their table's SSTables were taken.
// It returns an error when such an SSTable's table has no recorded SSTables.
func (k *sstableKeys) snapshotKeys(dirs []snapshotDir) (keys []backupspec.EncryptionKey, inferred []string, err error) {
	used := make([]bool, len(k.keys))
	var (
		missing      []string
		missingCount int
	)
	for i := range dirs {
		d := &dirs[i]
		if d.Progress == nil {
			continue
		}
		tk := k.tables[d.Keyspace][d.Table]
		var gens map[string]int
		if tk != nil {
			gens = tk.gens
		}
		seen := make(map[string]struct{})
		for _, f := range d.Progress.files {
			id, err := sstable.ExtractID(f.Name)
			if err != nil {
				// Not an SSTable component.
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}

			idx, ok := gens[id]
			if !ok && len(gens) > 0 {
				for _, idx := range gens {
					if idx != noEncryptionKey {
						used[idx] = true
					}
				}
				inferred = append(inferred, fmt.Sprintf("%s.%s %s", d.Keyspace, d.Table, id))
				continue
			}
			if !ok {
				missingCount++
				if len(missing) < maxReportedMissingSSTables {
					missing = append(missing, fmt.Sprintf("%s.%s %s", d.Keyspace, d.Table, f.Name))
				}
				continue
			}
			if idx != noEncryptionKey {
				used[idx] = true
			}
		}
	}
	if missingCount > 0 {
		return nil, nil, errors.Errorf("can't determine the encryption keys of the snapshot: "+
			"%d SSTable(s) of the snapshot are in neither sstable_info answer taken right before and after the snapshot, "+
			"and their table has no SSTables in either answer (%s); the snapshot is deleted, retry the backup to take a new one",
			missingCount, strings.Join(missing, ", "))
	}

	var out []backupspec.EncryptionKey
	for i, ok := range used {
		if ok {
			out = append(out, k.keys[i])
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Provider != out[j].Provider {
			return out[i].Provider < out[j].Provider
		}
		return out[i].Host < out[j].Host
	})
	return out, inferred, nil
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

// sstableKeysRecorder collects a host's sstable_info answers in the snapshot
// stage. After the first failed call it stops calling sstable_info and
// records nothing, so the host's manifest gets no keys.
type sstableKeysRecorder struct {
	w   *worker
	h   hostInfo
	rec *sstableKeys
}

func (w *worker) newSSTableKeysRecorder(h hostInfo) *sstableKeysRecorder {
	return &sstableKeysRecorder{w: w, h: h, rec: newSSTableKeys()}
}

// record calls sstable_info for the unit's keyspace and records the SSTables
// of the unit's tables.
func (r *sstableKeysRecorder) record(ctx context.Context, u Unit, afterSnapshot bool) {
	when := "before snapshot"
	if afterSnapshot {
		when = "after snapshot"
	}
	if r.rec == nil {
		return
	}
	infos, err := r.w.Client.SSTableInfo(ctx, r.h.IP, u.Keyspace, "")
	if err != nil {
		r.w.Logger.Info(ctx, "Couldn't read SSTable encryption keys, manifest won't list them",
			"host", r.h.IP,
			"keyspace", u.Keyspace,
			"when", when,
			"error", err,
		)
		r.rec = nil
		return
	}
	tables := make(map[string]struct{}, len(u.Tables))
	for _, t := range u.Tables {
		tables[t] = struct{}{}
	}
	r.rec.add(infos, func(keyspace, table string) bool {
		if keyspace != u.Keyspace {
			return false
		}
		if u.AllTables || len(tables) == 0 {
			return true
		}
		_, ok := tables[table]
		return ok
	})
}

// done stores the recorded answers for the index stage.
func (r *sstableKeysRecorder) done() {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	if r.w.sstableKeys == nil {
		r.w.sstableKeys = make(map[string]*sstableKeys)
	}
	r.w.sstableKeys[r.h.IP] = r.rec
}

// indexEncryptionKeys sets the host's manifest keys to the keys of the
// snapshot's SSTables in dirs. It fails when an SSTable of the snapshot is
// in neither sstable_info answer. When the answers are not available, for
// example sstable_info failed or the run was resumed after the snapshot
// stage, the host gets no keys.
func (w *worker) indexEncryptionKeys(ctx context.Context, h hostInfo, dirs []snapshotDir) error {
	w.mu.Lock()
	rec := w.sstableKeys[h.IP]
	// The answers are not needed after indexing.
	delete(w.sstableKeys, h.IP)
	w.mu.Unlock()

	if rec == nil {
		return nil
	}
	keys, inferred, err := rec.snapshotKeys(dirs)
	if err != nil {
		return err
	}
	if len(inferred) > 0 {
		w.Logger.Info(ctx, "SSTables of the snapshot rewritten before the second sstable_info call, "+
			"the keys of all their table's SSTables from both answers are taken",
			"host", h.IP,
			"count", len(inferred),
			"sstables", inferred[:min(len(inferred), maxReportedMissingSSTables)],
		)
	}
	w.Logger.Info(ctx, "SSTable encryption keys of the snapshot", "host", h.IP, "keys", keys)

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.encryptionKeys == nil {
		w.encryptionKeys = make(map[string][]backupspec.EncryptionKey)
	}
	w.encryptionKeys[h.IP] = keys
	return nil
}

// hostEncryptionKeys returns the keys of the host's snapshot for its manifest.
func (w *worker) hostEncryptionKeys(h hostInfo) []backupspec.EncryptionKey {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.encryptionKeys[h.IP]
}
