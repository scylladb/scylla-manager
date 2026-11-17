// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
)

// Shapes as on lab: AWS and GCP clusters, Scylla 2026.3.5.
var (
	testKmsA = map[string]string{
		"components": "Data", "enabled": "true", "key_id": "61726e3a01",
		"key_provider": "KmsKeyProviderFactory", "kms_host": "key-a",
	}
	// Same key, another data key.
	testKmsA2 = map[string]string{
		"components": "Data, Index", "enabled": "true", "key_id": "61726e3a02",
		"key_provider": "KmsKeyProviderFactory", "kms_host": "key-a",
	}
	testKmsB = map[string]string{
		"key_provider": "KmsKeyProviderFactory", "kms_host": "key-b", "key_id": "61726e3a03",
	}
	testGcpC = map[string]string{"key_provider": "GcpKeyProviderFactory", "gcp_host": "key-c", "key_id": "0a24"}
	// A provider without a named host.
	testLocal = map[string]string{
		"key_provider": "LocalFileSystemKeyProviderFactory", "secret_key_file": "/etc/scylla/keys/k",
		"cipher_algorithm": "AES/CBC/PKCS5Padding", "secret_key_strength": "128",
	}

	wantKmsA  = backupspec.EncryptionKey{Provider: "KmsKeyProviderFactory", Host: "key-a"}
	wantKmsB  = backupspec.EncryptionKey{Provider: "KmsKeyProviderFactory", Host: "key-b"}
	wantGcpC  = backupspec.EncryptionKey{Provider: "GcpKeyProviderFactory", Host: "key-c"}
	wantLocal = backupspec.EncryptionKey{Provider: "LocalFileSystemKeyProviderFactory"}
)

const (
	gen1 = "3gp9_0yso_2rq402d3ylyf4gqbyb"
	gen2 = "3gp9_0yso_2rq402d3ylyf4gqbyc"
	gen3 = "3gp9_0yso_2rq402d3ylyf4gqbyd"
	gen4 = "3gp9_0yso_2rq402d3ylyf4gqbye"
)

func testSSTable(gen string, opts map[string]string) scyllaclient.SSTableInfo {
	s := scyllaclient.SSTableInfo{
		Generation: scyllaclient.SSTableGeneration(gen),
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

// testSnapshotDir returns a snapshot dir with all components of the SSTables
// with the given generations, plus Scylla's manifest and schema files.
func testSnapshotDir(keyspace, table string, gens ...string) snapshotDir {
	files := []fileInfo{{Name: "manifest.json"}, {Name: "schema.cql"}}
	for _, g := range gens {
		for _, c := range []string{"Data.db", "Index.db", "TOC.txt", "Scylla.db"} {
			files = append(files, fileInfo{Name: "me-" + g + "-big-" + c})
		}
	}
	return snapshotDir{Keyspace: keyspace, Table: table, Progress: &RunProgress{files: files}}
}

func allTables(string, string) bool { return true }

func TestSSTableKeysSnapshotKeys(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		answers [][]scyllaclient.TableSSTables
		dirs    []snapshotDir
		want    []backupspec.EncryptionKey
		// wantInferred are the SSTables in neither answer, which take all their table's keys.
		wantInferred []string
		wantErr      string
	}{
		{
			name: "unencrypted",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, nil)}}},
			},
			dirs: []snapshotDir{testSnapshotDir("ks", "t1", gen1)},
			want: nil,
		},
		{
			name: "one key across SSTables and tables",
			answers: [][]scyllaclient.TableSSTables{{
				{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA), testSSTable(gen2, testKmsA2)}},
				{Keyspace: "ks", Table: "t2", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen3, testKmsA)}},
			}},
			dirs: []snapshotDir{testSnapshotDir("ks", "t1", gen1, gen2), testSnapshotDir("ks", "t2", gen3)},
			want: []backupspec.EncryptionKey{wantKmsA},
		},
		{
			// SSTables written before a key change keep the old key until compaction rewrites them.
			name: "several keys, sorted",
			answers: [][]scyllaclient.TableSSTables{{
				{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{
					testSSTable(gen1, testKmsB), testSSTable(gen2, testGcpC), testSSTable(gen3, testLocal), testSSTable(gen4, testKmsA),
				}},
			}},
			dirs: []snapshotDir{testSnapshotDir("ks", "t1", gen1, gen2, gen3, gen4)},
			want: []backupspec.EncryptionKey{wantGcpC, wantKmsA, wantKmsB, wantLocal},
		},
		{
			// The before answer still lists the SSTable with the old key,
			// compaction rewrote it with the new key after the snapshot.
			name: "SSTable compacted after the snapshot keeps its key",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA)}}},
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen2, testKmsB)}}},
			},
			dirs: []snapshotDir{testSnapshotDir("ks", "t1", gen1)},
			want: []backupspec.EncryptionKey{wantKmsA},
		},
		{
			name: "SSTable flushed by the snapshot is in the after answer",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA)}}},
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA), testSSTable(gen2, testKmsB)}}},
			},
			dirs: []snapshotDir{testSnapshotDir("ks", "t1", gen1, gen2)},
			want: []backupspec.EncryptionKey{wantKmsA, wantKmsB},
		},
		{
			name: "keys of live SSTables not in the snapshot are not written",
			answers: [][]scyllaclient.TableSSTables{
				{
					{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA), testSSTable(gen2, testKmsB)}},
					{Keyspace: "ks", Table: "skipped", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen3, testGcpC)}},
				},
			},
			dirs: []snapshotDir{testSnapshotDir("ks", "t1", gen1)},
			want: []backupspec.EncryptionKey{wantKmsA},
		},
		{
			name: "integer generations",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable("7", testKmsA)}}},
			},
			dirs: []snapshotDir{testSnapshotDir("ks", "t1", "7")},
			want: []backupspec.EncryptionKey{wantKmsA},
		},
		{
			// gen2 was flushed by the snapshot and compacted into gen3
			// before the second call; both written with the table's key B.
			name: "SSTable in neither answer takes all keys of its table",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA)}}},
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA), testSSTable(gen3, testKmsB)}}},
			},
			dirs:         []snapshotDir{testSnapshotDir("ks", "t1", gen2)},
			want:         []backupspec.EncryptionKey{wantKmsA, wantKmsB},
			wantInferred: []string{"ks.t1 " + gen2},
		},
		{
			// The table's key changes from A to B between the calls: gen2 may
			// have either key, so both are listed. gen1 (A) is compacted away
			// before the second call.
			name: "key change between the calls lists both keys",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA)}}},
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen3, testKmsB)}}},
			},
			dirs:         []snapshotDir{testSnapshotDir("ks", "t1", gen2, gen3)},
			want:         []backupspec.EncryptionKey{wantKmsA, wantKmsB},
			wantInferred: []string{"ks.t1 " + gen2},
		},
		{
			// Keys of the table are taken from both answers, deduplicated;
			// other tables' keys are not.
			name: "SSTable in neither answer takes only its own table's keys",
			answers: [][]scyllaclient.TableSSTables{
				{
					{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA), testSSTable(gen4, testKmsA2)}},
					{Keyspace: "ks", Table: "t2", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen3, testGcpC)}},
				},
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA)}}},
			},
			dirs:         []snapshotDir{testSnapshotDir("ks", "t1", gen2)},
			want:         []backupspec.EncryptionKey{wantKmsA},
			wantInferred: []string{"ks.t1 " + gen2},
		},
		{
			name: "SSTable in neither answer in an unencrypted table",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "system_schema", Table: "columns", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, nil)}}},
				{{Keyspace: "system_schema", Table: "columns", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen3, nil)}}},
			},
			dirs:         []snapshotDir{testSnapshotDir("system_schema", "columns", gen1, gen2)},
			want:         nil,
			wantInferred: []string{"system_schema.columns " + gen2},
		},
		{
			name: "SSTable in neither answer of a table without SSTables fails",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: nil}},
				{{Keyspace: "ks", Table: "t1", SSTables: nil}},
			},
			dirs:    []snapshotDir{testSnapshotDir("ks", "t1", gen2)},
			wantErr: "1 SSTable(s) of the snapshot are in neither sstable_info answer",
		},
		{
			name: "table missing from the answers fails",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA)}}},
			},
			dirs:    []snapshotDir{testSnapshotDir("ks", "t1", gen1), testSnapshotDir("ks", "t2", gen2, gen3)},
			wantErr: "2 SSTable(s) of the snapshot",
		},
		{
			name:    "empty snapshot dir",
			answers: [][]scyllaclient.TableSSTables{{}},
			dirs:    []snapshotDir{testSnapshotDir("ks", "t1")},
			want:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := newSSTableKeys()
			for _, a := range tc.answers {
				rec.add(a, allTables)
			}
			got, inferred, err := rec.snapshotKeys(tc.dirs)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("snapshotKeys() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("snapshotKeys() mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantInferred, inferred); diff != "" {
				t.Fatalf("snapshotKeys() inferred mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// fakeSSTableInfoServer serves storage_service/sstable_info answers in order,
// one per call, and accepts snapshots.
type fakeSSTableInfoServer struct {
	mu        sync.Mutex
	answers   [][]scyllaclient.TableSSTables
	status    int
	calls     []string
	snapshots int
}

func (s *fakeSSTableInfoServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/storage_service/snapshots":
		s.snapshots++
		s.calls = append(s.calls, "snapshot")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	case "/storage_service/sstable_info":
		s.calls = append(s.calls, "sstable_info "+r.URL.Query().Get("keyspace"))
		if s.status != 0 {
			w.WriteHeader(s.status)
			_, _ = w.Write([]byte(`{"message": "Not found", "code": 404}`))
			return
		}
		var a []scyllaclient.TableSSTables
		if len(s.answers) > 0 {
			a, s.answers = s.answers[0], s.answers[1:]
		}
		_ = json.NewEncoder(w).Encode(a)
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message": "Not found", "code": 404}`))
	}
}

func TestSnapshotRecordsSSTableKeysForIndex(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		answers   [][]scyllaclient.TableSSTables
		status    int
		unit      Unit
		dirs      []snapshotDir
		wantCalls []string
		wantKeys  []backupspec.EncryptionKey
		wantErr   string
	}{
		{
			// SSTable gen1 (key A) is compacted into gen2 (key B) while
			// the snapshot is uploaded, the manifest still lists key A.
			name: "keys of the snapshot, not of the live table",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA)}}},
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA), testSSTable(gen3, testKmsB)}}},
			},
			unit:      Unit{Keyspace: "ks", AllTables: true},
			dirs:      []snapshotDir{testSnapshotDir("ks", "t1", gen1, gen3)},
			wantCalls: []string{"sstable_info ks", "snapshot", "sstable_info ks"},
			wantKeys:  []backupspec.EncryptionKey{wantKmsA, wantKmsB},
		},
		{
			name: "only the unit's tables are recorded",
			answers: [][]scyllaclient.TableSSTables{
				{
					{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA)}},
					{Keyspace: "ks", Table: "t2", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen2, testKmsB)}},
				},
			},
			unit:      Unit{Keyspace: "ks", Tables: []string{"t1"}},
			dirs:      []snapshotDir{testSnapshotDir("ks", "t1", gen1)},
			wantCalls: []string{"sstable_info ks", "snapshot", "sstable_info ks"},
			wantKeys:  []backupspec.EncryptionKey{wantKmsA},
		},
		{
			// Key change to B before the backup: gen2, flushed by the snapshot with key B,
			// is compacted into gen3 before the second call.
			name: "flushed SSTable compacted before the second call",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA)}}},
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA), testSSTable(gen3, testKmsB)}}},
			},
			unit:      Unit{Keyspace: "ks", AllTables: true},
			dirs:      []snapshotDir{testSnapshotDir("ks", "t1", gen1, gen2)},
			wantCalls: []string{"sstable_info ks", "snapshot", "sstable_info ks"},
			wantKeys:  []backupspec.EncryptionKey{wantKmsA, wantKmsB},
		},
		{
			name: "key change between the calls",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA)}}},
				{{Keyspace: "ks", Table: "t1", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen3, testKmsB)}}},
			},
			unit:      Unit{Keyspace: "ks", AllTables: true},
			dirs:      []snapshotDir{testSnapshotDir("ks", "t1", gen2)},
			wantCalls: []string{"sstable_info ks", "snapshot", "sstable_info ks"},
			wantKeys:  []backupspec.EncryptionKey{wantKmsA, wantKmsB},
		},
		{
			name: "mismatch fails the index stage",
			answers: [][]scyllaclient.TableSSTables{
				{{Keyspace: "ks", Table: "t1", SSTables: nil}},
				{{Keyspace: "ks", Table: "t2", SSTables: []scyllaclient.SSTableInfo{testSSTable(gen1, testKmsA)}}},
			},
			unit:      Unit{Keyspace: "ks", AllTables: true},
			dirs:      []snapshotDir{testSnapshotDir("ks", "t1", gen2)},
			wantCalls: []string{"sstable_info ks", "snapshot", "sstable_info ks"},
			wantErr:   "can't determine the encryption keys of the snapshot",
		},
		{
			// Scylla versions without sstable_info: no keys, the backup goes on.
			name:      "sstable_info not available",
			status:    http.StatusNotFound,
			unit:      Unit{Keyspace: "ks", AllTables: true},
			dirs:      []snapshotDir{testSnapshotDir("ks", "t1", gen1)},
			wantCalls: []string{"sstable_info ks", "snapshot"},
			wantKeys:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := &fakeSSTableInfoServer{answers: tc.answers, status: tc.status}
			client, closeServer := scyllaclienttest.NewFakeScyllaServerWithHandler(t, srv)
			defer closeServer()

			w := &worker{
				workerTools: workerTools{
					SnapshotTag: "sm_20261117000000UTC",
					Client:      client,
					Logger:      log.NewDevelopment(),
				},
				Metrics: metrics.NewBackupMetrics(),
				Units:   []Unit{tc.unit},
			}
			h := hostInfo{IP: scyllaclienttest.TestHost}
			ctx := context.Background()

			if err := w.takeSnapshot(ctx, h); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.wantCalls, srv.calls); diff != "" {
				t.Fatalf("calls mismatch (-want +got):\n%s", diff)
			}

			err := w.indexEncryptionKeys(ctx, h, tc.dirs)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("indexEncryptionKeys() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.wantKeys, w.hostEncryptionKeys(h)); diff != "" {
				t.Fatalf("keys mismatch (-want +got):\n%s", diff)
			}
			if _, ok := w.sstableKeys[h.IP]; ok {
				t.Fatal("sstable_info answers kept after indexing")
			}
		})
	}
}

// TestReindexWithoutSnapshotAnswers covers a resumed run: it re-indexes
// without taking a snapshot, which must neither fail nor drop the keys of
// the manifest already uploaded by the previous run.
func TestReindexWithoutSnapshotAnswers(t *testing.T) {
	t.Parallel()

	w := &worker{workerTools: workerTools{Logger: log.NewDevelopment()}}
	h := hostInfo{IP: "192.168.100.11"}
	if err := w.indexEncryptionKeys(context.Background(), h, []snapshotDir{testSnapshotDir("ks", "t1", gen1)}); err != nil {
		t.Fatal(err)
	}
	if keys := w.hostEncryptionKeys(h); keys != nil {
		t.Fatalf("keys = %v, want none", keys)
	}

	// The manifest, with the keys, is uploaded before any resumable stage,
	// so a resumed run never rewrites it.
	for _, s := range StageOrder() {
		if s.Resumable() && s.Index() <= StageManifest.Index() {
			t.Fatalf("stage %s is resumable and not after %s, a resumed run would rewrite the manifest without its keys", s, StageManifest)
		}
	}
}
