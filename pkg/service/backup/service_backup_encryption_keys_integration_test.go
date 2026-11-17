// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package backup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/backupspec"

	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Encryption options of the local file key provider, which doesn't require any key service.
const (
	localKeyEncryption = "{'key_provider': 'LocalFileSystemKeyProviderFactory', " +
		"'secret_key_file': '/var/lib/scylla/sm_backup_test_key', 'cipher_algorithm': 'AES/CBC/PKCS5Padding', 'secret_key_strength': 128}"
	disabledEncryption = "{'key_provider': 'none'}"
)

var localKey = backupspec.EncryptionKey{Provider: "LocalFileSystemKeyProviderFactory"}

// TestBackupEncryptionKeysIntegration tests that node manifests list the encryption keys
// of the node's backed up SSTables, as reported by sstable_info.
func TestBackupEncryptionKeysIntegration(t *testing.T) {
	type table struct {
		keyspace string
		table    string
		// Replication factor in dc1.
		rf int
		// scylla_encryption_options, empty means not encrypted.
		encryption string
	}

	testCases := []struct {
		name        string
		description string
		tables      []table
		filter      []string
		// Number of dc1 node manifests listing the local key, the other manifests list no keys.
		hostsWithKey int
	}{
		{
			name:         "encrypted table replicated to all nodes",
			description:  "all manifests list the key",
			tables:       []table{{"ks1", "tab", 3, localKeyEncryption}},
			filter:       []string{"ks1"},
			hostsWithKey: 3,
		},
		{
			name:         "encrypted table replicated to a single node",
			description:  "only the manifest of the node storing the table lists the key",
			tables:       []table{{"ks1", "tab", 1, localKeyEncryption}},
			filter:       []string{"ks1"},
			hostsWithKey: 1,
		},
		{
			name:        "encrypted table excluded by filter",
			description: "manifests don't list the key of the table which is not backed up",
			tables:      []table{{"ks1", "tab", 3, localKeyEncryption}, {"ks1", "other", 3, ""}},
			filter:      []string{"ks1.other"},
		},
	}

	session := CreateScyllaManagerDBSession(t)
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				location       = testBackupLocation(fmt.Sprintf("backuptest-encryption-keys-%d", i))
				h              = newBackupTestHelper(t, session, defaultConfig(), location, nil)
				ctx            = t.Context()
				clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
			)

			Print("Given: tables with a single row")
			for _, tab := range tc.tables {
				createTable(t, clusterSession, tab.keyspace, tab.table, tab.rf, tab.encryption)
			}

			Print("And: backup target of dc1 with keyspace filter")
			props := defaultTestProperties(location, "")
			props["keyspace"] = tc.filter
			props["dc"] = []string{"dc1"}
			rawProps, err := json.Marshal(props)
			if err != nil {
				t.Fatal(err)
			}
			target, err := h.service.GetTarget(ctx, h.ClusterID, rawProps)
			if err != nil {
				t.Fatal(err)
			}

			Print("When: run backup")
			if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
				t.Fatal(err)
			}

			Printf("Then: %s", tc.description)
			h.assertManifestsEncryptionKeys(t, h.RunID, tc.hostsWithKey)
		})
	}
}

// TestBackupEncryptionKeysChangedDuringBackupIntegration tests that node manifests list the union of the encryption keys
// reported by sstable_info right before and right after taking the snapshot, when the table's encryption
// changes in between.
func TestBackupEncryptionKeysChangedDuringBackupIntegration(t *testing.T) {
	const testKeyspace = "backuptest_encryption_keys_changed"

	testCases := []struct {
		name        string
		description string
		// Table's scylla_encryption_options, empty means not encrypted.
		encryption string
		// Table's scylla_encryption_options set right before taking the snapshot on the first node,
		// after which each node's SSTables are rewritten right before taking its snapshot.
		newEncryption string
	}{
		{
			name:          "encryption enabled",
			description:   "manifests list the key reported only by sstable_info called after the snapshot",
			newEncryption: localKeyEncryption,
		},
		{
			name: "encryption disabled",
			description: "manifests list the key reported only by sstable_info called before the snapshot, " +
				"even though the snapshot SSTables are no longer encrypted with it",
			encryption:    localKeyEncryption,
			newEncryption: disabledEncryption,
		},
	}

	session := CreateScyllaManagerDBSession(t)
	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				location       = testBackupLocation(fmt.Sprintf("backuptest-encryption-keys-changed-%d", i))
				h              = newBackupTestHelper(t, session, defaultConfig(), location, nil)
				ctx, cancel    = context.WithCancel(t.Context())
				done           = make(chan struct{})
				clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
			)
			defer cancel()

			Print("Given: table with SSTables on disk")
			createTable(t, clusterSession, testKeyspace, BigTableName, 3, tc.encryption)
			FlushTable(t, h.Client, h.GetAllHosts(), testKeyspace, BigTableName)

			Print("And: snapshots blocked")
			type blockedSnapshot struct {
				host    string
				release chan struct{}
			}
			blocked := make(chan blockedSnapshot)
			h.Hrt.SetInterceptor(httpx.RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodPost || req.URL.Path != "/storage_service/snapshots" || req.URL.Query().Get("kn") != testKeyspace {
					return nil, nil
				}
				b := blockedSnapshot{host: req.URL.Hostname(), release: make(chan struct{})}
				select {
				case blocked <- b:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				select {
				case <-b.release:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}))

			Print("And: backup target of dc1 with keyspace filter")
			props := defaultTestProperties(location, "")
			props["keyspace"] = []string{testKeyspace}
			props["dc"] = []string{"dc1"}
			rawProps, err := json.Marshal(props)
			if err != nil {
				t.Fatal(err)
			}
			target, err := h.service.GetTarget(ctx, h.ClusterID, rawProps)
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				defer close(done)
				Print("When: backup is running")
				if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
					t.Errorf("Expected no error but got %+v", err)
				}
			}()
			// Wait for the backup to stop when the test fails, as it can't report errors after the test ends
			defer func() {
				cancel()
				<-done
			}()

			var alter sync.Once
			for range h.GetHostsFromDC("dc1") {
				select {
				case <-time.After(backupTimeout):
					t.Fatalf("Backup didn't take the snapshot in under %s", backupTimeout)
				case <-done:
					t.Fatal("Backup completed before taking the snapshot on all nodes")
				case b := <-blocked:
					alter.Do(func() {
						Print("And: table encryption changed")
						ExecStmt(t, clusterSession, fmt.Sprintf("ALTER TABLE %q.%q WITH scylla_encryption_options = %s",
							testKeyspace, BigTableName, tc.newEncryption))
					})
					Printf("And: SSTables rewritten on %s", b.host)
					if err := ExecOnHostStatus(t, b.host, fmt.Sprintf("nodetool upgradesstables -a %s %s", testKeyspace, BigTableName)); err != nil {
						t.Fatal(err)
					}
					close(b.release)
				}
			}

			select {
			case <-time.After(backupTimeout * 3):
				t.Fatalf("Backup failed to complete in under %s", backupTimeout*3)
			case <-done:
			}

			Printf("Then: %s", tc.description)
			h.assertManifestsEncryptionKeys(t, h.RunID, len(h.GetHostsFromDC("dc1")))
		})
	}
}

// createTable creates the table replicated to rf dc1 nodes with the given scylla_encryption_options
// (not encrypted when empty) and inserts a single row into it.
func createTable(t *testing.T, clusterSession gocqlx.Session, ks, tab string, rf int, encryption string) {
	t.Helper()

	ExecStmt(t, clusterSession, fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %q WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d}", ks, rf))
	stmt := fmt.Sprintf("CREATE TABLE %q.%q (id int PRIMARY KEY, data blob)", ks, tab)
	if encryption != "" {
		stmt += " WITH scylla_encryption_options = " + encryption
	}
	ExecStmt(t, clusterSession, stmt)
	ExecStmt(t, clusterSession, fmt.Sprintf("INSERT INTO %q.%q (id, data) VALUES (0, 0x00)", ks, tab))
}

// assertManifestsEncryptionKeys checks that each dc1 node has a manifest of the run,
// out of which exactly hostsWithKey list the local key, and the other ones list no keys.
func (h *backupTestHelper) assertManifestsEncryptionKeys(t *testing.T, runID uuid.UUID, hostsWithKey int) {
	t.Helper()

	hosts := h.GetHostsFromDC("dc1")
	keys := h.manifestsEncryptionKeys(t, runID)
	if len(keys) != len(hosts) {
		t.Fatalf("Expected manifests of %v, got manifests of %v", hosts, slices.Collect(maps.Keys(keys)))
	}
	var withKey int
	for _, host := range hosts {
		k, ok := keys[host]
		switch {
		case !ok:
			t.Fatalf("Missing manifest of %s", host)
		case cmp.Equal(k, []backupspec.EncryptionKey{localKey}):
			withKey++
		case len(k) > 0:
			t.Fatalf("Manifest of %s lists unexpected keys %v", host, k)
		}
	}
	if withKey != hostsWithKey {
		t.Fatalf("Expected %d manifests listing the local key, got %d: %v", hostsWithKey, withKey, keys)
	}
}

// manifestsEncryptionKeys returns the encryption keys listed in the node manifests of the run, mapped by node IP.
func (h *backupTestHelper) manifestsEncryptionKeys(t *testing.T, runID uuid.UUID) map[string][]backupspec.EncryptionKey {
	t.Helper()

	run, err := h.service.GetRun(t.Context(), h.ClusterID, h.TaskID, runID)
	if err != nil {
		t.Fatal(err)
	}
	manifests, _, _, _ := h.listS3Files()
	regular, _ := splitManifests(manifests)
	out := make(map[string][]backupspec.EncryptionKey)
	for _, m := range regular {
		if backup.SnapshotTagFromManifestPath(t, m) != run.SnapshotTag {
			continue
		}
		r, err := h.Client.RcloneOpen(t.Context(), ManagedClusterHost(), h.location.RemotePath(m))
		if err != nil {
			t.Fatal(err)
		}
		var mc backupspec.ManifestContentWithIndex
		err = mc.Read(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		out[mc.IP] = mc.EncryptionKeys
	}
	return out
}
