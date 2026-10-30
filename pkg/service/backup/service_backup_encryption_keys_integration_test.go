// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package backup_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/backupspec"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
)

// TestBackupEncryptionKeysIntegration tests that node manifests list the encryption keys
// of the backed up SSTables (encryption_keys manifest field).
// Unencrypted tables don't contribute any key, while encrypted tables contribute
// the key options reported by Scylla sstable_info (here: local file key provider).
func TestBackupEncryptionKeysIntegration(t *testing.T) {
	const (
		localKeyProvider = "LocalFileSystemKeyProviderFactory"
		ksStmt           = "CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
		plainTabStmt     = "CREATE TABLE %q.%q (id int PRIMARY KEY, data int)"
		encryptedTabStmt = "CREATE TABLE %q.%q (id int PRIMARY KEY, data int) WITH scylla_encryption_options = " +
			"{'cipher_algorithm': 'AES/ECB/PKCS5Padding', 'secret_key_strength': 128, " +
			"'key_provider': 'LocalFileSystemKeyProviderFactory', 'secret_key_file': '/var/lib/scylla/sm_backup_test_key'}"
	)

	testCases := []struct {
		name        string
		description string
		tabStmt     string
		// expectedProvider is the provider of the only key listed in manifests,
		// or empty when manifests shouldn't list any key.
		expectedProvider string
	}{
		{
			name:        "unencrypted table",
			description: "manifests don't list any encryption key",
			tabStmt:     plainTabStmt,
		},
		{
			name:             "encrypted table",
			description:      "manifests list the local file key provider",
			tabStmt:          encryptedTabStmt,
			expectedProvider: localKeyProvider,
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				ctx            = context.Background()
				location       = testBackupLocation(fmt.Sprintf("backuptest-encryption-keys-%d", i))
				session        = CreateScyllaManagerDBSession(t)
				h              = newBackupTestHelper(t, session, defaultConfig(), location, nil)
				clusterSession = CreateSessionAndDropAllKeyspaces(t, h.Client)
				ks             = "encryption_keys_ks"
				tab            = "tab"
			)

			Printf("Given: %s", tc.name)
			ExecStmt(t, clusterSession, fmt.Sprintf(ksStmt, ks))
			if err := clusterSession.ExecStmt(fmt.Sprintf(tc.tabStmt, ks, tab)); err != nil {
				if tc.expectedProvider != "" {
					t.Skipf("Encryption at rest is not supported by the cluster: %s", err)
				}
				t.Fatal(err)
			}
			for j := range 100 {
				ExecStmt(t, clusterSession, fmt.Sprintf("INSERT INTO %q.%q (id, data) VALUES (%d, %d)", ks, tab, j, j))
			}

			Print("When: run backup")
			rawProps, err := json.Marshal(defaultTestProperties(location, ks))
			if err != nil {
				t.Fatal(err)
			}
			target, err := h.service.GetTarget(ctx, h.ClusterID, rawProps)
			if err != nil {
				t.Fatal(err)
			}
			if err := h.service.Backup(ctx, h.ClusterID, h.TaskID, h.RunID, target); err != nil {
				t.Fatal(err)
			}

			Printf("Then: %s", tc.description)
			manifests, _, _, _ := h.listS3Files()
			regular, _ := splitManifests(manifests)
			if len(regular) == 0 {
				t.Fatal("Expected manifests")
			}
			var withKeys int
			for _, m := range regular {
				keys := h.manifestEncryptionKeys(ctx, m)
				if len(keys) == 0 {
					continue
				}
				withKeys++
				if tc.expectedProvider == "" {
					t.Fatalf("Expected no encryption keys in manifest %s, got %+v", m, keys)
				}
				if len(keys) != 1 || keys[0].Provider != tc.expectedProvider {
					t.Fatalf("Expected single %s key in manifest %s, got %+v", tc.expectedProvider, m, keys)
				}
				if !strings.HasSuffix(keys[0].SecretKeyFile, "sm_backup_test_key") && keys[0].SecretKeyFile != "" {
					t.Fatalf("Unexpected secret key file in manifest %s: %+v", m, keys[0])
				}
			}
			if tc.expectedProvider != "" && withKeys == 0 {
				t.Fatalf("Expected some manifests to list %s key", tc.expectedProvider)
			}
		})
	}
}

func (h *backupTestHelper) manifestEncryptionKeys(ctx context.Context, manifestPath string) []backupspec.EncryptionKey {
	h.T.Helper()

	m := backupspec.NewManifestInfoWithContent()
	if err := m.ParsePath(manifestPath); err != nil {
		h.T.Fatal(err)
	}
	r, err := h.Client.RcloneOpen(ctx, ManagedClusterHost(), h.location.RemotePath(manifestPath))
	if err != nil {
		h.T.Fatal(err)
	}
	defer r.Close()
	if err := m.Read(r); err != nil {
		h.T.Fatal(err)
	}
	return m.EncryptionKeys
}
