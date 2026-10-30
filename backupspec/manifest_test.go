// Copyright (C) 2017 ScyllaDB

package backupspec

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestManifestContentEncryptionKeysJSON(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		keys     []EncryptionKey
		wantJSON string
	}{
		{
			name:     "absent when there are no keys",
			keys:     nil,
			wantJSON: "",
		},
		{
			name:     "present when keys exist",
			keys:     []EncryptionKey{{Provider: "KmsKeyProviderFactory", KmsHost: "key-a", SecretKeyStrength: "128"}},
			wantJSON: `"encryption_keys":[{"provider":"KmsKeyProviderFactory","kms_host":"key-a","secret_key_strength":"128"}]`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(ManifestContent{EncryptionKeys: tc.keys})
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantJSON == "" {
				if strings.Contains(string(b), "encryption_keys") {
					t.Fatalf("expected no encryption_keys, got %s", b)
				}
				return
			}
			if !strings.Contains(string(b), tc.wantJSON) {
				t.Fatalf("expected %s in %s", tc.wantJSON, b)
			}
		})
	}

	t.Run("manifest without the field still decodes", func(t *testing.T) {
		t.Parallel()
		var m ManifestContent
		if err := json.Unmarshal([]byte(`{"version":"v2","cluster_name":"c","ip":"1.2.3.4","size":10}`), &m); err != nil {
			t.Fatal(err)
		}
		if m.EncryptionKeys != nil || m.ClusterName != "c" {
			t.Fatalf("unexpected decode result: %+v", m)
		}
	})
}

func TestManifestInfoParsePath(t *testing.T) {
	t.Parallel()

	opts := cmp.Options{
		cmp.Comparer(func(a, b uuid.UUID) bool { return a == b }),
		cmpopts.IgnoreUnexported(ManifestInfo{}),
	}

	prototype := ManifestInfo{
		ClusterID:   uuid.MustRandom(),
		DC:          "a",
		NodeID:      "b",
		TaskID:      uuid.MustRandom(),
		SnapshotTag: NewSnapshotTag(),
	}

	t.Run("normal", func(t *testing.T) {
		golden := prototype

		var m ManifestInfo
		if err := m.ParsePath(golden.Path()); err != nil {
			t.Fatal("ParsePath() error", err)
		}
		if diff := cmp.Diff(m, golden, opts); diff != "" {
			t.Fatal("ParsePath() diff", diff)
		}
	})

	t.Run("temporary", func(t *testing.T) {
		golden := prototype
		golden.Temporary = true

		var m ManifestInfo
		if err := m.ParsePath(golden.Path()); err != nil {
			t.Fatal("ParsePath() error", err)
		}
		if diff := cmp.Diff(m, golden, opts); diff != "" {
			t.Fatal("ParsePath() diff", diff)
		}
	})
}

func TestManifestInfoParsePathErrors(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name  string
		Path  string
		Error string
	}{
		{
			Name:  "invalid prefix",
			Path:  "foobar",
			Error: "expected backup",
		},
		{
			Name:  "invalid cluster ID",
			Path:  "backup/meta/cluster/bla",
			Error: "invalid UUID",
		},
		{
			Name:  "invalid static DC",
			Path:  "backup/meta/cluster/" + uuid.MustRandom().String() + "/bla",
			Error: "expected dc",
		},
		{
			Name:  "not a manifest file",
			Path:  RemoteManifestFile(uuid.MustRandom(), uuid.MustRandom(), NewSnapshotTag(), "dc", "nodeID") + ".old",
			Error: "expected one of [manifest.json.gz manifest.json.gz.tmp]",
		},
		{
			Name:  "manifest submatch",
			Path:  strings.Join(strings.Split(RemoteManifestFile(uuid.MustRandom(), uuid.MustRandom(), NewSnapshotTag(), "dc", "nodeID"), sep)[0:5], sep),
			Error: "no input at position 5",
		},
		{
			Name:  "SSTable dir",
			Path:  RemoteSSTableVersionDir(uuid.MustRandom(), "dc", "nodeID", "keyspace", "table", "version"),
			Error: "expected meta",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			var m ManifestInfo
			err := m.ParsePath(test.Path)
			if err == nil {
				t.Fatal("ParsePath() expected error")
			}

			t.Log("ParsePath():", err)
			if !strings.Contains(err.Error(), test.Error) {
				t.Fatalf("ParsePath() = %v, expected %v", err, test.Error)
			}
		})
	}
}
