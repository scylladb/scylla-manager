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

// TestManifestContentEncryptionKeys verifies that encryption keys are correctly unmarshalled whether present
// or not. Additionally, verifies that if no keys are present, the field is omitted from the resulting JSON.
func TestManifestContentEncryptionKeys(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name     string
		JSON     string
		Expected []EncryptionKey
	}{
		{
			Name: "manifest without the field",
			JSON: `{"version":"v2","cluster_name":"c"}`,
		},
		{
			Name: "host based and local keys",
			JSON: `{"version":"v2","encryption_keys":[` +
				`{"provider":"KmsKeyProviderFactory","host":"my-kms"},` +
				`{"provider":"GcpKeyProviderFactory","host":"my-gcp"},` +
				`{"provider":"AzureKeyProviderFactory","host":"my-azure"},` +
				`{"provider":"KmipKeyProviderFactory","host":"my-kmip"},` +
				`{"provider":"LocalFileSystemKeyProviderFactory"}]}`,
			Expected: []EncryptionKey{
				{Provider: "KmsKeyProviderFactory", Host: "my-kms"},
				{Provider: "GcpKeyProviderFactory", Host: "my-gcp"},
				{Provider: "AzureKeyProviderFactory", Host: "my-azure"},
				{Provider: "KmipKeyProviderFactory", Host: "my-kmip"},
				{Provider: "LocalFileSystemKeyProviderFactory"},
			},
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			var m ManifestContent
			if err := json.Unmarshal([]byte(test.JSON), &m); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(m.EncryptionKeys, test.Expected); diff != "" {
				t.Fatal("Unmarshal() diff", diff)
			}
		})
	}

	t.Run("omitted when empty", func(t *testing.T) {
		t.Parallel()

		b, err := json.Marshal(ManifestContent{})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "encryption_keys") {
			t.Fatalf("Marshal() = %s, expected no encryption_keys", b)
		}
	})
}
