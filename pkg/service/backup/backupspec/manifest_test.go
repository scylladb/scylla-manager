// Copyright (C) 2017 ScyllaDB

package backupspec

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func TestRemoteManifestParsePath(t *testing.T) {
	t.Parallel()

	opts := cmp.Options{
		UUIDComparer(),
		cmpopts.IgnoreFields(RemoteManifest{}, "CleanPath"),
		cmpopts.IgnoreUnexported(RemoteManifest{}),
	}

	prototype := RemoteManifest{
		ClusterID:   uuid.MustRandom(),
		DC:          "a",
		NodeID:      "b",
		TaskID:      uuid.MustRandom(),
		SnapshotTag: NewSnapshotTag(),
	}

	t.Run("normal", func(t *testing.T) {
		golden := prototype

		var m RemoteManifest
		if err := m.ParsePath(golden.RemoteManifestFile()); err != nil {
			t.Fatal("ParsePath() error", err)
		}
		if diff := cmp.Diff(m, golden, opts); diff != "" {
			t.Fatal("ParsePath() diff", diff)
		}
	})

	t.Run("temporary", func(t *testing.T) {
		golden := prototype
		golden.Temporary = true

		var m RemoteManifest
		if err := m.ParsePath(golden.RemoteManifestFile()); err != nil {
			t.Fatal("ParsePath() error", err)
		}
		if diff := cmp.Diff(m, golden, opts); diff != "" {
			t.Fatal("ParsePath() diff", diff)
		}
	})
}

func TestRemoteManifestParsePathErrors(t *testing.T) {
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

			var m RemoteManifest
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
