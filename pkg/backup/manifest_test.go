// Copyright (C) 2017 ScyllaDB

package backup

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

	golden := RemoteManifest{
		ClusterID:   uuid.MustRandom(),
		DC:          "a",
		NodeID:      "b",
		TaskID:      uuid.MustRandom(),
		SnapshotTag: NewSnapshotTag(),
	}

	for _, temporary := range []bool{false, true} {
		golden.Temporary = temporary

		var m RemoteManifest
		if err := m.ParsePartialPath(golden.RemoteManifestFile()); err != nil {
			t.Fatal("ParsePartialPath() error", err)
		}
		if diff := cmp.Diff(m, golden, opts); diff != "" {
			t.Fatal("ParsePartialPath() diff", diff)
		}
	}
}

func TestRemoteManifestParsePathEmpty(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name string
		Path string
	}{
		{
			Name: "empty",
			Path: "",
		},
		{
			Name: "backup prefix",
			Path: "backup",
		},
		{
			Name: "backup prefix with slash",
			Path: "/backup",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			var p RemoteManifest
			if err := p.ParsePartialPath(test.Path); err != nil {
				t.Fatal("ParsePartialPath() error", err)
			}
		})
	}
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
			Name:  "sSTable dir",
			Path:  RemoteSSTableVersionDir(uuid.MustRandom(), "dc", "nodeID", "keyspace", "table", "version"),
			Error: "expected meta",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			var m RemoteManifest
			err := m.ParsePartialPath(test.Path)
			if err == nil {
				t.Fatal("ParsePartialPath() expected error")
			}

			t.Log("ParsePartialPath():", err)
			if !strings.Contains(err.Error(), test.Error) {
				t.Fatalf("ParsePartialPath() = %v, expected %v", err, test.Error)
			}
		})
	}
}
