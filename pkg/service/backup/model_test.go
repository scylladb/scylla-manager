// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/pkg/rclone/rcserver"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func TestProviderMarshalUnmarshalText(t *testing.T) {
	t.Parallel()

	for _, k := range []Provider{S3} {
		b, err := k.MarshalText()
		if err != nil {
			t.Error(k, err)
		}
		var p Provider
		if err := p.UnmarshalText(b); err != nil {
			t.Error(err)
		}
		if k != p {
			t.Errorf("Got %s, expected %s", p, k)
		}
	}
}

func TestLocationMarshalUnmarshalText(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name     string
		Location Location
	}{
		{
			Name: "with dc",
			Location: Location{
				DC:       "dc",
				Provider: S3,
				Path:     "my-bucket.domain",
			},
		},
		{
			Name: "without dc",
			Location: Location{
				Provider: S3,
				Path:     "my-bucket.domain",
			},
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			golden := test.Location
			b, err := golden.MarshalText()
			if err != nil {
				t.Error(golden, err)
			}
			var l Location
			if err := l.UnmarshalText(b); err != nil {
				t.Error(err)
			}
			if golden != l {
				t.Errorf("Got %s, expected %s", l, golden)
			}
		})
	}
}

func TestInvalidLocationUnmarshalText(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name     string
		Location string
	}{
		{
			Name:     "empty",
			Location: "",
		},
		{
			Name:     "empty path",
			Location: "s3:",
		},
		{
			Name:     "empty path with dc",
			Location: "dc:s3:",
		},
		{
			Name:     "invalid dc",
			Location: "dc aaa:foo:bar",
		},
		{
			Name:     "invalid provider",
			Location: "foo:bar",
		},
		{
			Name:     "invalid path",
			Location: "s3:name boo",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			l := Location{}
			if err := l.UnmarshalText([]byte(test.Location)); err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestLocationRemotePath(t *testing.T) {
	t.Parallel()

	l := Location{
		Provider: S3,
		Path:     "foo",
	}

	table := []struct {
		Path       string
		RemotePath string
	}{
		{
			Path:       "bar",
			RemotePath: "s3:foo/bar",
		},
		{
			Path:       "/bar",
			RemotePath: "s3:foo/bar",
		},
	}

	for _, test := range table {
		if p := l.RemotePath(test.Path); p != test.RemotePath {
			t.Error("expected", test.RemotePath, "got", p)
		}
	}
}

func TestDCLimitMarshalUnmarshalText(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name    string
		DCLimit DCLimit
	}{
		{
			Name: "with dc",
			DCLimit: DCLimit{
				DC:    "dc",
				Limit: 100,
			},
		},
		{
			Name: "without dc",
			DCLimit: DCLimit{
				Limit: 100,
			},
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			golden := test.DCLimit
			b, err := golden.MarshalText()
			if err != nil {
				t.Error(golden, err)
			}
			var r DCLimit
			if err := r.UnmarshalText(b); err != nil {
				t.Error(err)
			}
			if golden != r {
				t.Errorf("Got %s, expected %s", r, golden)
			}
		})
	}
}

func TestCatLimitIsEnoughToDownloadManifest(t *testing.T) {
	var manifest manifestContent

	const (
		keyspaces     = 2
		tables        = 8000
		filesPerTable = 200
		tokensRanges  = 256
	)

	metaFilesFormats := []string{
		"mc-%d-big-Data.db",
		"mc-%d-big-Index.db",
		"mc-%d-big-Scylla.db",
		"mc-%d-big-Digest.crc32",
		"mc-%d-big-TOC.txt",
		"mc-%d-big-Filter.db",
		"mc-%d-big-Statistics.db",
		"mc-%d-big-Summary.db",
	}

	totalFiles := keyspaces * tables * filesPerTable
	Printf("Given: manifest with %d keyspaces each having %d tables, each having %d SST files, %d files in total", keyspaces, tables, filesPerTable, totalFiles)

	manifest.Index = make([]filesInfo, 0, tables)

	for k := 0; k < keyspaces; k++ {
		keyspaceName := uuid.MustRandom()
		for t := 0; t < tables; t++ {
			tableName := uuid.MustRandom()
			idx := filesInfo{
				Keyspace: keyspaceName.String(),
				Table:    tableName.String(),
				Version:  strings.ReplaceAll(uuid.MustRandom().String(), "-", ""),
				Files:    make([]string, 0, filesPerTable),
			}

			for f := filesPerTable; f > 0; f -= len(metaFilesFormats) {
				for _, mfmt := range metaFilesFormats {
					idx.Size += rand.Int63n(1 * 1024 * 1024 * 1024)
					idx.Files = append(idx.Files, fmt.Sprintf(mfmt, f))
				}
			}
			manifest.Index = append(manifest.Index, idx)
		}

		manifest.Tokens = make([]int64, tokensRanges, tokensRanges)
		for i := 0; i < tokensRanges; i++ {
			manifest.Tokens[i] = rand.Int63()
		}
	}

	var buf bytes.Buffer
	if err := manifest.Write(&buf); err != nil {
		t.Fatal(err)
	}

	Printf("Then: manifest takes %dB", buf.Len())

	// Lets reserve 20% space for errors in calculations
	limit := rcserver.CatLimit * 0.80
	if buf.Len() >= int(limit) {
		t.Errorf("Cat limit is not enouogh to download manifest of %d size", buf.Len())
	}
}
