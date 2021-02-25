// Copyright (C) 2017 ScyllaDB

package backup

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/pkg/rclone/rcserver"
	. "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	. "github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

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
	var manifest ManifestContent

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

	manifest.Index = make([]FilesMeta, 0, tables)

	for k := 0; k < keyspaces; k++ {
		keyspaceName := uuid.MustRandom()
		for t := 0; t < tables; t++ {
			tableName := uuid.MustRandom()
			idx := FilesMeta{
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
