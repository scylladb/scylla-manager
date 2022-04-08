// Copyright (C) 2017 ScyllaDB

package downloader_test

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/downloader"
	backup "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
)

func TestListNodeSnapshots(t *testing.T) {
	location := backup.Location{Provider: "testdata"}

	table := []struct {
		Name   string
		Option downloader.Option
		NodeID uuid.UUID
	}{
		{
			Name:   "Default",
			NodeID: uuid.MustParse("942ba1b6-30a3-441e-ac3c-158864d8b861"),
		},
		{
			Name:   "Wrong node id",
			NodeID: uuid.MustRandom(),
		},
		{
			Name:   "Not matching keyspace",
			NodeID: uuid.MustRandom(),
			Option: downloader.WithKeyspace([]string{"foo"}),
		},
	}

	dir, err := ioutil.TempDir("", "scylla-manager-downloader-lookup")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	var (
		ctx    = context.Background()
		logger = log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	)
	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			var opts []downloader.Option
			if test.Option != nil {
				opts = append(opts, test.Option)
			}

			d, err := downloader.New(location, dir, logger, opts...)
			if err != nil {
				t.Fatal("New() error", err)
			}

			snapshots, err := d.ListNodeSnapshots(ctx, test.NodeID)
			if err != nil {
				t.Fatal("ListNodeSnapshots() error", err)
			}
			testutils.SaveGoldenJSONFileIfNeeded(t, snapshots)
			var golden []string
			testutils.LoadGoldenJSONFile(t, &golden)
			if diff := cmp.Diff(snapshots, golden); diff != "" {
				t.Error(diff)
			}
		})
	}
}

func TestListNodes(t *testing.T) {
	location := backup.Location{Provider: "testdata"}

	dir, err := ioutil.TempDir("", "scylla-manager-downloader-lookup")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	var (
		ctx    = context.Background()
		logger = log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	)

	d, err := downloader.New(location, dir, logger)
	if err != nil {
		t.Fatal("New() error", err)
	}

	nodes, err := d.ListNodes(ctx)

	t.Run("nodes", func(t *testing.T) {
		testutils.SaveGoldenJSONFileIfNeeded(t, nodes)
		var golden downloader.NodeInfoSlice
		testutils.LoadGoldenJSONFile(t, &golden)
		if diff := cmp.Diff(nodes, golden, testutils.UUIDComparer()); diff != "" {
			t.Error(diff)
		}
	})

	t.Run("nodes text", func(t *testing.T) {
		b := &strings.Builder{}
		nodes.WriteTo(b)
		text := b.String()

		testutils.SaveGoldenTextFileIfNeeded(t, text)
		golden := testutils.LoadGoldenTextFile(t)
		if diff := cmp.Diff(text, golden); diff != "" {
			t.Error(diff)
		}
	},
	)
}
