// Copyright (C) 2017 ScyllaDB

package downloader_test

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/downloader"
	backup "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/testutils"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
)

func TestDownload(t *testing.T) {
	var (
		location = backup.Location{Provider: "testdata"}
		criteria = downloader.ManifestLookupCriteria{
			NodeID:      uuid.MustParse("942ba1b6-30a3-441e-ac3c-158864d8b861"),
			SnapshotTag: "sm_20210215151954UTC",
		}
	)

	tmpDir, err := ioutil.TempDir("", "scylla-manager-rclone")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	table := []struct {
		Name   string
		Option downloader.Option
	}{
		{
			Name:   "Default",
			Option: func(d *downloader.Downloader) error { return nil },
		},
		{
			Name:   "Upload dir mode",
			Option: downloader.WithTableDirMode(downloader.UploadTableDirMode),
		},
		{
			Name:   "SSTable dir mode",
			Option: downloader.WithTableDirMode(downloader.SSTableLoaderTableDirMode),
		},
		{
			Name:   "Filter single table",
			Option: downloader.WithKeyspace([]string{"system_auth.roles"}),
		},
		{
			Name:   "Filter many tables",
			Option: downloader.WithKeyspace([]string{"system_a*"}),
		},
		{
			Name: "Clear table",
			Option: func(d *downloader.Downloader) error {
				dir := path.Join(d.Root(), "system_auth", "role_permissions-f4d5d0c0671be202bc241807c243e80b")
				if err := os.MkdirAll(dir, 0755); err != nil {
					return err
				}
				if err := ioutil.WriteFile(path.Join(dir, "a"), []byte("foo"), 0755); err != nil {
					return err
				}
				if err := ioutil.WriteFile(path.Join(dir, "b"), []byte("bar"), 0755); err != nil {
					return err
				}

				opts := []downloader.Option{
					downloader.WithKeyspace([]string{"system_auth.role_permissions"}),
					downloader.WithClearTables(),
				}
				for _, o := range opts {
					if err := o(d); err != nil {
						return err
					}
				}

				return nil
			},
		},
	}

	var (
		ctx    = context.Background()
		logger = log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	)
	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			dir := path.Join(tmpDir, path.Base(t.Name()))

			d, err := downloader.New(location, dir, logger, test.Option)
			if err != nil {
				t.Fatal("New() error", err)
			}

			m, err := d.LookupManifest(ctx, criteria)
			if err != nil {
				t.Fatal("LookupManifest() error", err)
			}

			t.Run("plan", func(t *testing.T) {
				plan, err := d.DryRun(ctx, m)
				if err != nil {
					t.Fatal(err)
				}
				testutils.SaveGoldenJSONFileIfNeeded(t, plan)
				var golden downloader.Plan
				testutils.LoadGoldenJSONFile(t, &golden)
				if diff := cmp.Diff(plan, golden, testutils.UUIDComparer(), cmpopts.IgnoreUnexported(plan)); diff != "" {
					t.Error(diff)
				}
			})

			t.Run("plan text", func(t *testing.T) {
				plan, err := d.DryRun(ctx, m)
				if err != nil {
					t.Fatal(err)
				}
				b := &strings.Builder{}
				plan.WriteTo(b)
				text := b.String()

				testutils.SaveGoldenTextFileIfNeeded(t, text)
				golden := testutils.LoadGoldenTextFile(t)
				if diff := cmp.Diff(text, golden); diff != "" {
					t.Error(diff)
				}
			})

			t.Run("files", func(t *testing.T) {
				if err := d.Download(ctx, m); err != nil {
					t.Fatal(err)
				}
				var files []string

				fdst, err := fs.NewFs(ctx, dir)
				if err != nil {
					t.Fatal("NewFs() error", err)
				}
				operations.ListJSON(ctx, fdst, "", &operations.ListJSONOpt{Recurse: true, FilesOnly: true}, func(item *operations.ListJSONItem) error {
					files = append(files, item.Path)
					return nil
				})
				sort.Strings(files)
				testutils.SaveGoldenJSONFileIfNeeded(t, files)
				var golden []string
				testutils.LoadGoldenJSONFile(t, &golden)
				if diff := cmp.Diff(files, golden); diff != "" {
					t.Error(diff)
				}
			})
		})
	}
}
