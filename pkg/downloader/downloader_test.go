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

	clearTableOption := func(dir string, opts ...downloader.Option) func(d *downloader.Downloader) error {
		return func(d *downloader.Downloader) error {
			dir := path.Join(d.Root(), dir)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(path.Join(dir, "a"), []byte("foo"), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(path.Join(dir, "b"), []byte("bar"), 0o755); err != nil {
				return err
			}
			for _, o := range opts {
				if err := o(d); err != nil {
					return err
				}
			}
			return nil
		}
	}

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
			Option: clearTableOption(
				"system_auth/role_permissions-f4d5d0c0671be202bc241807c243e80b",
				downloader.WithKeyspace([]string{"system_auth.role_permissions"}),
				downloader.WithClearTables(),
			),
		},
		{
			Name: "Clear table upload dir mode",
			Option: clearTableOption(
				"system_auth/role_permissions-3afbe79f219431a7add7f5ab90d8ec9c/upload",
				downloader.WithKeyspace([]string{"system_auth.role_permissions"}),
				downloader.WithTableDirMode(downloader.UploadTableDirMode),
				downloader.WithClearTables(),
			),
		},
		{
			Name: "Clear table sstable dir mode",
			Option: clearTableOption(
				"system_auth/role_permissions",
				downloader.WithKeyspace([]string{"system_auth.role_permissions"}),
				downloader.WithTableDirMode(downloader.SSTableLoaderTableDirMode),
				downloader.WithClearTables(),
			),
		},
	}

	var (
		ctx    = context.Background()
		logger = log.NewDevelopmentWithLevel(zapcore.InfoLevel)
	)
	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "scylla-manager-downloader")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)

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

func TestDownloadOwnerCheck(t *testing.T) {
	location := backup.Location{Provider: "testdata"}
	d, err := downloader.New(location, "/root/", log.NewDevelopment())

	err = d.Download(context.Background(), dummyManifest())
	if err == nil || !strings.Contains(err.Error(), "run command as root") {
		t.Fatalf("New() error %s, expected run command as root", err)
	}
}

func TestDownloadFilterCheck(t *testing.T) {
	location := backup.Location{Provider: "testdata"}
	d, err := downloader.New(location, ".", log.NewDevelopment(), downloader.WithKeyspace([]string{"bla"}))

	err = d.Download(context.Background(), dummyManifest())
	if err == nil || !strings.Contains(err.Error(), "no data matching filters bla") {
		t.Fatalf("New() error %s, expected no data matching filters bla", err)
	}
}

func dummyManifest() backup.ManifestInfoWithContent {
	return backup.ManifestInfoWithContent{
		ManifestInfo: &backup.ManifestInfo{},
		ManifestContent: &backup.ManifestContent{
			Index: []backup.FilesMeta{
				{
					Keyspace: "foo",
					Table:    "bar",
				},
			},
		},
	}
}
