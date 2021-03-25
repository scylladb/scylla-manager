// Copyright (C) 2017 ScyllaDB

package downloader_test

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/downloader"
	backup "github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
	"go.uber.org/zap/zapcore"
)

func TestLookupErrors(t *testing.T) {
	location := backup.Location{Provider: "testdata"}

	table := []struct {
		Name     string
		Criteria downloader.ManifestLookupCriteria
		Error    string
	}{
		{
			Name: "Wrong node id",
			Criteria: downloader.ManifestLookupCriteria{
				NodeID:      uuid.MustRandom(),
				SnapshotTag: "sm_20210215151954UTC",
			},
			Error: "unknown node ID",
		},
		{
			Name: "Wrong snapshot",
			Criteria: downloader.ManifestLookupCriteria{
				NodeID:      uuid.MustParse("942ba1b6-30a3-441e-ac3c-158864d8b861"),
				SnapshotTag: "x",
			},
			Error: "unknown snapshot",
		},
	}

	dir, err := ioutil.TempDir("", "scylla-manager-downloader")
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
			d, err := downloader.New(location, dir, logger)
			if err != nil {
				t.Fatal("New() error", err)
			}

			if test.Error == "" {
				t.Fatal("Missing test error")
			}

			_, err = d.LookupManifest(ctx, test.Criteria)
			if err == nil {
				t.Error("LookupManifest() expected error")
			}
			if !strings.Contains(err.Error(), test.Error) {
				t.Errorf("LookupManifest() error %s, expected %s", err, test.Error)
			} else {
				t.Logf("LookupManifest() error %s", err)
			}
		})
	}
}
