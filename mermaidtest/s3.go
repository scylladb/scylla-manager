// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var (
	flagS3DataDir = flag.String("s3-data-dir", "", "location of a local folder mounted by the test s3 instance")
)

// S3InitBucket recreates a local bucket if s3-data-dir flag is specified.
func S3InitBucket(t *testing.T, bucket string) {
	t.Helper()

	if *flagS3DataDir == "" {
		t.Logf("No local data dir specified clearing bucket %s skipped", bucket)
		return
	}

	p := filepath.Join(*flagS3DataDir, bucket)
	if err := os.RemoveAll(p); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p, 0700); err != nil {
		t.Fatal(err)
	}
}
