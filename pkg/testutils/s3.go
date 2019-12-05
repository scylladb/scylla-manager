// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var (
	flagS3DataDir         = flag.String("s3-data-dir", "", "path to test S3 instance root data dir")
	flagS3Endpoint        = flag.String("s3-endpoint", "", "test S3 instance endpoint")
	flagS3AccessKeyID     = flag.String("s3-access-key-id", "", "test S3 instance access key")
	flagS3SecretAccessKey = flag.String("s3-secret-access-key", "", "test S3 instance secret")
)

// S3InitBucket recreates a local bucket if s3-data-dir flag is specified.
func S3InitBucket(t *testing.T, bucket string) {
	t.Helper()

	if !flag.Parsed() {
		flag.Parse()
	}
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

// S3Credentials returns endpoint and credentials to test S3 instance.
func S3Credentials() (endpoint, accessKeyID, secretAccessKey string) {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagS3Endpoint, *flagS3AccessKeyID, *flagS3SecretAccessKey
}
