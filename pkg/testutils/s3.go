// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
)

var (
	flagS3DataDir           = flag.String("s3-data-dir", "", "path to test S3 instance root data dir")
	flagS3Provider          = flag.String("s3-provider", "", "test S3 instance provider")
	flagS3Endpoint          = flag.String("s3-endpoint", "", "test S3 instance endpoint")
	flagS3AccessKeyID       = flag.String("s3-access-key-id", "", "test S3 instance access key")
	flagS3SecretAccessKey   = flag.String("s3-secret-access-key", "", "test S3 instance secret")
	flagLocalStorageDataDir = flag.String("localstorage-data-dir", "./localstorage/data", "path to test localstorage root data dir")
)

// InitBucket recreates a local bucket for testconfig.BackupProvider.
func InitBucket(t *testing.T, bucket string) {
	t.Helper()

	if !flag.Parsed() {
		flag.Parse()
	}

	switch testconfig.BackupProvider() {
	case backupspec.S3:
		S3InitBucket(t, bucket)
	case backupspec.LocalStorage:
		LocalStorageInitBucket(t, bucket)
	default:
		t.Fatalf("unsupported provider %s", testconfig.BackupProvider())
	}
}

// LocalStorageInitBucket recreates a local bucket if localstorage-data-dir flag is specified.
func LocalStorageInitBucket(t *testing.T, bucket string) {
	t.Helper()

	if !flag.Parsed() {
		flag.Parse()
	}
	if *flagLocalStorageDataDir == "" {
		t.Logf("No localstorage data dir specified, skipped clearing bucket %s", bucket)
		return
	}

	initDir(t, filepath.Join(*flagLocalStorageDataDir, bucket))
}

// S3InitBucket recreates a local bucket if s3-data-dir flag is specified.
func S3InitBucket(t *testing.T, bucket string) {
	t.Helper()

	if !flag.Parsed() {
		flag.Parse()
	}
	if *flagS3DataDir == "" {
		t.Logf("No s3 data dir specified, skipped clearing bucket %s", bucket)
		return
	}

	initDir(t, filepath.Join(*flagS3DataDir, bucket))
}

func initDir(t *testing.T, dir string) {
	t.Helper()

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
}

// S3Credentials returns provider, endpoint, and credentials to test S3 instance.
func S3Credentials() (provider, endpoint, accessKeyID, secretAccessKey string) {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagS3Provider, *flagS3Endpoint, *flagS3AccessKeyID, *flagS3SecretAccessKey
}
