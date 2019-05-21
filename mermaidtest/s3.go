// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/scylladb/mermaid/scyllaclient"
)

var (
	flagS3Provider          = flag.String("s3-provider", "Minio", "s3 service provider")
	flagS3Region            = flag.String("s3-region", "us-east-1", "s3 region")
	flagS3EnvAuth           = flag.Bool("s3-envauth", false, "take s3 config from environment")
	flagS3AccessKeyID       = flag.String("s3-access-key-id", "minio", "s3 access key id")
	flagS3SecretAccessKey   = flag.String("s3-secret-access-key", "minio123", "s3 access key secret")
	flagS3Endpoint          = flag.String("s3-endpoint", "http://192.168.100.99:9000", "s3 service compatible endpoint")
	flagS3DisableChecksum   = flag.Bool("s3-disable-checksum", true, "disable checksum for operations")
	flagS3UploadConcurrency = flag.Int("s3-upload-concurrency", 4, "how many concurent uploads per operation")
	flagS3LocalDataDir      = flag.String("s3-local-data-dir", "", "location of a local folder mounted by the test s3 instance")
)

// S3ParamsFromFlags get s3 remote params from the cli flags.
func S3ParamsFromFlags() scyllaclient.S3Params {
	return scyllaclient.S3Params{
		Provider:          *flagS3Provider,
		Region:            *flagS3Region,
		EnvAuth:           *flagS3EnvAuth,
		AccessKeyID:       *flagS3AccessKeyID,
		SecretAccessKey:   *flagS3SecretAccessKey,
		Endpoint:          *flagS3Endpoint,
		DisableChecksum:   *flagS3DisableChecksum,
		UploadConcurrency: *flagS3UploadConcurrency,
	}
}

// S3InitBucket recreates a local bucket if s3-local-data-dir flag is specified.
func S3InitBucket(t *testing.T, bucket string) {
	t.Helper()

	if *flagS3LocalDataDir == "" {
		t.Logf("No local data dir specified clearing bucket %s skipped", bucket)
		return
	}

	p := filepath.Join(*flagS3LocalDataDir, bucket)
	if err := os.RemoveAll(p); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p, 0700); err != nil {
		t.Fatal(err)
	}
}
