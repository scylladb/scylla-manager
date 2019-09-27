// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/multierr"
)

var (
	flagS3DataDir         = flag.String("s3-data-dir", "", "location of a local folder mounted by the test s3 instance")
	flagS3Endpoint        = flag.String("s3-endpoint", "", "s3 service compatible endpoint")
	flagS3AccessKeyID     = flag.String("s3-access-key-id", "", "s3 access key id")
	flagS3SecretAccessKey = flag.String("s3-secret-access-key", "", "s3 access key secret")
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

// S3SetEnvAuth sets environment variables when needed on the local machine.
func S3SetEnvAuth(t *testing.T) {
	t.Helper()

	errs := multierr.Combine(
		os.Setenv("AWS_S3_ENDPOINT", S3TestEndpoint()),
		os.Setenv("AWS_ACCESS_KEY_ID", *flagS3AccessKeyID),
		os.Setenv("AWS_SECRET_ACCESS_KEY", *flagS3SecretAccessKey),
	)

	if errs != nil {
		t.Fatal(errs)
	}
}

// S3TestEndpoint returns an endpoint for testing like minio.
func S3TestEndpoint() string {
	return *flagS3Endpoint
}
