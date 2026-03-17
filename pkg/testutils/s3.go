// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"flag"
	"path"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
)

var (
	flagS3DataDir         = flag.String("s3-data-dir", "", "path to test S3 instance root data dir")
	flagS3Provider        = flag.String("s3-provider", "", "test S3 instance provider")
	flagS3Endpoint        = flag.String("s3-endpoint", "", "test S3 instance endpoint")
	flagS3AccessKeyID     = flag.String("s3-access-key-id", "", "test S3 instance access key")
	flagS3SecretAccessKey = flag.String("s3-secret-access-key", "", "test S3 instance secret")
)

// S3InitBucket recreates a local bucket if s3-data-dir flag is specified.
func S3InitBucket(t *testing.T, bucket string) {
	t.Helper()

	p := path.Join("/shared", bucket)
	_, _, err := ExecOnHost(testconfig.ManagedClusterHost(), "rm -rf "+p)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = ExecOnHost(testconfig.ManagedClusterHost(), "mkdir -p -m 777 "+p)
	if err != nil {
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
