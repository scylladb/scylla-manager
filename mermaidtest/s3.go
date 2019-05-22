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
	flagS3DataDir         = flag.String("s3-data-dir", "", "location of a local folder mounted by the test s3 instance")
	flagS3Endpoint        = flag.String("s3-endpoint", "", "s3 service compatible endpoint")
	flagS3AccessKeyID     = flag.String("s3-access-key-id", "", "s3 access key id")
	flagS3SecretAccessKey = flag.String("s3-secret-access-key", "", "s3 access key secret")
)

// NewS3Params return s3 remote params with AccessKeyID and SecretAccessKey from flags.
func NewS3Params() scyllaclient.S3Params {
	return scyllaclient.S3Params{
		Endpoint:        *flagS3Endpoint,
		AccessKeyID:     *flagS3AccessKeyID,
		SecretAccessKey: *flagS3SecretAccessKey,
		DisableChecksum: true,
	}
}

// NewS3ParamsEnvAuth return s3 remote params from flags.
func NewS3ParamsEnvAuth() scyllaclient.S3Params {
	return scyllaclient.S3Params{
		Endpoint:        *flagS3Endpoint,
		EnvAuth:         true,
		DisableChecksum: true,
	}
}

// S3InitBucket recreates a local bucket if s3-local-data-dir flag is specified.
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
