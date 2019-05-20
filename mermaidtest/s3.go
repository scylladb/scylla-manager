// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"flag"

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
