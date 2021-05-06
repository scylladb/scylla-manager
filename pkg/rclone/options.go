// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"fmt"
	"os"

	"github.com/rclone/rclone/fs"
	"github.com/scylladb/go-set/strset"
)

//go:generate go run -tags +ignore generate_options.go

const (
	// Default value of 5MB caused that we encountered problems with S3
	// returning 5xx. In order to reduce number of requests to S3, we are
	// increasing chunk size by ten times, which decreases number of
	// requests by ten times.
	defaultChunkSize = "50M"

	_true  = "true"
	_false = "false"
)

// GlobalOptions is an alias for rclone fs.ConfigInfo.
type GlobalOptions = fs.ConfigInfo

// DefaultGlobalOptions returns rclong fs.ConfigInfo initialized with default
// values.
func DefaultGlobalOptions() GlobalOptions {
	c := fs.NewConfig()
	// Pass all logs, our logger decides which one to print.
	c.LogLevel = fs.LogLevelDebug
	// Don't use JSON log format in logging.
	c.UseJSONLog = false
	// Skip based on size only, not mod-time or checksum.
	c.SizeOnly = true
	// Skip post copy check of checksums.
	c.IgnoreChecksum = true
	// Delete even if there are I/O errors.
	c.IgnoreErrors = true
	// Don't update destination mod-time if files identical.
	c.NoUpdateModTime = true

	// The number of checkers to run in parallel.
	// Checkers do the equality checking of files during a sync.
	// For some storage systems (e.g. S3, Swift, Dropbox) this can take a
	// significant amount of time so they are run in parallel.
	// The default is to run 8 checkers in parallel.
	c.Checkers = 8
	// The number of file transfers to run in parallel.
	// It can sometimes be useful to set this to a smaller number if the remote
	// is giving a lot of timeouts or bigger if you have lots of bandwidth and a fast remote.
	// The default is to run 4 file transfers in parallel.
	//
	// Scylla:
	// In order to reduce memory footprint, we allow at most two concurrent uploads.
	// transfers * chunk size gives rough estimate how much memory for
	// upload buffers will be allocated.
	c.Transfers = 2
	// Number of low level retries to do. (default 10)
	// This applies to operations like S3 chunk upload.
	c.LowLevelRetries = 20
	// Maximum number of stats groups to keep in memory. On max oldest is discarded. (default 1000).
	c.MaxStatsGroups = 1000
	// Set proper agent for backend clients.
	c.UserAgent = UserAgent()

	// With this option set, files will be created and deleted as requested,
	// but existing files will never be updated. If an existing file does not
	// match between the source and destination, rclone will give the error
	// Source and destination exist but do not match: immutable file modified.
	c.Immutable = false

	return *c
}

var s3Providers = strset.New(
	"AWS", "Minio", "Alibaba", "Ceph", "DigitalOcean",
	"IBMCOS", "Wasabi", "Dreamhost", "Netease", "Other",
)

func DefaultS3Options() S3Options {
	return S3Options{
		Provider:        "AWS",
		ChunkSize:       defaultChunkSize,
		DisableChecksum: _true,
		EnvAuth:         _true,
		// Because of access denied issues with Minio.
		// see https://github.com/rclone/rclone/issues/4633
		NoCheckBucket:     _true,
		UploadConcurrency: "2",
	}
}

func (o *S3Options) Validate() error {
	if o.Endpoint != "" && o.Provider == "" {
		return fmt.Errorf("specify provider for the endpoint %s, available providers are: %s", o.Endpoint, s3Providers)
	}

	if o.Provider != "" && !s3Providers.Has(o.Provider) {
		return fmt.Errorf("unknown provider: %s", o.Provider)
	}

	return nil
}

// AutoFill sets region (if empty) from identity service, it only works when
// running in AWS.
func (o *S3Options) AutoFill() {
	if o.Region == "" && o.Endpoint == "" {
		o.Region = awsRegionFromMetadataAPI()
	}
}

func DefaultGCSOptions() GCSOptions {
	return GCSOptions{
		AllowCreateBucket: _false,
		// fine-grained buckets, and IAM bucket-level settings for uniform buckets.
		// each object. Permissions will be controlled by the ACL rules for
		// This option must be _true if we don't want rclone to set permission on
		BucketPolicyOnly: _true,
		ChunkSize:        defaultChunkSize,
	}
}

// AutoFill sets ServiceAccountFile if the default file exists.
func (o *GCSOptions) AutoFill() {
	const defaultServiceAccountFile = "/etc/scylla-manager-agent/gcs-service-account.json"

	if o.ServiceAccountFile == "" {
		if _, err := os.Stat(defaultServiceAccountFile); err == nil {
			o.ServiceAccountFile = defaultServiceAccountFile
		}
	}
}

func DefaultAzureOptions() AzureOptions {
	return AzureOptions{
		ChunkSize:       defaultChunkSize,
		DisableChecksum: _true,
	}
}

// AutoFill sets region (if empty) from identity service, it only works when
// running in AWS.
func (o *AzureOptions) AutoFill() {
	if o.Account == "" {
		o.UseMsi = _true
	}
}
