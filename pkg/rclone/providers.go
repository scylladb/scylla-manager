// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/scylladb/go-reflectx"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/rclone/backend/localdir"
	"go.uber.org/multierr"
)

var providers = strset.New()

// HasProvider returns true iff provider was registered.
func HasProvider(name string) bool {
	return providers.Has(name)
}

// RegisterLocalDirProvider must be called before server is started.
// It allows for adding dynamically adding localdir providers.
func RegisterLocalDirProvider(name, description, rootDir string) error {
	if _, err := os.Stat(rootDir); os.IsNotExist(err) {
		return errors.Wrapf(err, "register local dir provider %s", rootDir)
	}

	localdir.Init(name, description, rootDir)

	errs := multierr.Combine(
		fs.ConfigFileSet(name, "type", name),
		fs.ConfigFileSet(name, "disable_checksum", "true"),
	)
	if errs != nil {
		return errors.Wrapf(errs, "register localdir provider %s", name)
	}
	fs.Infof(nil, "registered localdir provider [name=%s, root=%s]", name, rootDir)

	providers.Add(name)

	return nil
}

// MustRegisterLocalDirProvider calls RegisterLocalDirProvider and panics on
// error.
func MustRegisterLocalDirProvider(name, description, rootDir string) {
	if err := RegisterLocalDirProvider(name, description, rootDir); err != nil {
		panic(err)
	}
}

// S3Options represents a selected subset of rclone S3 backend options for
// togged with yaml for inclusion in config objects.
type S3Options struct {
	AccessKeyID           string `yaml:"access_key_id"`
	SecretAccessKey       string `yaml:"secret_access_key"`
	Provider              string `yaml:"provider"`
	Region                string `yaml:"region"`
	Endpoint              string `yaml:"endpoint"`
	ServerSideEncryption  string `yaml:"server_side_encryption"`
	SSEKMSKeyID           string `yaml:"sse_kms_key_id"`
	UploadConcurrency     string `yaml:"upload_concurrency"`
	ChunkSize             string `yaml:"chunk_size"`
	UseAccelerateEndpoint string `yaml:"use_accelerate_endpoint"`
}

var s3Providers = strset.New(
	"AWS", "Minio", "Alibaba", "Ceph", "DigitalOcean",
	"IBMCOS", "Wasabi", "Dreamhost", "Netease", "Other",
)

// Validate returns error if option values are not set properly.
func (opts *S3Options) Validate() error {
	if opts.Endpoint != "" && opts.Provider == "" {
		return fmt.Errorf("specify provider for the endpoint %s, available providers are: %s", opts.Endpoint, s3Providers)
	}

	if opts.Provider != "" && !s3Providers.Has(opts.Provider) {
		return fmt.Errorf("unknown provider: %s", opts.Provider)
	}

	return nil
}

const (
	// In order to reduce memory footprint, by default we allow at most two
	// concurrent requests.
	// upload_concurrency * chunk_size gives rough estimate how much upload
	// buffers will be allocated.
	defaultUploadConcurrency = 2

	// Default value of 5MB caused that we encountered problems with S3
	// returning 5xx. In order to reduce number of requests to S3, we are
	// increasing chunk size by ten times, which should decrease number of
	// requests by ten times.
	defaultChunkSize = "50M"
)

// RegisterS3Provider must be called before server is started.
// It allows for adding dynamically adding s3 provider named s3.
func RegisterS3Provider(opts S3Options) error {
	const name = "s3"

	if err := opts.Validate(); err != nil {
		return err
	}

	if opts.Provider == "" {
		opts.Provider = "AWS"
	}

	// Auto set region if needed
	if opts.Region == "" && opts.Endpoint == "" {
		opts.Region = awsRegionFromMetadataAPI()
	}

	if opts.UploadConcurrency == "" {
		opts.UploadConcurrency = strconv.Itoa(defaultUploadConcurrency)
	}

	if opts.ChunkSize == "" {
		opts.ChunkSize = defaultChunkSize
	}

	// Set common properties
	errs := multierr.Combine(
		fs.ConfigFileSet(name, "type", "s3"),
		fs.ConfigFileSet(name, "provider", opts.Provider),
		fs.ConfigFileSet(name, "env_auth", "true"),
		fs.ConfigFileSet(name, "disable_checksum", "true"),
		fs.ConfigFileSet(name, "list_chunk", "200"),
		// Because of access denied issues with Minio.
		// see https://github.com/rclone/rclone/issues/4633
		fs.ConfigFileSet(name, "no_check_bucket", "true"),

		registerProvider(name, opts),
	)
	// Check for errors
	if errs != nil {
		return errors.Wrap(errs, "register provider")
	}

	return nil
}

// MustRegisterS3Provider calls RegisterS3Provider and panics on error.
func MustRegisterS3Provider(provider, endpoint, accessKeyID, secretAccessKey string) {
	if err := RegisterS3Provider(S3Options{
		Provider:        provider,
		Endpoint:        endpoint,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}); err != nil {
		panic(err)
	}
}

// GCSOptions represents a selected subset of rclone GCS backend options for
// togged with yaml for inclusion in config objects.
type GCSOptions struct {
	ServiceAccountFile string `yaml:"service_account_file"`
	ChunkSize          string `yaml:"chunk_size"`
}

// RegisterGCSProvider must be called before server is started.
// It allows for adding dynamically adding gcs provider named gcs.
func RegisterGCSProvider(opts GCSOptions) error {
	const (
		name               = "gcs"
		serviceAccountPath = "/etc/scylla-manager-agent/gcs-service-account.json"
	)

	if opts.ChunkSize == "" {
		opts.ChunkSize = defaultChunkSize
	}

	if opts.ServiceAccountFile == "" {
		if _, err := os.Stat(serviceAccountPath); err == nil {
			opts.ServiceAccountFile = serviceAccountPath
		}
	}

	err := multierr.Combine(
		fs.ConfigFileSet(name, "type", name),
		// Disable bucket creation if it doesn't exists
		fs.ConfigFileSet(name, "allow_create_bucket", "false"),
		// This option must be true if we don't want rclone to set permission on
		// each object. Permissions will be controlled by the ACL rules
		// for fine-grained buckets, and IAM bucket-level settings for uniform buckets.
		fs.ConfigFileSet(name, "bucket_policy_only", "true"),

		registerProvider(name, opts),
	)
	if err != nil {
		return errors.Wrap(err, "configure provider")
	}

	return nil
}

// AzureOptions represents a selected subset of rclone Azure backend options
// for togged with yaml for inclusion in config objects.
type AzureOptions struct {
	Account   string `yaml:"account"`
	Key       string `yaml:"key"`
	ChunkSize string `yaml:"chunk_size"`
}

// RegisterAzureProvider must be called before server is started.
// It allows for adding dynamically adding gcs provider named gcs.
func RegisterAzureProvider(opts AzureOptions) error {
	const name = "azure"

	if opts.ChunkSize == "" {
		opts.ChunkSize = defaultChunkSize
	}

	err := multierr.Combine(
		fs.ConfigFileSet(name, "type", "azureblob"),
		func() error {
			if opts.Account != "" && opts.Key != "" {
				return nil
			}
			return fs.ConfigFileSet(name, "use_msi", "true")
		}(),

		registerProvider(name, opts),
	)
	if err != nil {
		return errors.Wrap(err, "configure provider")
	}

	return nil
}

func registerProvider(name string, options interface{}) error {
	var (
		m     = reflectx.NewMapper("yaml").FieldMap(reflect.ValueOf(options))
		extra = []string{"name=" + name}
		errs  error
	)
	for key, rval := range m {
		if s := rval.String(); s != "" {
			errs = multierr.Append(errs, fs.ConfigFileSet(name, key, s))
			if strings.Contains(key, "secret") || strings.Contains(key, "key") {
				extra = append(extra, key+"="+strings.Repeat("*", len(s)))
			} else {
				extra = append(extra, key+"="+s)
			}
		}
	}

	// Check for errors
	if errs != nil {
		return errors.Wrapf(errs, "register %s provider", name)
	}

	providers.Add(name)
	fs.Infof(nil, "registered %s provider [%s]", name, strings.Join(extra, ", "))

	return nil
}
