// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"os"
	"reflect"
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

// RegisterS3Provider must be called before server is started.
// It allows for adding dynamically adding s3 provider named s3.
func RegisterS3Provider(opts S3Options) error {
	const name = "s3"

	opts.AutoFill()
	if err := opts.Validate(); err != nil {
		return err
	}

	// Set common properties
	errs := multierr.Combine(
		fs.ConfigFileSet(name, "type", "s3"),
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
	opts := DefaultS3Options()
	opts.Provider = provider
	opts.Endpoint = endpoint
	opts.AccessKeyID = accessKeyID
	opts.SecretAccessKey = secretAccessKey

	if err := RegisterS3Provider(opts); err != nil {
		panic(err)
	}
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
