// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/scylladb/go-reflectx"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/rclone/backend/localdir"
	"go.uber.org/multierr"
)

// ErrNotFound is returned when remote call is not available.
var ErrNotFound = errors.New("not found")

// RegisterInMemoryConf registers global items for configuring backend
// providers in rclone.
// Function is idempotent.
// Has to be called again to refresh AWS_S3_ENDPOINT value.
func RegisterInMemoryConf() error {
	c := &inMemoryConf{}
	// Set inMemoryConf as default handler for rclone/fs configuration.
	fs.ConfigFileGet = c.Get
	fs.ConfigFileSet = c.Set
	fs.Debugf(nil, "registered in-memory config")

	return nil
}

// MustRegisterInMemoryConf calls RegisterInMemoryConf and panics on error.
func MustRegisterInMemoryConf() {
	if err := RegisterInMemoryConf(); err != nil {
		panic(err)
	}
}

// inMemoryConf is in-memory implementation of rclone configuration for remote file
// systems.
type inMemoryConf struct {
	mu       sync.Mutex
	sections map[string]map[string]string
}

// Get config key under section returning the the value and true if found or
// ("", false) otherwise.
func (c *inMemoryConf) Get(section, key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sections == nil {
		return "", false
	}
	s, ok := c.sections[section]
	if !ok {
		return "", false
	}
	v, ok := s[key]
	return v, ok
}

// Set the key in section to value.
// It doesn't save the config file.
func (c *inMemoryConf) Set(section, key, value string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sections == nil {
		c.sections = make(map[string]map[string]string)
	}
	s, ok := c.sections[section]
	if !ok {
		s = make(map[string]string)
	}
	if value == "" {
		delete(c.sections[section], value)
	} else {
		s[key] = value
		c.sections[section] = s
	}
	return
}

var providers = strset.New()

// RegisterLocalDirProvider must be called before server is started, and after
// RegisterInMemoryConf is called. It allows for adding dynamically adding
// localdir providers.
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
	Region                string `yaml:"region"`
	Endpoint              string `yaml:"endpoint"`
	ServerSideEncryption  string `yaml:"server_side_encryption"`
	SSEKMSKeyID           string `yaml:"sse_kms_key_id"`
	UploadConcurrency     string `yaml:"upload_concurrency"`
	UseAccelerateEndpoint string `yaml:"use_accelerate_endpoint"`
}

// RegisterS3Provider must be called before server is started, and after
// RegisterInMemoryConf is called. It allows for adding dynamically adding
// s3 provider named s3.
func RegisterS3Provider(opts S3Options) error {
	const name = "s3"

	// Auto set region if needed
	if opts.Region == "" && opts.Endpoint == "" {
		opts.Region = awsRegionFromMetadataAPI()
	}

	// Auto set upload concurrency, we assume here that with big nodes comes
	// more RAM and fast network cards and so we can increase the number of
	// streams in multipart upload.
	if opts.UploadConcurrency == "" {
		cpus := runtime.NumCPU()
		if cpus > 4 {
			opts.UploadConcurrency = strconv.Itoa(cpus)
		}
	}

	// Set common properties
	errs := multierr.Combine(
		fs.ConfigFileSet(name, "type", "s3"),
		fs.ConfigFileSet(name, "provider", "AWS"),
		fs.ConfigFileSet(name, "env_auth", "true"),
		fs.ConfigFileSet(name, "disable_checksum", "true"),
	)

	// Set custom properties
	var (
		m     = reflectx.NewMapper("yaml").FieldMap(reflect.ValueOf(opts))
		extra = []string{"name=" + name}
	)
	for key, rval := range m {
		if s := rval.String(); s != "" {
			errs = multierr.Append(errs, fs.ConfigFileSet(name, key, s))
			if key == "secret_access_key" {
				extra = append(extra, key+"="+strings.Repeat("*", len(s)))
			} else {
				extra = append(extra, key+"="+s)
			}
		}
	}

	// Check for errors
	if errs != nil {
		return errors.Wrapf(errs, "register s3 provider")
	}
	fs.Infof(nil, "registered s3 provider [%s]", strings.Join(extra, ", "))

	providers.Add(name)

	return nil
}

// MustRegisterS3Provider calls RegisterS3Provider and panics on error.
func MustRegisterS3Provider(endpoint, accessKeyID, secretAccessKey string) {
	if err := RegisterS3Provider(S3Options{
		Endpoint:        endpoint,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
	}); err != nil {
		panic(err)
	}
}
