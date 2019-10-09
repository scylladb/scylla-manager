// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"context"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/rclone/backend/localdir"
	"go.uber.org/multierr"
)

const (
	// S3Provider is the name of the AWS S3 file system provided by rclone.
	S3Provider = "s3"
)

var (
	// Set of all allowed/supported providers.
	providers = strset.New(S3Provider)
)

// MustRegisterLocalDirProvider calls RegisterLocalDirProvider and panics on
// error.
func MustRegisterLocalDirProvider(name, description, rootDir string) {
	if err := RegisterLocalDirProvider(name, description, rootDir); err != nil {
		panic(err)
	}
}

// RegisterLocalDirProvider must be called before server is started, and after
// RegisterInMemoryConf is called. It allows for adding dynamically adding
// localdir providers.
func RegisterLocalDirProvider(name, description, rootDir string) error {
	localdir.Init(name, description, rootDir)

	errs := multierr.Combine(
		fs.ConfigFileSet(name, "type", name),
		fs.ConfigFileSet(name, "disable_checksum", "true"),
	)
	if errs != nil {
		return errors.Wrapf(errs, "failed to register localdir provider %s", name)
	}
	fs.Infof(nil, "registered localdir provider %s rooted at %s", name, rootDir)

	providers.Add(name)

	return nil
}

// MustRegisterInMemoryConf calls RegisterInMemoryConf and panics on error.
func MustRegisterInMemoryConf() {
	if err := RegisterInMemoryConf(); err != nil {
		panic(err)
	}
}

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

	// Disable config manipulation over remote calls
	rc.Calls.Get("config/create").Fn = notFoundFn
	rc.Calls.Get("config/get").Fn = notFoundFn
	rc.Calls.Get("config/providers").Fn = notFoundFn
	rc.Calls.Get("config/delete").Fn = notFoundFn

	// Register s3 provider
	errs := multierr.Combine(
		fs.ConfigFileSet(S3Provider, "type", "s3"),
		fs.ConfigFileSet(S3Provider, "provider", "AWS"),
		fs.ConfigFileSet(S3Provider, "env_auth", "true"),
		fs.ConfigFileSet(S3Provider, "disable_checksum", "true"),
		fs.ConfigFileSet(S3Provider, "endpoint", os.Getenv("AWS_S3_ENDPOINT")),
	)
	if errs != nil {
		return errors.Wrapf(errs, "failed to register s3 provider")
	}
	fs.Infof(nil, "registered s3 provider")

	return nil
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

// ErrNotFound is returned when remote call is not available.
var ErrNotFound = errors.New("not found")

func notFoundFn(ctx context.Context, in rc.Params) (rc.Params, error) {
	return rc.Params{}, ErrNotFound
}
