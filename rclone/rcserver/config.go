// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
	"github.com/scylladb/go-set/strset"
	"go.uber.org/multierr"
)

const (
	// S3Provider is the name of the AWS S3 file system provided by rclone.
	S3Provider = "s3"
	// DataProvider is the name of the local file system overridden by mermaid.
	DataProvider = "data"
)

var (
	// Set of all allowed/supported providers.
	providers = strset.New(S3Provider, DataProvider)

	// ErrNotFound is returned when remote call is not available.
	ErrNotFound = errors.New("not found")
)

// RegisterInMemoryConf registers global items for configuring backend
// providers in rclone.
// Function is idempotent.
// Has to be called again to refresh AWS_S3_ENDPOINT value.
func RegisterInMemoryConf() {
	c := &inMemoryConf{}
	// Set inMemoryConf as default handler for rclone/fs configuration.
	fs.ConfigFileGet = c.Get
	fs.ConfigFileSet = c.Set

	// Disable config manipulation over remote calls.
	rc.Calls.Get("config/create").Fn = notFoundFn
	rc.Calls.Get("config/get").Fn = notFoundFn
	rc.Calls.Get("config/providers").Fn = notFoundFn
	rc.Calls.Get("config/delete").Fn = notFoundFn

	// Register providers.
	errs := multierr.Combine(
		fs.ConfigFileSet(S3Provider, "type", "s3"),
		fs.ConfigFileSet(S3Provider, "provider", "AWS"),
		fs.ConfigFileSet(S3Provider, "env_auth", "true"),
		fs.ConfigFileSet(S3Provider, "disable_checksum", "true"),
		fs.ConfigFileSet(S3Provider, "endpoint", os.Getenv("AWS_S3_ENDPOINT")),

		fs.ConfigFileSet(DataProvider, "type", "data"),
		fs.ConfigFileSet(DataProvider, "disable_checksum", "true"),
	)
	if errs != nil {
		panic(fmt.Sprintf("failed to register backend providers: %+v", errs))
	}
	fs.Debugf(nil, "registered in-memory config")
}

func notFoundFn(ctx context.Context, in rc.Params) (rc.Params, error) {
	return rc.Params{}, ErrNotFound
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
