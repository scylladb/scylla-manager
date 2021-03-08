// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"fmt"
	"sync"

	"github.com/rclone/rclone/fs"
	"github.com/scylladb/scylla-manager/pkg"
)

// GetConfig returns the rclone global config.
func GetConfig() *fs.ConfigInfo {
	return fs.GetConfig(nil) // nolint: staticcheck
}

// InitFsConfig enables in-memory config and sets default config values
// expected for correct agent behaviour.
func InitFsConfig() {
	initInMemoryConfig()

	c := GetConfig()

	// Don't use JSON log format in logging.
	c.UseJSONLog = false
	// Pass all logs, our logger decides which one to print.
	c.LogLevel = fs.LogLevelDebug

	// With this option set, files will be created and deleted as requested,
	// but existing files will never be updated. If an existing file does not
	// match between the source and destination, rclone will give the error
	// Source and destination exist but do not match: immutable file modified.
	c.Immutable = false
	// Skip post copy check of checksums.
	c.IgnoreChecksum = true
	// Skip based on size only, not mod-time or checksum.
	c.SizeOnly = true
	// Don't update destination mod-time if files identical.
	c.NoUpdateModTime = true

	// Number of low level retries to do. (default 10)
	// This applies to operations like S3 chunk upload.
	c.LowLevelRetries = 20

	// Delete even if there are I/O errors.
	c.IgnoreErrors = true
	// Maximum number of stats groups to keep in memory. On max oldest is discarded. (default 1000).
	c.MaxStatsGroups = 1000
	// Set proper agent for backend clients.
	c.UserAgent = UserAgent()
}

func initInMemoryConfig() {
	c := new(inMemoryConf)
	fs.ConfigFileGet = c.Get
	fs.ConfigFileSet = c.Set
	fs.Infof(nil, "registered in-memory fs config")
}

// inMemoryConf is in-memory implementation of rclone configuration for
// remote file systems.
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

// UserAgent returns string value that can be used as identifier in client
// calls to the service providers.
func UserAgent() string {
	return fmt.Sprintf("Scylla Manager Agent %s", pkg.Version())
}
