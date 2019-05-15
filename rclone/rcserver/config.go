// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"sync"

	"github.com/ncw/rclone/fs"
)

func init() {
	registerInMemoryConf()
}

func registerInMemoryConf() {
	c := &inMemoryConf{}
	// Set inMemoryConf as default handler for rclone/fs configuration.
	fs.ConfigFileGet = c.GetFlag
	fs.ConfigFileSet = c.Set
}

// inMemoryConf is in-memory implementation of rclone configuration for remote file
// systems.
type inMemoryConf struct {
	mu       sync.Mutex
	sections map[string]map[string]string
}

// GetFlag gets the config key under section returning the
// the value and true if found and or ("", false) otherwise.
func (c *inMemoryConf) GetFlag(section, key string) (string, bool) {
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

// Set sets the key in section to value.  It doesn't save
// the config file.
func (c *inMemoryConf) Set(section, key, value string) {
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
		return
	}
	s[key] = value
	c.sections[section] = s
}
