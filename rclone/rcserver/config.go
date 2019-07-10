// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"context"
	"fmt"
	"sync"

	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fs/rc"
)

func init() {
	registerInMemoryConf()
}

func registerInMemoryConf() {
	c := &inMemoryConf{}
	// Set inMemoryConf as default handler for rclone/fs configuration.
	fs.ConfigFileGet = c.Get
	fs.ConfigFileSet = c.Set

	call := rc.Calls.Get("config/create")
	call.Fn = func(ctx context.Context, in rc.Params) (rc.Params, error) {
		name, err := in.GetString("name")
		if err != nil {
			return nil, err
		}
		parameters := rc.Params{}
		err = in.GetStruct("parameters", &parameters)
		if err != nil {
			return nil, err
		}
		remoteType, err := in.GetString("type")
		if err != nil {
			return nil, err
		}
		c.Set(name, "type", remoteType)
		for k, v := range parameters {
			c.Set(name, k, fmt.Sprintf("%v", v))
		}
		return nil, nil
	}
	call = rc.Calls.Get("config/get")
	call.Fn = func(ctx context.Context, in rc.Params) (rc.Params, error) {
		name, err := in.GetString("name")
		if err != nil {
			return nil, err
		}
		section, ok := c.sections[name]
		if !ok {
			return nil, fmt.Errorf("unknown name %q", name)
		}
		params := rc.Params{}
		for key, val := range section {
			params[key] = val
		}
		return params, nil
	}

	fs.Debugf(nil, "config: registered in-memory config")
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
