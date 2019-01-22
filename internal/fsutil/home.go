// Copyright (C) 2017 ScyllaDB

package fsutil

import (
	"os"
	"os/user"
)

// HomeDir returns the home directory for current user or value of HOME
// environment variable if not implemented.
func HomeDir() string {
	usr, err := user.Current()
	if err == nil {
		return usr.HomeDir
	}
	return os.Getenv("HOME")
}
