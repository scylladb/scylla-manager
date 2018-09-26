// Copyright (C) 2017 ScyllaDB

package main

import (
	"os"
	"os/user"
	"path"
)

func redirectStdStreams() (*os.File, error) {
	p := "/tmp/scylla-manager.stdout"

	if u, err := user.Current(); err == nil && u.HomeDir != "" {
		p = path.Join(u.HomeDir, "stdout")
	}

	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	os.Stdout = f
	os.Stderr = f
	return f, nil
}
