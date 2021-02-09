// Copyright (C) 2017 ScyllaDB

package backup

import (
	"github.com/scylladb/scylla-manager/pkg/rclone"
)

// MustRegisterLocalDirProvider registers local directory provider and adds it
// to the internal providers list.
func MustRegisterLocalDirProvider(name, description, rootDir string) {
	rclone.MustRegisterLocalDirProvider(name, description, rootDir)
	providers.Add(name)
}
