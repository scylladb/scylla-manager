// Copyright (C) 2017 ScyllaDB

package swagger

import (
	"embed"
	"io/fs"
)

//go:generate ./update.sh
//go:embed dist
var dist embed.FS

// UI returns a file system containing swagger UI with loaded Scylla Manager spec.
func UI() fs.FS {
	f, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	return f
}
