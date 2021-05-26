// Copyright (C) 2017 ScyllaDB

package backup

import (
	"os"
)

const dataDir = "data:"

func keyspaceDir(keyspace string) string {
	return dataDir + keyspace
}

const (
	scyllaManifest = "manifest.json"
	scyllaSchema   = "schema.cql"

	sep = string(os.PathSeparator)
)
