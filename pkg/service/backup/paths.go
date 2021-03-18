// Copyright (C) 2017 ScyllaDB

package backup

import (
	"os"
	"path"
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

func ssTablePathWithKeyspacePrefix(keyspace, table, version, name string) string {
	return path.Join(
		"keyspace", keyspace,
		"table", table,
		version,
		name,
	)
}
