// Copyright (C) 2017 ScyllaDB

package backup

import "path"

const (
	dataDir = "data:"

	scyllaManifest = "manifest.json"
	scyllaSchema   = "schema.cql"
)

func keyspaceDir(keyspace string) string {
	return dataDir + keyspace
}

func uploadTableDir(keyspace, table, version string) string {
	return path.Join(
		keyspaceDir(keyspace),
		table+"-"+version,
		"upload",
	)
}
