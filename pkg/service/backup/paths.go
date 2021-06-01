// Copyright (C) 2017 ScyllaDB

package backup

const (
	dataDir = "data:"

	scyllaManifest = "manifest.json"
	scyllaSchema   = "schema.cql"
)

func keyspaceDir(keyspace string) string {
	return dataDir + keyspace
}
