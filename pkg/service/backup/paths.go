// Copyright (C) 2017 ScyllaDB

package backup

import (
	"path"

	. "github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
)

const (
	dataDir = "data:"

	scyllaManifest = "manifest.json"
	scyllaSchema   = "schema.cql"
)

func keyspaceDir(keyspace string) string {
	return dataDir + keyspace
}

func uploadTableDir(f FilesMeta) string {
	return path.Join(
		keyspaceDir(f.Keyspace),
		f.Table+"-"+f.Version,
		"upload",
	)
}
