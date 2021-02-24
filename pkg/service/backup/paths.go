// Copyright (C) 2017 ScyllaDB

package backup

import (
	"os"
	"path"
	"strings"

	"github.com/scylladb/scylla-manager/pkg/backup"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
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

func remoteManifestLevel(baseDir string) int {
	a := len(strings.Split(backup.RemoteManifestDir(uuid.Nil, "a", "b"), sep))
	b := len(strings.Split(baseDir, sep))
	return a - b
}

func ssTablePathWithKeyspacePrefix(keyspace, table, version, name string) string {
	return path.Join(
		"keyspace", keyspace,
		"table", table,
		version,
		name,
	)
}
