// Copyright (C) 2017 ScyllaDB

package backup

import (
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/pkg/errors"
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

	sep         = string(os.PathSeparator)
	tempFileExt = ".tmp"
)

func remoteManifestLevel(baseDir string) int {
	a := len(strings.Split(backup.RemoteManifestDir(uuid.Nil, "a", "b"), sep))
	b := len(strings.Split(baseDir, sep))
	return a - b
}

func tempFile(f string) string {
	return f + tempFileExt
}

// Adapted from Scylla's sstable detection code
// https://github.com/scylladb/scylla/blob/bb2e04cc8b8152bbe11749d79f0f136335c77602/sstables/sstables.cc#L2724
var (
	laMcFileNameRe                = `(?:la|mc)-\d+-\w+(-.*)`
	kaFileNameRe                  = `\w+-\w+-ka-\d+(-.*)`
	keyspaceTableNameRe           = `[a-zA-Z0-9_]+`
	tableVersionRe                = `[a-f0-9]{32}`
	keyspaceTableVersionPatternRe = `^keyspace/` + keyspaceTableNameRe + `/table/` + keyspaceTableNameRe + `/` + tableVersionRe + `/`

	keyspaceTableVersionLaMcRe     = regexp.MustCompile(keyspaceTableVersionPatternRe + laMcFileNameRe)
	keyspaceTableVersionKaRe       = regexp.MustCompile(keyspaceTableVersionPatternRe + kaFileNameRe)
	keyspaceTableVersionManifestRe = regexp.MustCompile(keyspaceTableVersionPatternRe + scyllaManifest)
)

// groupingKey returns key which can be used for grouping SSTable files.
// SSTable representation in snapshot consists of few files sharing same prefix,
// this key allows to group these files.
func groupingKey(path string) (string, error) {
	m := keyspaceTableVersionLaMcRe.FindStringSubmatch(path)
	if m != nil {
		return strings.TrimSuffix(path, m[1]), nil
	}
	m = keyspaceTableVersionKaRe.FindStringSubmatch(path)
	if m != nil {
		return strings.TrimSuffix(path, m[1]), nil
	}
	m = keyspaceTableVersionManifestRe.FindStringSubmatch(path)
	if m != nil {
		return path, nil
	}

	return "", errors.New("file path does not match sstable patterns")
}

func ssTablePathWithKeyspacePrefix(keyspace, table, version, name string) string {
	return path.Join(
		"keyspace", keyspace,
		"table", table,
		version,
		name,
	)
}
