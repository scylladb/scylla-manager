// Copyright (C) 2017 ScyllaDB

package backup

import "testing"

func NewSnapshotTag() string {
	return newSnapshotTag()
}

func SnapshotTagFromManifestPath(t *testing.T, s string) string {
	var m remoteManifest
	if err := m.ParsePartialPath(s); err != nil {
		t.Fatal(t)
	}
	return m.SnapshotTag
}

func ParsePartialPath(s string) error {
	var m remoteManifest
	return m.ParsePartialPath(s)
}
