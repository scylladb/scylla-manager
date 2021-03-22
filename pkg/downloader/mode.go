// Copyright (C) 2017 ScyllaDB

package downloader

import "github.com/pkg/errors"

// TableDirMode specifies type of desired download.
type TableDirMode string

const (
	// DefaultTableDirMode is intended for normal on node download when Scylla
	// server is shutdown. It downloads files directly to versioned table
	// directory.
	DefaultTableDirMode TableDirMode = ""
	// UploadTableDirMode uses upload dir inside versioned table directory.
	// It should be used for downloads when Scylla server is running.
	UploadTableDirMode TableDirMode = "upload"
	// SSTableLoaderTableDirMode is intended for use with sstableloader
	// where the resulting data structure it keyspace/table/files.
	SSTableLoaderTableDirMode TableDirMode = "sstableloader"
)

func (t TableDirMode) String() string {
	return string(t)
}

func (t TableDirMode) MarshalText() (text []byte, err error) {
	return []byte(t), nil
}

func (t *TableDirMode) UnmarshalText(text []byte) error {
	switch TableDirMode(text) {
	case DefaultTableDirMode:
		*t = DefaultTableDirMode
	case UploadTableDirMode:
		*t = UploadTableDirMode
	case SSTableLoaderTableDirMode:
		*t = SSTableLoaderTableDirMode
	default:
		return errors.Errorf("unsupported table dir mode")
	}

	return nil
}
