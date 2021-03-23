// Copyright (C) 2017 ScyllaDB

package downloader

import (
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/util/inexlist/ksfilter"
)

type Option func(d *Downloader) error

// WithKeyspace sets the keyspace/table filters.
func WithKeyspace(filters []string) Option {
	return func(d *Downloader) error {
		ksf, err := ksfilter.NewFilter(filters)
		if err != nil {
			return errors.Wrap(err, "keyspace/table filter")
		}
		d.keyspace = ksf
		return nil
	}
}

// WithClearTables would delete any data forom a table before downloading new
// files. It does not work with SSTableLoaderTableDirMode mode.
func WithClearTables() Option {
	return func(d *Downloader) error {
		d.clearTables = true
		return nil
	}
}

// WithTableDirMode specifies type of resulting directory structure.
func WithTableDirMode(mode TableDirMode) Option {
	return func(d *Downloader) error {
		d.mode = mode
		return nil
	}
}

// WithDryRun turns on the dry-run mode where no data operations are performed.
func WithDryRun() Option {
	return func(d *Downloader) error {
		d.dryRun = true
		return nil
	}
}
