// Copyright (C) 2017 ScyllaDB

// Package data is rclone backend based on local backend provided by rclone.
// The difference from local is that data is always rooted to the scylla data
// directory.
package data

import (
	"path/filepath"

	"github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
)

const (
	linkSuffix = ".rclonelink"
)

var (
	// RootDir represents absolute path under which data: file system will
	// operate.
	RootDir = "/var/lib/scylla/data"
)

// Register with Fs
func init() {
	fsi := &fs.RegInfo{
		Name:        "data",
		Description: "Jailed Scylla data",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name: "nounc",
			Help: "Disable UNC (long path names) conversion on Windows",
			Examples: []fs.OptionExample{{
				Value: "true",
				Help:  "Disables long file names",
			}},
		}, {
			Name:     "copy_links",
			Help:     "Follow symlinks and copy the pointed to item.",
			Default:  false,
			NoPrefix: true,
			ShortOpt: "L",
			Advanced: true,
		}, {
			Name:     "links",
			Help:     "Translate symlinks to/from regular files with a '" + linkSuffix + "' extension",
			Default:  false,
			NoPrefix: true,
			ShortOpt: "l",
			Advanced: true,
		}, {
			Name: "skip_links",
			Help: `Don't warn about skipped symlinks.
This flag disables warning messages on skipped symlinks or junction
points, as you explicitly acknowledge that they should be skipped.`,
			Default:  false,
			NoPrefix: true,
			Advanced: true,
		}, {
			Name: "no_unicode_normalization",
			Help: `Don't apply unicode normalization to paths and filenames (Deprecated)

This flag is deprecated now.  Rclone no longer normalizes unicode file
names, but it compares them with unicode normalization in the sync
routine instead.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "no_check_updated",
			Help: `Don't check to see if the files change during upload

Normally rclone checks the size and modification time of files as they
are being uploaded and aborts with a message which starts "can't copy
- source file is being updated" if the file changes during upload.

However on some file systems this modification time check may fail (eg
[Glusterfs #2206](https://github.com/rclone/rclone/issues/2206)) so this
check can be disabled with this flag.`,
			Default:  false,
			Advanced: true,
		}, {
			Name:     "one_file_system",
			Help:     "Don't cross filesystem boundaries (unix/macOS only).",
			Default:  false,
			NoPrefix: true,
			ShortOpt: "x",
			Advanced: true,
		}, {
			Name: "case_sensitive",
			Help: `Force the filesystem to report itself as case sensitive.

Normally the local backend declares itself as case insensitive on
Windows/macOS and case sensitive for everything else.  Use this flag
to override the default choice.`,
			Default:  false,
			Advanced: true,
		}, {
			Name: "case_insensitive",
			Help: `Force the filesystem to report itself as case insensitive

Normally the local backend declares itself as case insensitive on
Windows/macOS and case sensitive for everything else.  Use this flag
to override the default choice.`,
			Default:  false,
			Advanced: true,
		}},
	}
	fs.Register(fsi)
}

func NewFs(name, root string, m configmap.Mapper) (fs.Fs, error) {
	r := filepath.Join(RootDir, filepath.Clean(root))
	return local.NewFs(name, r, m)
}
