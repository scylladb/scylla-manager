// Copyright (C) 2023 ScyllaDB

package backupspec

import (
	"context"
	"path"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

// Issue #3288 showed that we need to be able to store multiple different SSTables
// with the same name and from the same node ID. In order to do that, we use rclone
// 'suffix' option to rename otherwise overwritten files during upload.
// Choosing snapshot tag as the suffix allows us to determine when to purge/restore versioned files.

// VersionedSSTable represents version of SSTable that we still need to store in a backup.
// (e.g. older version of 'md-2-big-Data.db' could be 'md-2-big-Data.db.sm_20230114183231UTC')
// Note, that the newest version of SSTable does not have snapshot tag extension.
type VersionedSSTable struct {
	Name string // Original SSTable name (e.g. md-2-big-Data.db)
	// Snapshot tag extension representing backup that introduced newer version of this SSTable (e.g. sm_20230114183231UTC).
	// Empty string for the newest version.
	Version string
	Size    int64
}

// NewVersionedSStable creates VersionedSSTable from listed item.
func NewVersionedSStable(item *scyllaclient.RcloneListDirItem) VersionedSSTable {
	ext := path.Ext(item.Name)
	if ext == "" || !IsSnapshotTag(ext[1:]) {
		return VersionedSSTable{
			Name: item.Name,
			Size: item.Size,
		}
	}

	return VersionedSSTable{
		Name:    strings.TrimSuffix(item.Name, ext),
		Version: ext[1:],
		Size:    item.Size,
	}
}

// FullName returns versioned file name.
func (vt VersionedSSTable) FullName() string {
	if vt.Version == "" {
		return vt.Name
	}
	return vt.Name + "." + vt.Version
}

// VersionedMap maps SSTable name to its versions with respect to currently restored snapshot tag.
type VersionedMap map[string]VersionedSSTable

// VersionedFileExt returns the snapshot tag extension of versioned file.
// If using alongside with RcloneMoveDir or RcloneCopyDir as suffix option,
// this extension will be added to files otherwise overwritten or deleted in the process.
func VersionedFileExt(snapshotTag string) string {
	return "." + snapshotTag
}

// IsVersionedFileRemovable checks if versioned file is redundant because of its newer versions.
// In order to decide that, the time of the oldest stored backup is required.
func IsVersionedFileRemovable(oldest time.Time, versioned string) (bool, error) {
	ext := path.Ext(versioned)
	// Don't remove the newest versions
	if ext == "" || !IsSnapshotTag(ext[1:]) {
		return false, nil
	}

	t, err := SnapshotTagTime(ext[1:])
	if err != nil {
		return false, err
	}
	// Versioned file can only belong to backups STRICTLY older than itself.
	// If it is older (or equally old) to the oldest, currently stored backup in remote location, it can be deleted.
	if !t.After(oldest) {
		return true, nil
	}
	return false, nil
}

// ListVersionedFiles gathers information about versioned files from specified dir.
func ListVersionedFiles(ctx context.Context, client *scyllaclient.Client, snapshotTag, host, dir string) (VersionedMap, error) {
	versionedFiles := make(VersionedMap)
	allVersions := make(map[string][]VersionedSSTable)

	opts := &scyllaclient.RcloneListDirOpts{
		FilesOnly: true,
	}
	f := func(item *scyllaclient.RcloneListDirItem) {
		v := NewVersionedSStable(item)
		allVersions[v.Name] = append(allVersions[v.Name], v)
	}

	if err := client.RcloneListDirIter(ctx, host, dir, opts, f); err != nil {
		return nil, errors.Wrapf(err, "host %s: listing versioned files", host)
	}

	restoreT, err := SnapshotTagTime(snapshotTag)
	if err != nil {
		return nil, err
	}
	futureT := time.Unix(1<<60, 0)
	// Chose correct version with respect to currently restored snapshot tag
	for _, versions := range allVersions {
		var (
			candidate VersionedSSTable
			candT     = time.Time{}
		)
		for _, v := range versions {
			tagT := futureT
			if v.Version != "" {
				tagT, err = SnapshotTagTime(v.Version)
				if err != nil {
					return nil, err
				}
			}

			if tagT.After(restoreT) {
				if candT.IsZero() || tagT.Before(candT) {
					candidate = v
					candT = tagT
				}
			}
		}

		versionedFiles[candidate.Name] = candidate
	}

	return versionedFiles, nil
}
