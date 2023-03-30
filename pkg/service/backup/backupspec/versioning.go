// Copyright (C) 2023 ScyllaDB

package backupspec

import (
	"path"
	"strings"
	"time"
)

// Issue #3288 showed that we need to be able to store multiple different SSTables
// with the same name and from the same node ID. In order to do that, we use rclone
// 'suffix' option to rename otherwise overwritten files during upload.
// Choosing snapshot tag as the suffix allows us to determine when to purge/restore versioned files.

// VersionedSSTable represents older version of SSTable that we still need to store in a backup.
// (e.g. older version of 'md-2-big-Data.db' could be 'md-2-big-Data.db.sm_20230114183231UTC')
// Note, that the newest version of SSTable does not have snapshot tag extension.
type VersionedSSTable struct {
	Name    string // Original SSTable name (e.g. md-2-big-Data.db)
	Version string // Snapshot tag extension representing backup that introduced newer version of this SSTable (e.g. sm_20230114183231UTC)
	Size    int64
}

// FullName returns versioned file name.
func (vt VersionedSSTable) FullName() string {
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

// VersionedFileCreationTime returns the time of versioned file creation
// (the time when the newer version of the file has been uploaded to the backup location).
func VersionedFileCreationTime(versioned string) (time.Time, error) {
	snapshotExt := path.Ext(versioned)[1:]
	return SnapshotTagTime(snapshotExt)
}

// IsVersionedFileRemovable checks if versioned file can be safely purged.
// In order to decide that, the time of the oldest stored backup is required.
func IsVersionedFileRemovable(oldest time.Time, versioned string) (bool, error) {
	t, err := VersionedFileCreationTime(versioned)
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

// SplitNameAndVersion splits versioned file name into its original name and its version.
func SplitNameAndVersion(versioned string) (string, string) {
	versionExt := path.Ext(versioned)
	baseName := strings.TrimSuffix(versioned, versionExt)
	return baseName, versionExt[1:]
}

// VersionedFileRegex is a rclone formatted regex that can be used to distinguish versioned files.
const VersionedFileRegex = `{**.sm_*UTC}`
