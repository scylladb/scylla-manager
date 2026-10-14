// Copyright (C) 2026 ScyllaDB

package backup

import "github.com/scylladb/scylla-manager/v3/pkg/util/slice"

// Stage specifies the backup worker stage.
type Stage string

// Stage enumeration.
const (
	StageInit          Stage = "INIT"
	StageSnapshot      Stage = "SNAPSHOT"
	StageAwaitSchema   Stage = "AWAIT_SCHEMA"
	StageIndex         Stage = "INDEX"
	StageManifest      Stage = "MANIFEST"
	StageSchema        Stage = "SCHEMA"
	StageDeduplicate   Stage = "DEDUPLICATE"
	StageUpload        Stage = "UPLOAD"
	StageMoveManifest  Stage = "MOVE_MANIFEST"
	StageRetentionLock Stage = "RETENTION_LOCK"
	StageMigrate       Stage = "MIGRATE"
	StagePurge         Stage = "PURGE"
	StageDone          Stage = "DONE"
)

var stageDescription = map[Stage]string{
	StageInit:          "initialising",
	StageSnapshot:      "taking snapshot",
	StageAwaitSchema:   "awaiting schema agreement",
	StageIndex:         "indexing snapshot files",
	StageManifest:      "uploading manifest files",
	StageSchema:        "uploading schema",
	StageUpload:        "uploading snapshot files",
	StageMoveManifest:  "moving manifest files",
	StageRetentionLock: "setting retention locks",
	StagePurge:         "purging stale snapshots",
}

// StageOrder listing of all stages in the order of execution.
func StageOrder() []Stage {
	return []Stage{
		StageInit,
		StageSnapshot,
		StageAwaitSchema,
		StageIndex,
		StageManifest,
		StageSchema,
		StageDeduplicate,
		StageUpload,
		StageMoveManifest,
		StageRetentionLock,
		StageMigrate,
		StagePurge,
		StageDone,
	}
}

// Resumable run can be continued.
// Only stages starting from StageDeduplicate are resumable.
// The reason for that is the gap between StageAwaitSchema during which
// the schema is dumped and StageSchema during which it is uploaded.
// Stopping and resuming in between those stages would drop the schema
// from the backup. One solution could be to allow re-dumping the schema
// on resume, but it would increase the timestamp gap between snapshot-ed
// sstables and dumped schema, which is not desirable. We also can't upload
// the schema right after it was dumped, as then we would upload a file
// that's not yet referenced by any manifest, so it would be potentially
// orphaned without any changes on the purge side.
func (s Stage) Resumable() bool {
	switch s {
	case StageDeduplicate, StageUpload, StageMoveManifest, StageRetentionLock, StageMigrate, StagePurge:
		return true
	default:
		return false
	}
}

// Index returns stage position among all stages, stage with index n+1 happens
// after stage n.
func (s Stage) Index() int {
	return slice.Index(StageOrder(), s)
}
