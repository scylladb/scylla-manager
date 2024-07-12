// Copyright (C) 2017 ScyllaDB

package backup

import "github.com/scylladb/scylla-manager/v3/pkg/util/slice"

// Stage specifies the backup worker stage.
type Stage string

// Stage enumeration.
const (
	StageInit         Stage = "INIT"
	StageSnapshot     Stage = "SNAPSHOT"
	StageAwaitSchema  Stage = "AWAIT_SCHEMA"
	StageIndex        Stage = "INDEX"
	StageManifest     Stage = "MANIFEST"
	StageSchema       Stage = "SCHEMA"
	StageDeduplicate  Stage = "DEDUPLICATE"
	StageUpload       Stage = "UPLOAD"
	StageMoveManifest Stage = "MOVE_MANIFEST"
	StageMigrate      Stage = "MIGRATE"
	StagePurge        Stage = "PURGE"
	StageDone         Stage = "DONE"
)

var stageDescription = map[Stage]string{
	StageInit:         "initialising",
	StageSnapshot:     "taking snapshot",
	StageAwaitSchema:  "awaiting schema agreement",
	StageIndex:        "indexing snapshot files",
	StageManifest:     "uploading manifest files",
	StageSchema:       "uploading cql schema",
	StageUpload:       "uploading snapshot files",
	StageMoveManifest: "moving manifest files",
	StagePurge:        "purging stale snapshots",
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
		StageMigrate,
		StagePurge,
		StageDone,
	}
}

// Resumable run can be continued.
func (s Stage) Resumable() bool {
	switch s {
	case StageIndex, StageManifest, StageUpload, StageDeduplicate, StageMoveManifest, StageMigrate, StagePurge:
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
