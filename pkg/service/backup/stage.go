// Copyright (C) 2017 ScyllaDB

package backup

import "github.com/scylladb/scylla-manager/v3/pkg/util/slice"

// Stage specifies the backup worker stage.
type Stage string

// Stage enumeration.
const (
	StageInit         Stage = "INIT"
	StageAwaitSchema  Stage = "AWAIT_SCHEMA"
	StageSnapshot     Stage = "SNAPSHOT"
	StageIndex        Stage = "INDEX"
	StageManifest     Stage = "MANIFEST"
	StageSchema       Stage = "SCHEMA"
	StageUpload       Stage = "UPLOAD"
	StageMoveManifest Stage = "MOVE_MANIFEST"
	StageMigrate      Stage = "MIGRATE"
	StagePurge        Stage = "PURGE"
	StageDone         Stage = "DONE"
)

var stageDescription = map[Stage]string{
	StageInit:         "initialising",
	StageAwaitSchema:  "awaiting schema agreement",
	StageSnapshot:     "taking snapshot",
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
		StageAwaitSchema,
		StageSnapshot,
		StageIndex,
		StageManifest,
		StageSchema,
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
	case StageIndex, StageManifest, StageUpload, StageMoveManifest, StageMigrate, StagePurge:
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
