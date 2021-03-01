// Copyright (C) 2017 ScyllaDB

package backupspec

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

var stageOrder = []Stage{
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

// StageOrder listing of all stages in the order of execution.
func StageOrder() []Stage {
	return stageOrder
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
	for i := 0; i < len(stageOrder); i++ {
		if s == stageOrder[i] {
			return i
		}
	}
	panic("Unknown stage " + s)
}

var stageName = map[Stage]string{
	StageInit:         "initialising",
	StageAwaitSchema:  "awaiting schema agreement",
	StageSnapshot:     "taking snapshot",
	StageIndex:        "indexing files",
	StageManifest:     "uploading manifests",
	StageSchema:       "uploading schema",
	StageUpload:       "uploading data",
	StageMoveManifest: "moving manifests",
	StageMigrate:      "migrating legacy metadata",
	StagePurge:        "retention",
	StageDone:         "",
}

// Name returns the stage name for humans.
func (s Stage) Name() string {
	return stageName[s]
}
