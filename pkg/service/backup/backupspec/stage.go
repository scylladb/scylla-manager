// Copyright (C) 2017 ScyllaDB

package backupspec

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

// RestoreStage specifies the restore worker stage.
type RestoreStage string

// RestoreStage enumeration.
const (
	StageRestoreInit          RestoreStage = "INIT"
	StageRestoreDropViews     RestoreStage = "DROP_VIEWS"
	StageRestoreDisableTGC    RestoreStage = "DISABLE_TGC"
	StageRestoreData          RestoreStage = "DATA"
	StageRestoreRepair        RestoreStage = "REPAIR"
	StageRestoreEnableTGC     RestoreStage = "ENABLE_TGC"
	StageRestoreRecreateViews RestoreStage = "RECREATE_VIEWS"
	StageRestoreDone          RestoreStage = "DONE"
)

// RestoreStageOrder lists all restore stages in the order of their execution.
func RestoreStageOrder() []RestoreStage {
	return []RestoreStage{
		StageRestoreInit,
		StageRestoreDropViews,
		StageRestoreDisableTGC,
		StageRestoreData,
		StageRestoreRepair,
		StageRestoreEnableTGC,
		StageRestoreRecreateViews,
		StageRestoreDone,
	}
}

// Index returns stage position in RestoreStageOrder.
func (s RestoreStage) Index() int {
	return slice.Index(RestoreStageOrder(), s)
}
