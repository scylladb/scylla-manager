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

// RestoreStage specifies the restore worker stage.
type RestoreStage string

// RestoreStage enumeration.
const (
	StageRestoreInit       RestoreStage = "INIT"
	StageRestoreDisableTGC              = "DISABLE_TGC"
	StageRestoreData       RestoreStage = "DATA"
	StageRestoreRepair     RestoreStage = "REPAIR"
	StageRestoreDone       RestoreStage = "DONE"
	StageRestoreEnableTGC               = "ENABLE_TGC"
)

// RestoreStageOrder lists all restore stages in the order of their execution.
var RestoreStageOrder = []RestoreStage{
	StageRestoreInit,
	StageRestoreDisableTGC,
	StageRestoreData,
	StageRestoreRepair,
	StageRestoreEnableTGC,
	StageRestoreDone,
}

// Index returns stage position in RestoreStageOrder.
func (s RestoreStage) Index() int {
	for i, stage := range RestoreStageOrder {
		if s == stage {
			return i
		}
	}
	panic("Unknown stage: " + s)
}
