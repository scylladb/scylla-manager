// Copyright (C) 2017 ScyllaDB

package backupspec

import (
	"github.com/scylladb/scylla-manager/v3/pkg/util/retry"
)

// Stage specifies the backup worker stage.
type Stage string

// Stage enumeration.
const (
	StageInit         Stage = "INIT"
	StageClusterCheck Stage = "CLUSTER_CHECK"
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
	StageClusterCheck,
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

func DefaultStagesBackoffSetup() map[Stage]retry.Backoff {
	return map[Stage]retry.Backoff{
		StageClusterCheck: retry.DefaultExponentialBackoff(),
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
	for i := 0; i < len(stageOrder); i++ {
		if s == stageOrder[i] {
			return i
		}
	}
	panic("Unknown stage " + s)
}
