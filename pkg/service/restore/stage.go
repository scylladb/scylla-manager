// Copyright (C) 2023 ScyllaDB

package restore

import "github.com/scylladb/scylla-manager/v3/pkg/util/slice"

// Stage specifies the restore stage.
type Stage string

// Stage enumeration.
const (
	StageInit              Stage = "INIT"
	StageDropViews         Stage = "DROP_VIEWS"
	StageDisableCompaction Stage = "DISABLE_COMPACTION"
	StageDisableTGC        Stage = "DISABLE_TGC"
	StageData              Stage = "DATA"
	StageRepair            Stage = "REPAIR"
	StageEnableTGC         Stage = "ENABLE_TGC"
	StageEnableCompaction  Stage = "ENABLE_COMPACTION"
	StageRecreateViews     Stage = "RECREATE_VIEWS"
	StageDone              Stage = "DONE"
)

// StageOrder lists all restore stages in the order of their execution.
func StageOrder() []Stage {
	return []Stage{
		StageInit,
		StageDropViews,
		StageDisableTGC,
		StageDisableCompaction,
		StageData,
		StageRepair,
		StageEnableCompaction,
		StageEnableTGC,
		StageRecreateViews,
		StageDone,
	}
}

// Index returns stage position in RestoreStageOrder.
func (s Stage) Index() int {
	return slice.Index(StageOrder(), s)
}
