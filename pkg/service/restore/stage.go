// Copyright (C) 2023 ScyllaDB

package restore

import "github.com/scylladb/scylla-manager/v3/pkg/util/slice"

// Stage specifies the restore stage.
type Stage string

// Stage enumeration.
const (
	StageInit          Stage = "INIT"
	StageDropViews     Stage = "DROP_VIEWS"
	StageDisableTGC    Stage = "DISABLE_TGC"
	StageData          Stage = "DATA"
	StageRepair        Stage = "REPAIR"
	StageEnableTGC     Stage = "ENABLE_TGC"
	StageRecreateViews Stage = "RECREATE_VIEWS"
	StageDone          Stage = "DONE"
)

// StageOrder lists all restore stages in the order of their execution.
func StageOrder() []Stage {
	return []Stage{
		StageInit,
		StageDropViews,
		StageDisableTGC,
		StageData,
		StageRepair,
		StageEnableTGC,
		StageRecreateViews,
		StageDone,
	}
}

// Index returns stage position in RestoreStageOrder.
func (s Stage) Index() int {
	return slice.Index(StageOrder(), s)
}
