// Copyright (C) 2017 ScyllaDB

package managerclient

// Stage enumeration.
const (
	RestoreStageInit          = "INIT"
	RestoreStageDropViews     = "DROP_VIEWS"
	StageDisableCompaction    = "DISABLE_COMPACTION"
	RestoreStageDisableTGC    = "DISABLE_TGC"
	RestoreStageData          = "DATA"
	RestoreStageRepair        = "REPAIR"
	RestoreStageEnableTG      = "ENABLE_TGC"
	StageEnableCompaction     = "ENABLE_COMPACTION"
	RestoreStageRecreateViews = "RECREATE_VIEWS"
	RestoreStageDone          = "DONE"
)

var restoreStageName = map[string]string{
	RestoreStageInit:          "initialising",
	RestoreStageDropViews:     "dropping restored views",
	StageDisableCompaction:    "disabling restored tables auto-compaction",
	RestoreStageDisableTGC:    "disabling restored tables tombstone_gc",
	RestoreStageData:          "restoring backed-up data",
	RestoreStageRepair:        "repairing restored tables",
	RestoreStageEnableTG:      "enabling restored tables tombstone_gc",
	StageEnableCompaction:     "enabling restored tables auto-compaction",
	RestoreStageRecreateViews: "recreating restored views",
	RestoreStageDone:          "",
}

// RestoreStageName returns verbose name for restore stage.
func RestoreStageName(s string) string {
	return restoreStageName[s]
}
