// Copyright (C) 2017 ScyllaDB

package managerclient

// Stage enumeration.
const (
	RestoreStageInit       = "INIT"
	RestoreStageDisableTGC = "DISABLE_TGC"
	RestoreStageData       = "DATA"
	RestoreStageRepair     = "REPAIR"
	RestoreStageEnableTG   = "ENABLE_TGC"
	RestoreStageDone       = "DONE"
)

var restoreStageName = map[string]string{
	RestoreStageInit:       "initialising",
	RestoreStageDisableTGC: "disabling restored tables tombstone_gc",
	RestoreStageData:       "restoring backed-up data",
	RestoreStageRepair:     "repairing restored tables",
	RestoreStageEnableTG:   "enabling restored tables tombstone_gc",
	RestoreStageDone:       "",
}

// RestoreStageName returns verbose name for restore stage.
func RestoreStageName(s string) string {
	return restoreStageName[s]
}
