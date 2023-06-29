// Copyright (C) 2017 ScyllaDB

package managerclient

// Stage enumeration.
const (
	RestoreStageInit   = "INIT"
	RestoreStageData   = "DATA"
	RestoreStageRepair = "REPAIR"
	RestoreStageDone   = "DONE"
)

var restoreStageName = map[string]string{
	RestoreStageInit:   "initialising",
	RestoreStageData:   "restoring backed-up data",
	RestoreStageRepair: "repairing restored tables",
	RestoreStageDone:   "",
}

// RestoreStageName returns verbose name for restore stage.
func RestoreStageName(s string) string {
	return restoreStageName[s]
}
