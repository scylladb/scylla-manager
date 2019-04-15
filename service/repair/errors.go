// Copyright (C) 2017 ScyllaDB

package repair

import "errors"

// Repair errors
var (
	ErrActiveRepair = errors.New("repair already in progress")
)
