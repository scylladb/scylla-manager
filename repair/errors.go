// Copyright (C) 2017 ScyllaDB

package repair

import "errors"

// Repair errors
var (
	ErrDisabled     = errors.New("repair disabled")
	ErrActiveRepair = errors.New("repair already in progress")
)
