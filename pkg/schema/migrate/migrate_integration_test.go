// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package migrate

import (
	"github.com/scylladb/gocqlx/v2/migrate"
)

func init() {
	migrate.Callback = Callback
}

var oldReg migrate.CallbackRegister

func saveRegister() {
	oldReg = make(migrate.CallbackRegister)
	for k, v := range reg {
		oldReg[k] = v
	}
}

func restoreRegister() {
	reg = oldReg
}
