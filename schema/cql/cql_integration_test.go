// Copyright (C) 2017 ScyllaDB

// +build all integration

package cql

import (
	"github.com/scylladb/gocqlx/migrate"
	log "github.com/scylladb/golog"
)

func init() {
	Logger = log.NewDevelopment()
	migrate.Callback = MigrateCallback
}

var _register map[nameEvent]callback

func saveRegister() {
	_register = make(map[nameEvent]callback, len(register))
	for k, v := range register {
		_register[k] = v
	}
}

func restoreRegister() {
	register = make(map[nameEvent]callback, len(_register))
	for k, v := range _register {
		register[k] = v
	}
}
