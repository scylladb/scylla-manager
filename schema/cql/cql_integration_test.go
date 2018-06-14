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
