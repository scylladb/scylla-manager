// Copyright (C) 2017 ScyllaDB

package cql

import (
	"context"
	"sync"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/migrate"
	log "github.com/scylladb/golog"
)

var (
	// Logger is available for callers to set as they see fit
	// Default is log.NopLogger
	Logger = log.NopLogger

	register = make(map[string]migrate.CallbackFunc)
	lock     = &sync.Mutex{}
)

func registerMigrationCallback(name string, f migrate.CallbackFunc) {
	lock.Lock()
	defer lock.Unlock()

	register[name] = f
}

// MigrateCallback is the main callback dispatcher we use for custom migrations
func MigrateCallback(ctx context.Context, session *gocql.Session, ev migrate.CallbackEvent, name string) error {
	lock.Lock()
	defer lock.Unlock()

	if f, ok := register[name]; ok {
		return f(ctx, session, ev, name)
	}

	return nil
}
