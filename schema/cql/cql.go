// Copyright (C) 2017 ScyllaDB

package cql

import (
	"context"
	"sync"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/migrate"
)

var (
	// Logger allows for setting a custom logger that can be used by migration
	// callback functions, default is log.NopLogger.
	Logger = log.NopLogger
)

type callback func(ctx context.Context, session *gocql.Session, logger log.Logger) error

type nameEvent struct {
	name  string
	event migrate.CallbackEvent
}

var (
	register = make(map[nameEvent]callback)
	lock     = sync.Mutex{}
)

func registerMigrationCallback(name string, ev migrate.CallbackEvent, f callback) {
	lock.Lock()
	defer lock.Unlock()
	register[nameEvent{name, ev}] = f
}

func migrationCallback(name string, ev migrate.CallbackEvent) callback {
	lock.Lock()
	defer lock.Unlock()
	return register[nameEvent{name, ev}]
}

// MigrateCallback is the main callback dispatcher we use for custom migrations.
func MigrateCallback(ctx context.Context, session *gocql.Session, ev migrate.CallbackEvent, name string) error {
	if ev == migrate.BeforeMigration {
		Logger.Info(ctx, "Running migration", "migration", name)
	}

	if f := migrationCallback(name, ev); f != nil {
		return f(ctx, session, Logger.With("migration", name, "event", ev))
	}

	return nil
}
