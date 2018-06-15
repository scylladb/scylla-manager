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
	lock     = &sync.Mutex{}
)

func registerMigrationCallback(name string, ev migrate.CallbackEvent, f callback) {
	lock.Lock()
	defer lock.Unlock()
	register[nameEvent{name, ev}] = f
}

// MigrateCallback is the main callback dispatcher we use for custom migrations.
func MigrateCallback(ctx context.Context, session *gocql.Session, ev migrate.CallbackEvent, name string) error {
	lock.Lock()
	defer lock.Unlock()

	if f, ok := register[nameEvent{name, ev}]; ok {
		l := Logger.With("event", ev, "migration", name)
		l.Debug(ctx, "Start")
		err := f(ctx, session, l)
		l.Debug(ctx, "Stop")
		return err
	}

	return nil
}
