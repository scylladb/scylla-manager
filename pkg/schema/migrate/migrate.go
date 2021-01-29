// Copyright (C) 2017 ScyllaDB

package migrate

import (
	"context"
	"sync"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
)

// Logger allows for setting a custom logger that can be used by migration
// callback functions, default is log.NopLogger.
var Logger = log.NopLogger

type callback func(ctx context.Context, session gocqlx.Session, logger log.Logger) error

type nameEvent struct {
	name  string
	event migrate.CallbackEvent
}

var (
	register = make(map[nameEvent]callback)
	lock     = sync.Mutex{}
)

func registerCallback(name string, ev migrate.CallbackEvent, f callback) {
	lock.Lock()
	defer lock.Unlock()
	register[nameEvent{name, ev}] = f
}

func findCallback(name string, ev migrate.CallbackEvent) callback {
	lock.Lock()
	defer lock.Unlock()
	return register[nameEvent{name, ev}]
}

// Callback is the main callback dispatcher we use for custom migrations.
func Callback(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
	if ev == migrate.BeforeMigration {
		Logger.Info(ctx, "Running migration", "migration", name)
	}

	if f := findCallback(name, ev); f != nil {
		return f(ctx, session, Logger.With("migration", name, "event", ev))
	}

	return nil
}
