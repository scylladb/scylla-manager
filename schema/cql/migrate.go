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
	logger log.Logger

	register = make(map[string]migrate.CallbackFunc)
	lock     = &sync.Mutex{}
)

func registerMigrationCallback(name string, f migrate.CallbackFunc) {
	lock.Lock()
	defer lock.Unlock()

	register[name] = f
}

func MigrateCallback(ctx context.Context, session *gocql.Session, ev migrate.CallbackEvent, name string) error {
	lock.Lock()
	defer lock.Unlock()

	if f, ok := register[name]; ok {
		return f(ctx, session, ev, name)
	}

	return nil
}

func SetLogger(l log.Logger) {
	logger = l
}
