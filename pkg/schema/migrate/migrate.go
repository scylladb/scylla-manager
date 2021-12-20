// Copyright (C) 2017 ScyllaDB

package migrate

import (
	"context"
	"strings"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
)

var reg = make(migrate.CallbackRegister)

// Logger is a default logger use in code based migrations.
var Logger = log.NopLogger

// Callback is the main callback dispatcher we use for custom migrations.
func Callback(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
	if ev == migrate.BeforeMigration && strings.HasPrefix(name, "v") {
		Logger.Info(ctx, "Running migration", "migration", name)
	}
	return reg.Callback(ctx, session, ev, name)
}
