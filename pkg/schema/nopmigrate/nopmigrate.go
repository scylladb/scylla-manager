// Copyright (C) 2021 ScyllaDB

package nopmigrate

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
)

var reg = make(migrate.CallbackRegister)

func nopCallback(context.Context, gocqlx.Session, migrate.CallbackEvent, string) error {
	return nil
}

func init() {
	reg.Add(migrate.CallComment, "rewriteHealthCheck30", nopCallback)
	reg.Add(migrate.CallComment, "setExistingTasksDeleted", nopCallback)
}

// Callback is a sibling of migrate.Callback that registers all mandatory callbacks as nop.
var Callback = reg.Callback
