// Copyright (C) 2021 ScyllaDB

package nopmigrate

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
)

var reg = make(migrate.CallbackRegister)

func nopCallback(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
	return nil
}

func init() {
	reg.Add(migrate.CallComment, "rewriteHealthCheck30", nopCallback)
}

// Callback is a sibling of migrate.Callback that registers all mandatory callbacks as nop.
var Callback = reg.Callback
