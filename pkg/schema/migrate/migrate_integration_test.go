// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package migrate

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/migrate"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/schema"
)

func init() {
	migrate.Callback = Callback
	Logger = log.NewDevelopment()
}

var oldReg migrate.CallbackRegister

func saveRegister() {
	oldReg = make(migrate.CallbackRegister)
	for k, v := range reg {
		oldReg[k] = v
	}
}

func restoreRegister() {
	reg = oldReg
}

func testCallback(t *testing.T, ev migrate.CallbackEvent, name string, prepare, validate func(session gocqlx.Session) error) {
	cb := reg.Find(ev, name)

	saveRegister()
	defer restoreRegister()
	session := testutils.CreateSessionWithoutMigration(t)

	reg.Add(ev, name, func(ctx context.Context, session gocqlx.Session, ev migrate.CallbackEvent, name string) error {
		if err := prepare(session); err != nil {
			return errors.Wrap(err, "prepare")
		}
		if err := cb(ctx, session, ev, name); err != nil {
			return errors.Wrap(err, "callback")
		}
		if err := validate(session); err != nil {
			return errors.Wrap(err, "validate")
		}

		return nil
	})

	if err := migrate.FromFS(context.Background(), session, schema.Files); err != nil {
		t.Fatal(err)
	}
}
