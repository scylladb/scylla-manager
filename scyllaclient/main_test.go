// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	setupRclone()
	os.Exit(m.Run())
}
