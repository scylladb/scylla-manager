// Copyright (C) 2026 ScyllaDB

package managerclient

import (
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// Golden files hold times rendered in CET; a fixed zone (no DST) keeps UTC->local conversion tested.
	time.Local = time.FixedZone("CET", 3600)
	os.Exit(m.Run())
}
