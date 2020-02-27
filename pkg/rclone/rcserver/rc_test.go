// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"github.com/scylladb/mermaid/pkg/rclone/rcserver/internal"
)

func filterRcCallsForTests() {
	internal.RcloneSupportedCalls.Add(
		"rc/noop",
		"rc/error",
	)
	filterRcCalls()
}
