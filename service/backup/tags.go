// Copyright (C) 2017 ScyllaDB

package backup

import (
	"github.com/scylladb/mermaid/internal/timeutc"
)

func newSnapshotTag() string {
	return "sm_" + timeutc.Now().Format("20060102150405") + "UTC"
}

func claimTag(tag string) bool {
	return tagRegexp.MatchString(tag)
}
