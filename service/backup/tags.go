// Copyright (C) 2017 ScyllaDB

package backup

import (
	"regexp"
	"time"

	"github.com/scylladb/mermaid/internal/timeutc"
)

func newSnapshotTag() string {
	return snapshotTagAt(timeutc.Now())
}

func snapshotTagAt(t time.Time) string {
	return "sm_" + t.UTC().Format("20060102150405") + "UTC"
}

var tagRegexp = regexp.MustCompile("^sm_[0-9]{14}UTC$")

func isSnapshotTag(tag string) bool {
	return tagRegexp.MatchString(tag)
}
