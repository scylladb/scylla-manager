// Copyright (C) 2017 ScyllaDB

package backup

import (
	"regexp"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
)

var (
	tagDateFormat = "20060102150405"
	tagRegexp     = regexp.MustCompile("^sm_([0-9]{14})UTC$")
)

func newSnapshotTag() string {
	return snapshotTagAt(timeutc.Now())
}

func snapshotTagAt(t time.Time) string {
	return "sm_" + t.UTC().Format(tagDateFormat) + "UTC"
}

func isSnapshotTag(tag string) bool {
	return tagRegexp.MatchString(tag)
}

func snapshotTagTime(tag string) (time.Time, error) {
	m := tagRegexp.FindStringSubmatch(tag)
	if m == nil {
		return time.Time{}, errors.New("wrong format")
	}
	return timeutc.Parse(tagDateFormat, m[1])
}
