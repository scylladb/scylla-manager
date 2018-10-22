// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/uuid"
)

const (
	nowSafety     = 30 * time.Second
	rfc822WithSec = "02 Jan 06 15:04:05 MST"
)

// TaskSplit attempts to split a string into type and id.
func TaskSplit(s string) (taskType string, taskID uuid.UUID, err error) {
	i := strings.LastIndex(s, "/")
	if i != -1 {
		taskType = s[:i]
	}
	taskID, err = uuid.Parse(s[i+1:])
	return
}

// TaskJoin creates a new type id string in the form `taskType/taskId`.
func TaskJoin(taskType string, taskID interface{}) string {
	return fmt.Sprint(taskType, "/", taskID)
}

// ParseStartDate parses the supplied string as a strfmt.DateTime.
func ParseStartDate(value string) (strfmt.DateTime, error) {
	now := timeutc.Now()

	if value == "now" {
		return strfmt.DateTime(now.Add(nowSafety)), nil
	}

	if strings.HasPrefix(value, "now") {
		d, err := time.ParseDuration(value[3:])
		if err != nil {
			return strfmt.DateTime{}, err
		}
		if d < 0 {
			return strfmt.DateTime(time.Time{}), errors.New("start date cannot be in the past")
		}
		if d < nowSafety {
			return strfmt.DateTime(time.Time{}), errors.Errorf("start date must be at least in %s", nowSafety)
		}
		return strfmt.DateTime(now.Add(d)), nil
	}

	// No more heuristics, assume the user passed a date formatted string
	t, err := timeutc.Parse(time.RFC3339, value)
	if err != nil {
		return strfmt.DateTime(t), err
	}
	if t.Before(now) {
		return strfmt.DateTime(time.Time{}), errors.New("start date cannot be in the past")
	}
	if t.Before(now.Add(nowSafety)) {
		return strfmt.DateTime(time.Time{}), errors.Errorf("start date must be at least in %s", nowSafety)
	}
	return strfmt.DateTime(t), nil
}

func uuidFromLocation(location string) (uuid.UUID, error) {
	l, err := url.Parse(location)
	if err != nil {
		return uuid.Nil, err
	}
	_, id := path.Split(l.Path)

	return uuid.Parse(id)
}

// FormatPercent simply creates a percent representation  of the supplied value.
func FormatPercent(p int64) string {
	return fmt.Sprint(p, "%")
}

// FormatTime formats the supplied DateTime in `02 Jan 06 15:04:05 MST` format.
func FormatTime(t strfmt.DateTime) string {
	if isZero(t) {
		return ""
	}
	return time.Time(t).Local().Format(rfc822WithSec)
}

// FormatDuration creates and formats the duration between
// the supplied DateTime values.
func FormatDuration(t0 strfmt.DateTime, t1 strfmt.DateTime) string {
	var d time.Duration
	if isZero(t1) {
		d = timeutc.Now().Sub(time.Time(t0))
	} else {
		d = time.Time(t1).Sub(time.Time(t0))
	}

	return fmt.Sprint(d.Truncate(time.Second))
}

func isZero(t strfmt.DateTime) bool {
	return time.Time(t).IsZero()
}

func dumpMap(m map[string]interface{}) string {
	if len(m) == 0 {
		return ""
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	s := make([]string, 0, len(m))
	for _, k := range keys {
		s = append(s, fmt.Sprint(k, ":", m[k]))
	}
	return strings.Join(s, ", ")
}
