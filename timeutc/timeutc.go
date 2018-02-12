// Copyright (C) 2017 ScyllaDB

package timeutc

import "time"

const (
	day = 24 * time.Hour
)

// Now returns current time in UTC.
func Now() time.Time {
	return time.Now().UTC()
}

// Parse calls time.Parse and returns value in UCT.
func Parse(layout, value string) (time.Time, error) {
	t, err := time.Parse(layout, value)
	return t.UTC(), err
}

// Since returns the time elapsed since t.
func Since(t time.Time) time.Duration {
	return Now().Sub(t.UTC())
}

// TodayMidnight returns local midnight time in UTC.
func TodayMidnight() time.Time {
	return time.Now().AddDate(0, 0, 1).Truncate(day).UTC()
}
