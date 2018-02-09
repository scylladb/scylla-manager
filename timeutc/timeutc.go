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

// Since returns the time elapsed since t.
func Since(t time.Time) time.Duration {
	return Now().Sub(t.UTC())
}

// Parse parses time according to RFC3339 and returns time in UTC.
func Parse(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

// TodayMidnight returns local midnight time in UTC.
func TodayMidnight() time.Time {
	return time.Now().AddDate(0, 0, 1).Truncate(day).UTC()
}
