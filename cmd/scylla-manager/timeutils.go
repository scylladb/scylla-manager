// Copyright (C) 2017 ScyllaDB

package main

import "time"

const day = 24 * time.Hour

func midnight() time.Time {
	return time.Now().AddDate(0, 0, 1).Truncate(day).UTC()
}

func nowPlus(d time.Duration) time.Time {
	return time.Now().Add(d).Truncate(time.Second).UTC()
}
