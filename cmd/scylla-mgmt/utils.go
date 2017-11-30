// Copyright (C) 2017 ScyllaDB

package main

import "time"

func midnight() time.Time {
	day := 24 * time.Hour
	return time.Now().Round(day).Add(day)
}
