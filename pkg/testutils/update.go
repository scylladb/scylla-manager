// Copyright (C) 2017 ScyllaDB

package testutils

import "flag"

var flagUpdate = flag.Bool("update", false, "update .golden files")

// UpdateGoldenFiles true integration tests that support it should update their
// golden files.
func UpdateGoldenFiles() bool {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagUpdate
}
