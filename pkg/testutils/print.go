// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"fmt"
	"os"
)

// Print prints msg to stderr.
func Print(msg string) {
	fmt.Fprintf(os.Stderr, "--- %s\n", msg)
}

// Printf prints format string to stderr.
func Printf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "--- "+format+"\n", args...)
}
