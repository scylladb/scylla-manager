// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"fmt"
	"os"
)

// Print prints msg to stderr.
func Print(msg string) {
	fmt.Fprintf(os.Stderr, "--- %s\n", msg)
}
