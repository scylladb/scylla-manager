package mermaid

import (
	"github.com/gocql/gocql"
)

type UUID = gocql.UUID

// Common errors
var (
	ErrNotFound = gocql.ErrNotFound
)
