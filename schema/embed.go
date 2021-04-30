// Copyright (C) 2017 ScyllaDB

package schema

import "embed"

// Files contains *.cql schema migration files.
//go:embed *.cql
var Files embed.FS
