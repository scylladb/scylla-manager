// Copyright (C) 2017 ScyllaDB

// +build tools

package mermaid

// This file exists only to trick modules into getting extra packages,
// typically tools that are part of other packages that are otherwise wiped by
// go mod tidy.
// This is useful to for having consistent library version and it's tooling.

import (
	_ "github.com/golang/mock/mockgen"

	_ "github.com/go-openapi/analysis"
	_ "github.com/go-openapi/jsonpointer"
	_ "github.com/go-openapi/jsonreference"
	_ "github.com/go-openapi/loads"
	_ "github.com/go-openapi/spec"
)
