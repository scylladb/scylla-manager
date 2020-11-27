// Copyright (C) 2017 ScyllaDB

// +build modules

package mod

// This file exists only to trick modules into getting extra packages,
// typically tools that are part of other packages that are otherwise wiped by
// go mod tidy.

import (
	_ "github.com/golang/mock/mockgen"
	_ "golang.org/x/tools/cmd/stress"
	_ "github.com/go-openapi/runtime"
)
