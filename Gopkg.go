// Copyright (C) 2017 ScyllaDB

// +build dep

package mermaid

// This file exists only to trick dep into getting extra packages, typically
// tools that are part of other packages that are otherwise wiped by dep.
// This is useful to for having consistent library version and it's tooling.

import (
	_ "github.com/golang/mock/mockgen"
)
