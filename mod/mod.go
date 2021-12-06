// Copyright (C) 2017 ScyllaDB

//go:build modules
// +build modules

package mod

// This file exists only to trick modules into getting extra packages,
// typically tools that are part of other packages that are otherwise wiped by
// go mod tidy.

import (
	_ "github.com/go-enry/go-license-detector/v4/cmd/license-detector"
	_ "github.com/go-openapi/runtime"
	_ "github.com/go-swagger/go-swagger/cmd/swagger"
	_ "github.com/golang/mock/mockgen"
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
	_ "github.com/goreleaser/goreleaser/cmd"
	_ "github.com/mikefarah/yq/v3/cmd"
	_ "github.com/pressly/sup/cmd/sup"
	_ "github.com/scylladb/gocqlx/v2/cmd/schemagen"
	_ "golang.org/x/tools/cmd/stress"
)
