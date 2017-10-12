// Copyright (C) 2017 ScyllaDB

package mermaid

//go:generate rm -Rvf internal
//go:generate mkdir internal
//go:generate swagger generate client -A mermaid -T ../../../swagger/template -f ../../../swagger/restapi.json -t ./internal
