// Copyright (C) 2017 ScyllaDB

package scylla

//go:generate rm -Rvf internal
//go:generate mkdir internal
//go:generate swagger generate client -A scylla -f ../swagger/scylla.json -t ./internal
