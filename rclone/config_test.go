// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"testing"

	"github.com/rclone/rclone/fs/rc"
)

func TestInMemoryConfNotExposed(t *testing.T) {
	tests := []struct {
		Name string
		Path string
		In   rc.Params
	}{
		{
			Name: "create Config",
			Path: "config/create",
			In: rc.Params{
				"name": "testdata", "type": "local", "parameters": rc.Params{"extra": "data"},
			},
		},
		{
			Name: "get Remote",
			Path: "config/get",
			In:   rc.Params{"name": "testdata"},
		},
		{
			Name: "delete Remote",
			Path: "config/delete",
			In:   rc.Params{"name": "testdata"},
		},
	}

	initInMemoryConfig()

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			call := rc.Calls.Get(test.Path)
			if call != nil {
				t.Fatalf("Can't find %s call", test.Path)
			}
		})
	}
}
