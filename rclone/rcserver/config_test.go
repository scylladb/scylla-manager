// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"context"
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
			Name: "Create Config",
			Path: "config/create",
			In: rc.Params{
				"name": "testdata", "type": "local", "parameters": rc.Params{"extra": "data"},
			},
		},
		{
			Name: "Get Remote",
			Path: "config/get",
			In:   rc.Params{"name": "testdata"},
		},
		{
			Name: "Delete Remote",
			Path: "config/delete",
			In:   rc.Params{"name": "testdata"},
		},
	}

	RegisterInMemoryConf()

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			call := rc.Calls.Get(test.Path)
			if call == nil {
				t.Fatalf("Can't find %s call", test.Path)
			}
			out, err := call.Fn(context.Background(), test.In)
			if err == nil {
				t.Fatalf("Expected error but got: %+v", out)
			}
		})
	}
}
