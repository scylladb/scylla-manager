// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/rclone/rclone/fs/rc"
)

func TestInMemoryConf(t *testing.T) {
	tests := []struct {
		Name string
		Path string
		In   rc.Params
		Out  rc.Params
		Err  bool
	}{
		{
			Name: "Create Config",
			Path: "config/create",
			In: rc.Params{
				"name": "testdata", "type": "local", "parameters": rc.Params{"extra": "data"},
			},
			Out: nil,
			Err: false,
		},
		{
			Name: "Get Remote",
			Path: "config/get",
			In:   rc.Params{"name": "testdata"},
			Out:  rc.Params{"type": "local", "extra": "data"},
			Err:  false,
		},
		{
			Name: "About Remote",
			Path: "operations/about",
			In:   rc.Params{"fs": "testdata"},
			Out:  nil,
			Err:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			call := rc.Calls.Get(test.Path)
			if call == nil {
				t.Fatalf("Can't find %s call", test.Path)
			}
			out, err := call.Fn(context.Background(), test.In)
			if err != nil {
				if !test.Err {
					t.Fatalf("Didn't expect error: %+v", err)
				}
				return
			}
			if test.Err {
				t.Fatalf("Expected error but got: %+v", out)
			}
			if test.Out != nil {
				if diff := cmp.Diff(test.Out, out); diff != "" {
					t.Fatal(diff)
				}
			}
		})
	}
}
