// Copyright (C) 2017 ScyllaDB

package fsutil

import (
	"os/user"
	"path/filepath"
	"testing"
)

func TestExpand(t *testing.T) {
	u, err := user.Current()
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	table := []struct {
		Input  string
		Output string
		Err    bool
	}{
		{
			"/foo",
			"/foo",
			false,
		},

		{
			"~/foo",
			filepath.Join(u.HomeDir, "foo"),
			false,
		},

		{
			"",
			"",
			false,
		},

		{
			"~",
			u.HomeDir,
			false,
		},

		{
			"~foo/foo",
			"",
			true,
		},
	}

	for _, test := range table {
		actual, err := ExpandPath(test.Input)
		if (err != nil) != test.Err {
			t.Fatalf("Input: %#v\n\nErr: %s", test.Input, err)
		}
		if actual != test.Output {
			t.Fatalf("Input: %#v\n\nOutput: %#v", test.Input, actual)
		}
	}
}
