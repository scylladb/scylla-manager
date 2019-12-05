// Copyright (C) 2017 ScyllaDB

package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExpand(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal("UserHomeDir() error", err)
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
			filepath.Join(home, "foo"),
			false,
		},

		{
			"",
			"",
			false,
		},

		{
			"~",
			home,
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
