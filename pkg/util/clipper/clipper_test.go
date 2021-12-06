// Copyright (C) 2017 ScyllaDB

package clipper

import (
	"bytes"
	"flag"
	"os"
	"path"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var update = flag.Bool("update", false, "update .golden files")

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestClipperSay(t *testing.T) {
	table := []struct {
		Name     string
		Lines    []string
		Expected string
	}{
		{
			Name:     "empty",
			Lines:    nil,
			Expected: "empty_message.golden.txt",
		},
		{
			Name:     "single line",
			Lines:    []string{"hello"},
			Expected: "single_line.golden.txt",
		},
		{
			Name:     "overflow",
			Lines:    []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
			Expected: "overflow.golden.txt",
		},
		{
			Name:     "full",
			Lines:    []string{"a", "b", "c", "d", "e", "f", "g"},
			Expected: "full.golden.txt",
		},
	}
	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			f, err := os.ReadFile(path.Join("testdata/", test.Expected))
			if err != nil {
				t.Fatal(err)
			}

			buf := &bytes.Buffer{}
			if err := Say(buf, test.Lines...); err != nil {
				t.Fatal(err)
			}

			if *update {
				if err := os.WriteFile(test.Expected, buf.Bytes(), 0o666); err != nil {
					t.Error(err)
				}
			}

			if diff := cmp.Diff(string(f), buf.String()); diff != "" {
				t.Error(diff)
			}
		})
	}
}
