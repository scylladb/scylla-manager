// Copyright (C) 2017 ScyllaDB

package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
	"gopkg.in/yaml.v2"
)

var update = flag.Bool("update", false, "update .golden files")

func TestConfigParse(t *testing.T) {
	table := []struct {
		Name   string
		Input  string
		Golden string
	}{
		{
			Name:   "basic",
			Input:  "./testdata/config/basic.input.yaml",
			Golden: "./testdata/config/basic.golden.yaml",
		},
		{
			Name:   "scylla overwrite",
			Input:  "./testdata/config/scylla_overwrite.input.yaml",
			Golden: "./testdata/config/scylla_overwrite.golden.yaml",
		},
		{
			Name:   "https overwrite",
			Input:  "./testdata/config/https_overwrite.input.yaml",
			Golden: "./testdata/config/https_overwrite.golden.yaml",
		},
		{
			Name:   "debug overwrite",
			Input:  "./testdata/config/debug_overwrite.input.yaml",
			Golden: "./testdata/config/debug_overwrite.golden.yaml",
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			c, err := parseConfig(test.Input)
			if err != nil {
				t.Fatal(err)
			}
			buf := bytes.Buffer{}
			if err := yaml.NewEncoder(&buf).Encode(c); err != nil {
				t.Fatal(err)
			}

			if *update {
				if err := ioutil.WriteFile(test.Golden, buf.Bytes(), 0666); err != nil {
					t.Error(err)
				}
			}

			golden, err := ioutil.ReadFile(test.Golden)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(buf.Bytes(), golden); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
