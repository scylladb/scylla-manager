// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
)

var flagUpdate = flag.Bool("update", false, "update .golden files")

// UpdateGoldenFiles true integration tests that support it should update their
// golden files.
func UpdateGoldenFiles() bool {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagUpdate
}

// SaveGoldenJSONFileIfNeeded puts v as JSON to a new file named after test
// name in directory dir.
func SaveGoldenJSONFileIfNeeded(t testing.TB, v interface{}) {
	if !UpdateGoldenFiles() {
		return
	}

	t.Helper()

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, b, "", "  "); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(path.Dir(goldenJSONFileName(t)), 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(goldenJSONFileName(t), buf.Bytes(), 0666); err != nil {
		t.Error(err)
	}
}

// LoadGoldenJSONFile loads files written by SaveGoldenJSONFileIfNeeded.
func LoadGoldenJSONFile(t testing.TB, v interface{}) {
	t.Helper()

	b, err := ioutil.ReadFile(goldenJSONFileName(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

func goldenJSONFileName(t testing.TB) string {
	name := t.Name()
	name = strings.TrimPrefix(name, "Test")
	return "testdata/" + name + ".golden.json"
}
