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

// SaveGoldenJSONFileIfNeeded puts v as JSON to a new file named after test name.
func SaveGoldenJSONFileIfNeeded(t testing.TB, v interface{}) {
	t.Helper()

	if !UpdateGoldenFiles() {
		return
	}

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

// SaveGoldenTextFileIfNeeded puts s to a new file named after test name.
func SaveGoldenTextFileIfNeeded(t testing.TB, s string) {
	t.Helper()

	if !UpdateGoldenFiles() {
		return
	}

	if err := os.MkdirAll(path.Dir(goldenTextFileName(t)), 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(goldenTextFileName(t), []byte(s), 0666); err != nil {
		t.Error(err)
	}
}

// LoadGoldenTextFile loads files written by SaveGoldenTextFileIfNeeded.
func LoadGoldenTextFile(t testing.TB) string {
	t.Helper()

	b, err := ioutil.ReadFile(goldenTextFileName(t))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func goldenTextFileName(t testing.TB) string {
	name := t.Name()
	name = strings.TrimPrefix(name, "Test")
	return "testdata/" + name + ".golden.txt"
}
