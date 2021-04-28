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
func SaveGoldenJSONFileIfNeeded(tb testing.TB, v interface{}) {
	tb.Helper()

	if !UpdateGoldenFiles() {
		return
	}

	b, err := json.Marshal(v)
	if err != nil {
		tb.Fatal(err)
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, b, "", "  "); err != nil {
		tb.Fatal(err)
	}

	if err := os.MkdirAll(path.Dir(goldenJSONFileName(tb)), 0777); err != nil {
		tb.Fatal(err)
	}
	if err := ioutil.WriteFile(goldenJSONFileName(tb), buf.Bytes(), 0666); err != nil {
		tb.Error(err)
	}
}

// LoadGoldenJSONFile loads files written by SaveGoldenJSONFileIfNeeded.
func LoadGoldenJSONFile(tb testing.TB, v interface{}) {
	tb.Helper()

	b, err := ioutil.ReadFile(goldenJSONFileName(tb))
	if err != nil {
		tb.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		tb.Fatal(err)
	}
}

func goldenJSONFileName(tb testing.TB) string {
	tb.Helper()
	name := tb.Name()
	name = strings.TrimPrefix(name, "Test")
	return "testdata/" + name + ".golden.json"
}

// SaveGoldenTextFileIfNeeded puts s to a new file named after test name.
func SaveGoldenTextFileIfNeeded(tb testing.TB, s string) {
	tb.Helper()

	if !UpdateGoldenFiles() {
		return
	}

	if err := os.MkdirAll(path.Dir(goldenTextFileName(tb)), 0777); err != nil {
		tb.Fatal(err)
	}
	if err := ioutil.WriteFile(goldenTextFileName(tb), []byte(s), 0666); err != nil {
		tb.Error(err)
	}
}

// LoadGoldenTextFile loads files written by SaveGoldenTextFileIfNeeded.
func LoadGoldenTextFile(tb testing.TB) string {
	tb.Helper()

	b, err := ioutil.ReadFile(goldenTextFileName(tb))
	if err != nil {
		tb.Fatal(err)
	}
	return string(b)
}

func goldenTextFileName(tb testing.TB) string {
	tb.Helper()
	name := tb.Name()
	name = strings.TrimPrefix(name, "Test")
	return "testdata/" + name + ".golden.txt"
}
