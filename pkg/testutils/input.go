// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"
)

// ReadInputFile reads test input file.
func ReadInputFile(tb testing.TB) []byte {
	tb.Helper()

	b, err := ioutil.ReadFile(inputJSONFileName(tb))
	if err != nil {
		tb.Fatal(err)
	}

	return b
}

// ReadInputJSONFile reads test input file and unmarshall it into 'v'.
func ReadInputJSONFile(tb testing.TB, v interface{}) {
	tb.Helper()

	if err := json.Unmarshal(ReadInputFile(tb), v); err != nil {
		tb.Fatal(err)
	}
}

func inputJSONFileName(tb testing.TB) string {
	tb.Helper()
	name := tb.Name()
	name = strings.TrimPrefix(name, "Test")
	return "testdata/" + name + ".input.json"
}
