// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"
)

// ReadInputFile reads test input file.
func ReadInputFile(t testing.TB) []byte {
	t.Helper()

	b, err := ioutil.ReadFile(inputJSONFileName(t))
	if err != nil {
		t.Fatal(err)
	}

	return b
}

// ReadInputJSONFile reads test input file and unmarshall it into 'v'.
func ReadInputJSONFile(t testing.TB, v interface{}) {
	t.Helper()

	if err := json.Unmarshal(ReadInputFile(t), v); err != nil {
		t.Fatal(err)
	}
}

func inputJSONFileName(t testing.TB) string {
	name := t.Name()
	name = strings.TrimPrefix(name, "Test")
	return "testdata/" + name + ".input.json"
}
