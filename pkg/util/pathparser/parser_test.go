// Copyright (C) 2017 ScyllaDB

package pathparser

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/scylladb/mermaid/pkg/util/uuid"
)

func TestPartParser(t *testing.T) {
	t.Parallel()

	goldenID := uuid.MustRandom()
	goldenString := "42"
	goldenInt := 42
	p := New(fmt.Sprintf("static/%s/%s/%d", goldenID.String(), goldenString, goldenInt), "/")

	var s string
	var id uuid.UUID
	var i int

	customIntParser := func(ptr *int) func(v string) error {
		return func(v string) error {
			var err error
			*ptr, err = strconv.Atoi(v)
			return err
		}
	}

	parsers := []Parser{
		Static("static"),
		ID(&id),
		String(&s),
		customIntParser(&i),
	}

	if err := p.Parse(parsers...); err != nil {
		t.Fatal(err)
	}

	if s != goldenString {
		t.Fatalf("Expected string to equal %s, got %s", goldenString, s)
	}
	if id.String() != goldenID.String() {
		t.Fatalf("Expected id to equal %s, got %s", goldenID.String(), id.String())
	}
	if i != goldenInt {
		t.Fatalf("Expected int to equal %d, got %d", goldenInt, i)
	}
}

func TestPartParserErrorCases(t *testing.T) {
	ts := []struct {
		Name    string
		Value   string
		Parsers []Parser
	}{
		{
			Name:    "Different static value",
			Value:   "asdf",
			Parsers: []Parser{Static("static")},
		},
		{
			Name:    "Not parsable uuid",
			Value:   "aa-aa",
			Parsers: []Parser{ID(&uuid.Nil)},
		},
	}

	for i := range ts {
		test := ts[i]
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			p := New(test.Value, "/")
			if err := p.Parse(test.Parsers...); err == nil {
				t.Fatal("expected to get error, got nil")
			}
		})
	}
}
