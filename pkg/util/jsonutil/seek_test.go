// Copyright (C) 2017 ScyllaDB

package jsonutil

import (
	"strings"
	"testing"
)

const sample = `
{
	"name": "Kevin",
	"cats": [
		"Newt"
	],
	"foods": {
		"fish": [
			"Tarbot",
			"Tuna"
		]
	},
	"pets":
	{
		"cats": [
			"Belle",
			"Pedro",
			"Cassidy",
			"dogs"
		],
		"fish": [
			"Billy"
		]
	},
	"dogs": [
		"Chloe",
		"Ellie",
		"Izzy",
		"Jessie",
		"Luna"
	]
}
`

type Person struct {
	Name string
	Dogs []string
	Cats []string
}

func TestSeekTopLevel(t *testing.T) {
	r := strings.NewReader(sample)
	decoder := NewDecoder(r)

	if err := decoder.Seek("dogs"); err != nil {
		t.Fatal(err)
	}

	var dogs []string
	for decoder.More() {
		var dog string
		if err := decoder.Decode(&dog); err != nil {
			t.Fatal(err)
		}

		dogs = append(dogs, dog)
	}

	if dogs[0] != "Chloe" {
		t.Fatalf("Expected Chloe, got %s", dogs[0])
	}

	if len(dogs) != 5 {
		t.Fatalf("Expected 5, got %d", len(dogs))
	}
}

func TestSeekNotFound(t *testing.T) {
	f := strings.NewReader(sample)
	decoder := NewDecoder(f)

	err := decoder.Seek("fish")

	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %s", err)
	}
}

func TestSeekNested(t *testing.T) {
	r := strings.NewReader(sample)
	decoder := NewDecoder(r)

	if err := decoder.Seek("pets/cats"); err != nil {
		t.Fatal(err)
	}

	var cats []string
	for decoder.More() {
		var cat string
		if err := decoder.Decode(&cat); err != nil {
			t.Fatal(err)
		}

		cats = append(cats, cat)
	}

	if cats[0] != "Belle" {
		t.Fatalf("Expected Chloe, got %s", cats[0])
	}

	if len(cats) != 4 {
		t.Fatalf("Expected 4, got %d", len(cats))
	}

	r.Reset(sample)

	if err := decoder.Seek("pets/fish"); err != nil {
		t.Fatal(err)
	}
	var fishes []string
	for decoder.More() {
		var fish string
		if err := decoder.Decode(&fish); err != nil {
			t.Fatal(err)
		}

		fishes = append(fishes, fish)
	}

	if fishes[0] != "Billy" {
		t.Fatalf("Expected Billy, got %s", fishes[0])
	}

	if len(fishes) != 1 {
		t.Fatalf("Expected 1, got %d", len(fishes))
	}
}
