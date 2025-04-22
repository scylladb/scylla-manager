// Copyright (C) 2025 ScyllaDB

package maps

import (
	"errors"
	"fmt"
	"maps"
	"testing"
)

func TestSetFromSlice(t *testing.T) {
	testCases := []struct {
		name string
		in   []string
		out  map[string]struct{}
	}{
		{
			name: "empty",
			in:   []string{},
			out:  map[string]struct{}{},
		},
		{
			name: "simple",
			in:   []string{"a", "b", "c"},
			out: map[string]struct{}{
				"a": {},
				"b": {},
				"c": {},
			},
		},
		{
			name: "duplicates",
			in:   []string{"a", "b", "b", "c"},
			out: map[string]struct{}{
				"a": {},
				"b": {},
				"c": {},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := SetFromSlice(tc.in)
			if !maps.Equal(tc.out, out) {
				t.Fatalf("Got: %v, expected: %v", out, tc.out)
			}
		})
	}
}

func TestMapKeys(t *testing.T) {
	testCases := []struct {
		name string
		in   map[int]string
		f    func(int) string
		out  map[string]string
	}{
		{
			name: "empty",
			in:   map[int]string{},
			out:  map[string]string{},
		},
		{
			name: "simple",
			in: map[int]string{
				1: "a",
				2: "b",
				3: "c",
			},
			f: func(v int) string {
				return fmt.Sprint(v)
			},
			out: map[string]string{
				"1": "a",
				"2": "b",
				"3": "c",
			},
		},
		{
			name: "duplicates",
			in: map[int]string{
				1: "a",
				2: "a",
				3: "a",
			},
			f: func(v int) string {
				return "dup"
			},
			out: map[string]string{
				"dup": "a",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := MapKey(tc.in, tc.f)
			if !maps.Equal(tc.out, out) {
				t.Fatalf("Got: %v, expected: %v", out, tc.out)
			}
		})
	}
}

func TestMapKeyWithError(t *testing.T) {
	mockErr := errors.New("mock error")
	testCases := []struct {
		name string
		in   map[int]string
		f    func(int) (string, error)
		out  map[string]string
		err  error
	}{
		{
			name: "empty",
			in:   map[int]string{},
			out:  map[string]string{},
		},
		{
			name: "simple",
			in: map[int]string{
				1: "a",
				2: "b",
				3: "c",
			},
			f: func(v int) (string, error) {
				return fmt.Sprint(v), nil
			},
			out: map[string]string{
				"1": "a",
				"2": "b",
				"3": "c",
			},
		},
		{
			name: "duplicates",
			in: map[int]string{
				1: "a",
				2: "a",
				3: "a",
			},
			f: func(v int) (string, error) {
				return "dup", nil
			},
			out: map[string]string{
				"dup": "a",
			},
		},
		{
			name: "error",
			in: map[int]string{
				1: "a",
				2: "b",
				3: "c",
			},
			f: func(v int) (string, error) {
				if v == 2 {
					return "", mockErr
				}
				return fmt.Sprint(v), nil
			},
			err: mockErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out, err := MapKeyWithError(tc.in, tc.f)
			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("Got: %v, expected: %v", err, tc.err)
				}
			} else {
				if !maps.Equal(tc.out, out) {
					t.Fatalf("Got: %v, expected: %v", out, tc.out)
				}
			}
		})
	}
}

func TestHasAnyKey(t *testing.T) {
	testCases := []struct {
		name  string
		in    map[string]struct{}
		items []string
		out   bool
	}{
		{
			name:  "empty",
			in:    map[string]struct{}{},
			items: []string{"a"},
			out:   false,
		},
		{
			name: "single true",
			in: map[string]struct{}{
				"a": {},
				"b": {},
				"c": {},
			},
			items: []string{"b"},
			out:   true,
		},
		{
			name: "single false",
			in: map[string]struct{}{
				"a": {},
				"b": {},
				"c": {},
			},
			items: []string{"d"},
			out:   false,
		},
		{
			name: "multiple true",
			in: map[string]struct{}{
				"a": {},
				"b": {},
				"c": {},
			},
			items: []string{"d", "a", "e"},
			out:   true,
		},
		{
			name: "duplicates",
			in: map[string]struct{}{
				"a": {},
				"b": {},
				"c": {},
			},
			items: []string{"d", "d", "c", "c"},
			out:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := HasAnyKey(tc.in, tc.items...)
			if tc.out != out {
				t.Fatalf("Got: %v, expected: %v", out, tc.out)
			}
		})
	}
}
