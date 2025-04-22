// Copyright (C) 2025 ScyllaDB

package slices

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestMap(t *testing.T) {
	testCases := []struct {
		name string
		in   []int
		out  []string
	}{
		{
			name: "empty",
			in:   []int{},
			out:  []string{},
		},
		{
			name: "simple",
			in:   []int{1, 2, 3},
			out:  []string{"1", "2", "3"},
		},
		{
			name: "duplicates",
			in:   []int{1, 2, 2, 3},
			out:  []string{"1", "2", "2", "3"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := Map(tc.in, func(v int) string {
				return fmt.Sprint(v)
			})
			if !slices.Equal(tc.out, out) {
				t.Fatalf("Got: %v, expected: %v", out, tc.out)
			}
		})
	}
}

func TestMapWithError(t *testing.T) {
	mockErr := errors.New("mock error")
	testCases := []struct {
		name string
		in   []int
		f    func(int) (string, error)
		out  []string
		err  error
	}{
		{
			name: "empty",
			in:   []int{},
			out:  []string{},
		},
		{
			name: "simple",
			in:   []int{1, 2, 3},
			f: func(v int) (string, error) {
				return fmt.Sprint(v), nil
			},
			out: []string{"1", "2", "3"},
		},
		{
			name: "duplicates",
			in:   []int{1, 2, 2, 3},
			f: func(v int) (string, error) {
				return fmt.Sprint(v), nil
			},
			out: []string{"1", "2", "2", "3"},
		},
		{
			name: "error",
			in:   []int{1, 2, 3},
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
			out, err := MapWithError(tc.in, tc.f)
			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("Got: %v, expected: %v", err, tc.err)
				}
			} else {
				if !slices.Equal(tc.out, out) {
					t.Fatalf("Got: %v, expected: %v", out, tc.out)
				}
			}
		})
	}
}

type myInt int

func (i myInt) String() string {
	return fmt.Sprint(int(i))
}

func TestMapToString(t *testing.T) {
	testCases := []struct {
		name string
		in   []myInt
		out  []string
	}{
		{
			name: "empty",
			in:   []myInt{},
			out:  []string{},
		},
		{
			name: "simple",
			in:   []myInt{1, 2, 3},
			out:  []string{"1", "2", "3"},
		},
		{
			name: "duplicates",
			in:   []myInt{1, 2, 2, 3},
			out:  []string{"1", "2", "2", "3"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := MapToString(tc.in)
			if !slices.Equal(tc.out, out) {
				t.Fatalf("Got: %v, expected: %v", out, tc.out)
			}
		})
	}
}
