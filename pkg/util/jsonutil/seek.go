// Copyright (C) 2017 ScyllaDB

package jsonutil

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
)

// ErrKeyNotFound is an error if a key does not exist.
var ErrKeyNotFound = errors.New("key not found")

// Decoder is a wrapper around a json.Decoder.
type Decoder struct {
	*json.Decoder
}

// NewDecoder creates a new JSON decoder.
func NewDecoder(r io.Reader) Decoder {
	return Decoder{json.NewDecoder(r)}
}

// Seek seeks forward-only to a path in the JSON.
// This should always be used when the underlying Reader is at the beginning of the root JSON object.
// The reader must be reset to the start if seeking for multiple JSON objects.
func (d Decoder) Seek(path string) error {
	var (
		err   error
		level int

		splitPath = strings.Split(path, "/")
		depth     = len(splitPath) - 1
	)

	for t, err := d.Token(); err == nil; t, err = d.Token() {
		s, ok := t.(string)
		if !ok {
			continue
		}

		if s != splitPath[level] {
			if err := d.skip(); err != nil {
				return err
			}
			continue
		}

		t, err = d.Token()
		if err != nil {
			return err
		}

		if x, ok := t.(json.Delim); ok && (x == '{' || x == '[') {
			if level == depth {
				return nil
			}
			level++
		}
	}

	if err == nil {
		err = ErrKeyNotFound
	}

	return err
}

// Skip consumes the contents of the current JSON object
// or returns doing nothing if the current position is
// not the start of a struct or array.
func (d Decoder) skip() error {
	var (
		err   error
		level int
	)

	for t, err := d.Token(); err == nil; t, err = d.Token() {
		if x, ok := t.(json.Delim); ok {
			if x == '{' || x == '[' {
				level++
			} else if x == '}' || x == ']' {
				level--
			}
		}
		if level == 0 {
			return nil
		}
	}

	return err
}
