// Copyright (C) 2017 ScyllaDB

package kv

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/uuid"
)

func TestFsStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "mermaid.internal.kv.TestFsStore")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.Remove(dir)
	}()
	s, err := NewFsStore(dir, "test")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("not found", func(t *testing.T) {
		if _, err := s.Get(uuid.MustRandom()); err != mermaid.ErrNotFound {
			t.Fatal(err)
		}
	})

	t.Run("get", func(t *testing.T) {
		id := uuid.MustRandom()
		data := []byte("test")

		if err := s.Put(id, data); err != nil {
			t.Fatal(err)
		}
		v, err := s.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(v, data); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("update", func(t *testing.T) {
		id := uuid.MustRandom()
		data0 := []byte("test0")
		data1 := []byte("test1")

		if err := s.Put(id, data0); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(id, data1); err != nil {
			t.Fatal(err)
		}
		v, err := s.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(v, data1); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("delete", func(t *testing.T) {
		id := uuid.MustRandom()
		data := []byte("test")

		if err := s.Put(id, data); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(id, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(id); err != mermaid.ErrNotFound {
			t.Fatal(err)
		}
	})
}
