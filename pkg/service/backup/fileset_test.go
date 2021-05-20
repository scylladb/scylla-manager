// Copyright (C) 2017 ScyllaDB

package backup

import (
	"testing"
)

func TestFilesSet(t *testing.T) {
	fs := make(fileSet)
	files := []string{
		"/a/b/c",
		"/a/b/d",
		"/a/b/e",
		"/z/o/o",
	}

	t.Run("has", func(t *testing.T) {
		for _, f := range files {
			fs.Add(f)
		}
		for _, f := range files {
			if !fs.Has(f) {
				t.Errorf("fs.Has(%s) = false, expected true", f)
			}
		}
		if fs.Has("/a/b") {
			t.Errorf("Unexpected success")
		}
	})

	t.Run("remove", func(t *testing.T) {
		fs.Remove(files[0])
		if fs.Has(files[0]) {
			t.Errorf("Unexpected success")
		}
	})

	t.Run("dirs", func(t *testing.T) {
		if len(fs.Dirs()) != 2 {
			t.Errorf("Dirs() = %s, expected %d elements", fs.Dirs(), 2)
		}
	})

	t.Run("size", func(t *testing.T) {
		if fs.Size() != 3 {
			t.Errorf("Size() = %d, expected %d", fs.Size(), 3)
		}
	})

	t.Run("each", func(t *testing.T) {
		var all []string
		fs.Each(func(s string) bool {
			all = append(all, s)
			return true
		})
		if len(all) != fs.Size() {
			t.Errorf("Each() = %s, expected %d items", all, fs.Size())
		}
	})

	t.Run("each break", func(t *testing.T) {
		var all []string
		fs.Each(func(s string) bool {
			all = append(all, s)
			return false
		})
		if len(all) != 1 {
			t.Errorf("Each() = %s, expected %d items", all, 1)
		}
	})
}
