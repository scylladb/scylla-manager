// Copyright (C) 2017 ScyllaDB

package backup

import (
	"path"

	"github.com/scylladb/go-set/strset"
)

// fileSet is a customised set implementation for keeping full paths of files.
// It optimises memory usage of under assumption that the number of distinct
// directories is strictly less than the number of files.
type fileSet map[string]*strset.Set

func (fs fileSet) Add(item string) {
	s := fs.itemDirSet(item, true)
	s.Add(path.Base(item))
}

func (fs fileSet) Has(item string) bool {
	s := fs.itemDirSet(item, false)
	return s != nil && s.Has(path.Base(item))
}

func (fs fileSet) Remove(item string) {
	s := fs.itemDirSet(item, false)
	if s != nil {
		s.Remove(path.Base(item))
	}
}

func (fs fileSet) AddFiles(dir string, files []string) {
	fs.dirSet(dir, true).Add(files...)
}

func (fs fileSet) HasAnyFiles(dir string, files []string) bool {
	s := fs.dirSet(dir, false)
	return s != nil && s.HasAny(files...)
}

func (fs fileSet) RemoveFiles(dir string, files []string) {
	s := fs.dirSet(dir, false)
	if s != nil {
		s.Remove(files...)
	}
}

func (fs fileSet) Dirs() []string {
	var dirs []string
	for k, v := range fs {
		if !v.IsEmpty() {
			dirs = append(dirs, k)
		}
	}
	return dirs
}

func (fs fileSet) DirSet(dir string) *strset.Set {
	return fs.dirSet(dir, true)
}

func (fs fileSet) Size() int {
	size := 0
	for _, files := range fs {
		size += files.Size()
	}
	return size
}

func (fs fileSet) Each(f func(string) bool) {
	cont := true

	decorator := func(dir string) func(item string) bool {
		return func(item string) bool {
			cont = f(path.Join(dir, item))
			return cont
		}
	}

	for dir, files := range fs {
		files.Each(decorator(dir))
		if !cont {
			break
		}
	}
}

func (fs fileSet) itemDirSet(item string, create bool) *strset.Set {
	return fs.dirSet(path.Dir(item), create)
}

func (fs fileSet) dirSet(dir string, create bool) *strset.Set {
	s, ok := fs[dir]
	if !ok && create {
		s = strset.New()
		fs[dir] = s
	}
	return s
}
