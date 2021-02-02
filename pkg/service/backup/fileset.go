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
	s := fs.dirSet(item, true)
	s.Add(path.Base(item))
}

func (fs fileSet) Has(item string) bool {
	s := fs.dirSet(item, false)
	return s != nil && s.Has(path.Base(item))
}

func (fs fileSet) Remove(item string) {
	s := fs.dirSet(item, false)
	if s != nil {
		s.Remove(path.Base(item))
	}
}

func (fs fileSet) DirSet(dir string) *strset.Set {
	s, ok := fs[dir]
	if !ok {
		s = strset.New()
		fs[dir] = s
	}
	return s
}

func (fs fileSet) dirSet(item string, create bool) *strset.Set {
	s, ok := fs[path.Dir(item)]
	if !ok && create {
		s = strset.New()
		fs[path.Dir(item)] = s
	}
	return s
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
