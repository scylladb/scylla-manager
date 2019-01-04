// Copyright (C) 2017 ScyllaDB

package kv

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/uuid"
)

// FsStore stores data as files in a given directory. The directory structure
// is flat, files are saved in 0400 mode.
type FsStore struct {
	dir string
	ext string
}

// NewFsStore creates a new FsStore.
func NewFsStore(dir string, ext string) (*FsStore, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if ext != "" {
		ext = "." + ext
	}

	return &FsStore{dir: dir, ext: ext}, nil
}

// Get returns saved data, if file is not found ErrNotFound is reported.
func (m *FsStore) Get(id uuid.UUID) ([]byte, error) {
	filename := m.path(id)

	b, err := ioutil.ReadFile(filename)
	if os.IsNotExist(err) {
		err = mermaid.ErrNotFound
	}
	return b, err
}

// Put updates or deletes file with a given ID.
func (m *FsStore) Put(id uuid.UUID, data []byte) error {
	if len(data) == 0 {
		return m.delete(id)
	}
	return m.save(id, data)
}

func (m *FsStore) save(id uuid.UUID, data []byte) error {
	if err := ioutil.WriteFile(m.path(id), data, 0600); err != nil {
		return errors.Wrapf(err, "unable to store identity file %q", m.path(id))
	}
	return nil
}

func (m *FsStore) delete(id uuid.UUID) error {
	if err := os.Remove(m.path(id)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (m *FsStore) path(id uuid.UUID) string {
	return filepath.Join(m.dir, id.String()+m.ext)
}
