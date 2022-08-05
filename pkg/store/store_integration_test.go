// Copyright (C) 2017 ScyllaDB

//go:build all || integration
// +build all integration

package store_test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/service"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

const questionKey = "Ultimate Question of Life, the Universe, and Everything"

type question struct {
	ClusterID uuid.UUID
	Answer    string `json:"answer"`
}

func (q *question) Key() (clusterID uuid.UUID, key string) {
	return q.ClusterID, questionKey
}

func (q *question) MarshalBinary() (data []byte, err error) {
	if q.Answer == "" {
		return nil, nil
	}
	return []byte(q.Answer), nil
}

func (q *question) UnmarshalBinary(data []byte) error {
	q.Answer = string(data)
	return nil
}

func TestStorageIntegration(t *testing.T) {
	session := CreateScyllaManagerDBSession(t)

	s := store.NewTableStore(session, table.Drawer)

	setup := func(t *testing.T) {
		t.Helper()
		ExecStmt(t, session, fmt.Sprintf("TRUNCATE %s", table.Drawer.Name()))
	}

	clusterID := uuid.MustRandom()
	answer := "42"
	truth := &question{
		ClusterID: clusterID,
		Answer:    answer,
	}

	t.Run("Put new entry", func(t *testing.T) {
		setup(t)
		if err := s.Put(truth); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Get existing entry", func(t *testing.T) {
		setup(t)
		if err := s.Put(truth); err != nil {
			t.Fatal(err)
		}
		if err := s.Get(truth); err != nil {
			t.Fatal(err)
		}
		if cmp.Diff(truth.ClusterID, clusterID, UUIDComparer()) != "" {
			t.Fatal("invalid cluster id")
		}

		if cmp.Diff(truth.Answer, answer) != "" {
			t.Fatal("invalid entry value")
		}
	})

	t.Run("ErrNotFound is returned on non existing entry lookup", func(t *testing.T) {
		setup(t)
		if err := s.Get(truth); err == nil || !errors.Is(err, service.ErrNotFound) {
			t.Fatal("expected to get NotFound error")
		}
	})

	t.Run("Update entry", func(t *testing.T) {
		setup(t)

		if err := s.Put(truth); err != nil {
			t.Fatal(err)
		}
		answer = "1337"
		truth.Answer = answer
		if err := s.Put(truth); err != nil {
			t.Fatal(err)
		}
		if err := s.Get(truth); err != nil {
			t.Fatal(err)
		}

		if cmp.Diff(truth.Answer, answer) != "" {
			t.Fatal("invalid entry value")
		}
	})

	t.Run("ErrNotFound is returned on deleted entry lookup", func(t *testing.T) {
		setup(t)
		if err := s.Put(truth); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(truth); err != nil {
			t.Fatal(err)
		}
		if err := s.Get(truth); err == nil || !errors.Is(err, service.ErrNotFound) {
			t.Fatal("expected to get NotFound error")
		}
	})

	t.Run("DeleteAll deletes all values associated to given cluster", func(t *testing.T) {
		setup(t)
		if err := s.Put(truth); err != nil {
			t.Fatal(err)
		}
		truth.Answer = "1337"
		if err := s.Put(truth); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteAll(clusterID); err != nil {
			t.Fatal(err)
		}
		if err := s.Get(truth); err == nil || !errors.Is(err, service.ErrNotFound) {
			t.Fatal("expected to get NotFound error")
		}
	})
}
