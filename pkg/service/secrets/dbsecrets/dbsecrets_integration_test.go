// Copyright (C) 2017 ScyllaDB

// +build all integration

package dbsecrets_test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/pkg/schema/table"
	"github.com/scylladb/mermaid/pkg/service"
	"github.com/scylladb/mermaid/pkg/service/secrets/dbsecrets"
	. "github.com/scylladb/mermaid/pkg/testutils"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

const truthKey = "truth"

type Truth struct {
	ClusterID uuid.UUID
	Answer    string `json:"answer"`
}

func (t *Truth) Key() (clusterID uuid.UUID, key string) {
	return t.ClusterID, truthKey
}

func (t *Truth) MarshalBinary() (data []byte, err error) {
	if t.Answer == "" {
		return nil, nil
	}
	return []byte(t.Answer), nil
}

func (t *Truth) UnmarshalBinary(data []byte) error {
	t.Answer = string(data)
	return nil
}

func TestStorageIntegration(t *testing.T) {
	session := CreateSession(t)

	s, err := dbsecrets.New(session)
	if err != nil {
		t.Fatal(err)
	}

	setup := func(t *testing.T) {
		t.Helper()
		ExecStmt(t, session, fmt.Sprintf("TRUNCATE %s", table.Secrets.Name()))
	}

	clusterID := uuid.MustRandom()
	answer := "42"
	truth := &Truth{
		ClusterID: clusterID,
		Answer:    answer,
	}

	t.Run("Put new secret", func(t *testing.T) {
		setup(t)
		if err := s.Put(truth); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Get existing secret", func(t *testing.T) {
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
			t.Fatal("invalid secret value")
		}
	})

	t.Run("ErrNotFound is returned on non existing secret lookup", func(t *testing.T) {
		setup(t)
		if err := s.Get(truth); err == nil || err != service.ErrNotFound {
			t.Fatal("expected to get NotFound error")
		}
	})

	t.Run("Update secret", func(t *testing.T) {
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
			t.Fatal("invalid secret value")
		}
	})

	t.Run("ErrNotFound is returned on deleted secret lookup", func(t *testing.T) {
		setup(t)
		if err := s.Put(truth); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(truth); err != nil {
			t.Fatal(err)
		}
		if err := s.Get(truth); err == nil || err != service.ErrNotFound {
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
		if err := s.Get(truth); err == nil || err != service.ErrNotFound {
			t.Fatal("expected to get NotFound error")
		}
	})
}
