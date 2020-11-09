// Copyright (C) 2017 ScyllaDB

package secrets

import (
	"testing"

	"github.com/scylladb/scylla-manager/pkg/service"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

type testSecret []byte

func newTestSecret(version byte) *testSecret {
	return &testSecret{version}
}

func (v *testSecret) Key() (clusterID uuid.UUID, key string) {
	return uuid.Nil, "test"
}

func (v *testSecret) MarshalBinary() (data []byte, err error) {
	return *v, nil
}

func (v *testSecret) UnmarshalBinary(data []byte) error {
	*v = data
	return nil
}

type testStore []byte

var _ Store = &testStore{}

func (t *testStore) Put(secret KeyValue) error {
	v, err := secret.MarshalBinary()
	if err != nil {
		return err
	}
	*t = v
	return nil
}

func (t *testStore) Get(secret KeyValue) error {
	if len(*t) == 0 {
		return service.ErrNotFound
	}
	return secret.UnmarshalBinary(*t)
}

const deleted = 0xf

func (t *testStore) Delete(secret KeyValue) error {
	*t = testStore{deleted}
	return nil
}

func (t *testStore) DeleteAll(clusterID uuid.UUID) error {
	*t = testStore{deleted}
	return nil
}

func (t *testStore) version() byte {
	if len(*t) == 0 {
		return 0
	}
	return []byte(*t)[0]
}

func TestPutWithRollback(t *testing.T) {
	t.Parallel()

	t.Run("delete", func(t *testing.T) {
		s := &testStore{}
		r, err := PutWithRollback(s, newTestSecret(1))
		if err != nil {
			t.Fatal("PutWithRollback() error ", err)
		}
		if s.version() != 1 {
			t.Fatal("Wrong version", s.version())
		}
		r()
		if s.version() != deleted {
			t.Fatalf("Got version %d, expected deleted", s.version())
		}
	})

	t.Run("update", func(t *testing.T) {
		s := &testStore{}
		r, err := PutWithRollback(s, newTestSecret(1))
		if err != nil {
			t.Fatal("PutWithRollback() error ", err)
		}
		r, err = PutWithRollback(s, newTestSecret(2))
		if err != nil {
			t.Fatal("PutWithRollback() error ", err)
		}
		if s.version() != 2 {
			t.Fatal("Wrong version", s.version())
		}
		r()
		if s.version() != 1 {
			t.Fatal("Wrong version", s.version())
		}
	})
}
