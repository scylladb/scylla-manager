// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/scylladb/mermaid/uuid"
)

//go:generate mockgen -destination mock_kv_test.go -mock_names Store=MockStore -package cluster github.com/scylladb/mermaid/internal/kv Store

func TestPutWithRollback(t *testing.T) {
	t.Parallel()

	id := uuid.MustRandom()
	v0 := []byte{0}
	v1 := []byte{1}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := NewMockStore(ctrl)
	gomock.InOrder(
		m.EXPECT().Get(id).Return(v0, nil),
		m.EXPECT().Put(id, v1).Return(nil),
		m.EXPECT().Put(id, v0).Return(nil),
	)

	r, err := putWithRollback(m, id, v1)
	if err != nil {
		t.Fatal(err)
	}
	r()
}
