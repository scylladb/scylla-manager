// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

//go:generate mockgen -destination mock_policy_test.go -mock_names Policy=mockPolicy -package scheduler github.com/scylladb/scylla-manager/v3/pkg/service/scheduler Policy
//go:generate mockgen -destination mock_runner_test.go -mock_names Runner=mockRunner -package scheduler github.com/scylladb/scylla-manager/v3/pkg/service/scheduler Runner

func TestPolicyRunner(t *testing.T) {
	t.Run("policy error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := uuid.MustRandom()
		k := uuid.MustRandom()
		r := uuid.MustRandom()
		e := errors.New("test")

		mp := NewmockPolicy(ctrl)
		mp.EXPECT().PreRun(c, k, r).Return(e)
		mr := NewmockRunner(ctrl)

		p := PolicyRunner{
			Policy: mp,
			Runner: mr,
		}
		if err := p.Run(context.Background(), c, k, r, nil); err != e {
			t.Fatal("expected", e, "got", err)
		}
	})

	t.Run("runner error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		c := uuid.MustRandom()
		k := uuid.MustRandom()
		r := uuid.MustRandom()
		e := errors.New("test")

		mp := NewmockPolicy(ctrl)
		gomock.InOrder(
			mp.EXPECT().PreRun(c, k, r).Return(nil),
			mp.EXPECT().PostRun(c, k, r),
		)
		mr := NewmockRunner(ctrl)
		mr.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(e)

		p := PolicyRunner{
			Policy: mp,
			Runner: mr,
		}
		if err := p.Run(context.Background(), c, k, r, nil); err != e {
			t.Fatal("expected", e, "got", err)
		}
	})
}

func TestNewLockClusterPolicy(t *testing.T) {
	c := uuid.MustRandom()
	k := uuid.MustRandom()
	r := uuid.MustRandom()
	p := NewLockClusterPolicy()

	if err := p.PreRun(c, k, r); err != nil {
		t.Fatal(err)
	}

	if err := p.PreRun(c, uuid.MustRandom(), uuid.MustRandom()); err == nil {
		t.Fatal("expected error")
	}

	if err := p.PreRun(uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()); err != nil {
		t.Fatal(err)
	}

	p.PostRun(c, uuid.MustRandom(), uuid.MustRandom())

	if err := p.PreRun(c, uuid.MustRandom(), uuid.MustRandom()); err != nil {
		t.Fatal(errClusterBusy)
	}
}
