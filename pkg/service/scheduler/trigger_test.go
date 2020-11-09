// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"testing"

	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func TestTrigger(t *testing.T) {
	task := &Task{
		ClusterID: uuid.MustRandom(),
		Type:      mockTask,
		ID:        uuid.MustRandom(),
		Enabled:   true,
	}

	t.Run("run cancel pending", func(t *testing.T) {
		tg := newTrigger(task)
		if !tg.Run() {
			t.Fatal("Run() failed")
		}
		if tg.CancelPending() {
			t.Fatal("CancelPending() unexpected success")
		}
	})

	t.Run("cancel cancel pending", func(t *testing.T) {
		tg := newTrigger(task)
		if !tg.Cancel() {
			t.Fatal("Cancel() failed")
		}
		if tg.CancelPending() {
			t.Fatal("CancelPending() unexpected success")
		}
	})

	t.Run("run twice", func(t *testing.T) {
		tg := newTrigger(task)
		if !tg.Run() {
			t.Fatal("Run() failed")
		}
		if tg.Run() {
			t.Fatal("Run() unexpected success")
		}
	})
}
