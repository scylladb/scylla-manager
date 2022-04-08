// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestResolverChangeName(t *testing.T) {
	ti := taskInfo{
		ClusterID: uuid.MustRandom(),
		TaskID:    uuid.MustRandom(),
		TaskName:  "name",
	}
	r := newResolver()
	r.Put(ti)
	ti.TaskName = "new name"
	r.Put(ti)

	v := ti
	v.TaskName = "name"
	if r.FillTaskID(&v) {
		t.Fatalf("Name not removed")
	}
	v.TaskName = "new name"
	if !r.FillTaskID(&v) {
		t.Fatalf("FillTaskID() = %v, expected %v", v, ti)
	}
}

func TestResolverNameUUIDHijacking(t *testing.T) {
	ti0 := taskInfo{
		ClusterID: uuid.MustRandom(),
		TaskID:    uuid.MustRandom(),
	}
	ti1 := taskInfo{
		ClusterID: uuid.MustRandom(),
		TaskID:    uuid.MustRandom(),
		TaskName:  ti0.TaskID.String(),
	}
	r := newResolver()
	r.Put(ti0)
	r.Put(ti1)

	if eid, _ := r.FindByID(ti0.TaskID); eid != ti0 {
		t.Fatalf("FindByID() = %v, expected %v", eid, ti0)
	}
}
