// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"testing"

	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

func TestResolverFind(t *testing.T) {
	setup := []string{
		"02b3bc0a-9973-408e-828d-c29fe4292edd",
		"0d257f4b-25a5-4e49-b26e-418383eb200b",
		"4aa2a2c1-8fed-47db-96e7-4ffad1007a0d",
		"9c961ba1-0b98-44d5-aff1-e1bfd53f0f43",
		"9ea60b8a-00b1-4662-8293-4bec9311fbda",
		"c0db1e4d-6aae-4a08-9aa7-8863afca16f6",
		"eb831839-4860-40e4-a35d-e4847f03d9cb",
	}

	table := []struct {
		Pre   string
		Match bool
	}{
		{
			Pre:   "99999999",
			Match: false,
		},
		{
			Pre:   "4aa2a2c1",
			Match: true,
		},
		{
			Pre:   "eb831839-4860-40e4-a35d-e4847f03d9cb",
			Match: true,
		},
		{
			Pre:   "eb831839-4860-40e4-a35d-e4847f03d9cbXXXXX",
			Match: false,
		},
	}

	r := newResolver()
	for _, s := range setup {
		r.Put(taskInfo{TaskID: uuid.MustParse(s)})
	}

	for i := range table {
		test := table[i]
		t.Run(test.Pre, func(t *testing.T) {
			if _, ok := r.Find(test.Pre); ok != test.Match {
				t.Fatalf("Find() %v, expected %v", ok, test.Match)
			}
		})
	}
}

func TestResolverFindByName(t *testing.T) {
	setup := []string{
		"foo",
		"bar",
		"foobar",
	}

	table := []struct {
		Pre   string
		Match bool
	}{
		{
			Pre:   "foo",
			Match: true,
		},
		{
			Pre:   "foob",
			Match: true,
		},
		{
			Pre:   "bar",
			Match: true,
		},
		{
			Pre:   "f",
			Match: false,
		},
	}

	r := newResolver()
	for _, s := range setup {
		r.Put(taskInfo{TaskID: uuid.MustRandom(), TaskName: s})
	}

	for i := range table {
		test := table[i]
		t.Run(test.Pre, func(t *testing.T) {
			if _, ok := r.Find(test.Pre); ok != test.Match {
				t.Fatalf("Find() %v, expected %v", ok, test.Match)
			}
		})
	}
}

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

	if _, ok := r.Find("name"); ok {
		t.Fatalf("Name not removed")
	}
	if v, _ := r.Find("new"); v != ti {
		t.Fatalf("Find() = %v, expected %v", v, ti)
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

	if eid, _ := r.Find(ti0.TaskID.String()); eid != ti0 {
		t.Fatalf("Find() = %v, expected %v", eid, ti0)
	}
}
