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
			Pre:   "9",
			Match: false,
		},
		{
			Pre:   "4a",
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
		r.Put(extTaskID{TaskID: uuid.MustParse(s)})
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
		r.Put(extTaskID{TaskID: uuid.MustRandom(), TaskName: s})
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
	eid := extTaskID{
		ClusterID: uuid.MustRandom(),
		TaskID:    uuid.MustRandom(),
		TaskName:  "name",
	}
	r := newResolver()
	r.Put(eid)
	eid.TaskName = "new name"
	r.Put(eid)

	if _, ok := r.Find("name"); ok {
		t.Fatalf("Name not removed")
	}
	if v, _ := r.Find("new"); v != eid {
		t.Fatalf("Find() = %v, expected %v", v, eid)
	}
}

func TestResolverNameUUIDHijacking(t *testing.T) {
	eid0 := extTaskID{
		ClusterID: uuid.MustRandom(),
		TaskID:    uuid.MustRandom(),
	}
	eid1 := extTaskID{
		ClusterID: uuid.MustRandom(),
		TaskID:    uuid.MustRandom(),
		TaskName:  eid0.TaskID.String(),
	}
	r := newResolver()
	r.Put(eid0)
	r.Put(eid1)

	if eid, _ := r.Find(eid0.TaskID.String()); eid != eid0 {
		t.Fatalf("Find() = %v, expected %v", eid, eid0)
	}
}
