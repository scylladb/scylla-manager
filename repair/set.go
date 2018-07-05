// Copyright (C) 2017 ScyllaDB

package repair

import "github.com/fatih/set"

func newSet(items []string) set.Interface {
	s := set.New(set.NonThreadSafe)
	for _, item := range items {
		s.Add(item)
	}
	return s
}
