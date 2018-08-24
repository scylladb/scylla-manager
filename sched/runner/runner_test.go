// Copyright (C) 2017 ScyllaDB

package runner

import "testing"

func TestStatusMarshalText(t *testing.T) {
	statuses := []Status{
		StatusNew,
		StatusStarting,
		StatusRunning,
		StatusStopping,
		StatusStopped,
		StatusDone,
		StatusError,
	}

	var v Status
	for i, s := range statuses {
		b, err := s.MarshalText()
		if err != nil {
			t.Error(err)
		}
		if err := v.UnmarshalText(b); err != nil {
			t.Error(err)
		}
		if v != s {
			t.Error(i, "expected", s, "got", v)
		}
	}
}
