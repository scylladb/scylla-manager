// Copyright (C) 2024 ScyllaDB

//go:build all || integration

package backup

type dthHelper struct {
	before func()
	after  func(skipped, uploaded, size int64)
}

func (d dthHelper) beforeDeduplicateHost() {
	d.before()
}

func (d dthHelper) afterDeduplicateHost(skipped, uploaded, size int64) {
	d.after(skipped, uploaded, size)
}

func (s *Service) SetDeduplicateTestHooks(before func(), after func(skipped, uploaded, size int64)) {
	s.dth = dthHelper{
		before: before,
		after:  after,
	}
}

func (s *Service) RemoveDeduplicateTestHooks() {
	s.dth = nil
}
