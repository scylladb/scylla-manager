// Copyright (C) 2017 ScyllaDB

package schema

// Init exports init for test purposes.
func (t Table) Init() Table {
	return t.init()
}
