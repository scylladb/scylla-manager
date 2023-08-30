// Copyright (C) 2023 ScyllaDB

package repair

// TablePreference describes partial predefined order in which tables should be repaired.
type TablePreference interface {
	// KSLess compares priorities of two keyspaces according to preference.
	KSLess(ks1, ks2 string) bool
	// TLess compares priorities of two tables from the same keyspace according to preference.
	TLess(ks, t1, t2 string) bool
}

// internalTablePreference orders internal tables before user tables.
// It additionally orders internal tables by their importance.
type internalTablePreference []Unit

func NewInternalTablePreference() TablePreference {
	return internalTablePreference{
		{
			Keyspace: "system_auth",
			Tables: []string{
				"role_attributes",
				"role_members",
			},
		},
		{Keyspace: "system_distributed"},
		{Keyspace: "system_distributed_everywhere"},
		{Keyspace: "system_traces"},
	}
}

func (it internalTablePreference) KSLess(ks1, ks2 string) bool {
	if ks1 == ks2 {
		return false
	}
	for _, u := range it {
		if u.Keyspace == ks1 {
			return true
		}
		if u.Keyspace == ks2 {
			return false
		}
	}
	return false
}

func (it internalTablePreference) TLess(ks, t1, t2 string) bool {
	if t1 == t2 {
		return false
	}
	for _, u := range it {
		if u.Keyspace != ks {
			continue
		}
		for _, t := range u.Tables {
			if t == t1 {
				return true
			}
			if t == t2 {
				return false
			}
		}
	}
	return false
}
