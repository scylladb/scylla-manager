// Copyright (C) 2017 ScyllaDB

package backup

import "math/rand"

const systemSchema = "system_schema"

func unitsPerm(units []Unit) []int {
	perm := rand.Perm(len(units))

	// Move system_schema to the end if exists in order not to lose data.
	// Schema should be snapshotted after user tables are snapshotted.
	last := len(perm) - 1
	for i := range perm {
		if i < last && units[perm[i]].Keyspace == systemSchema {
			perm[i], perm[last] = perm[last], perm[i]
			break
		}
	}
	return perm
}
