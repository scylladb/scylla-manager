// Copyright (C) 2017 ScyllaDB

package backupspec

import "testing"

func TestStageName(t *testing.T) {
	t.Parallel()

	for _, s := range StageOrder() {
		if s != StageDone && s.Name() == "" {
			t.Errorf("%s.Name() is empty", s)
		}
	}
}
