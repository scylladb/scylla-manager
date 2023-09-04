// Copyright (C) 2017 ScyllaDB

package managerclient_test

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
)

func TestBackupStageName(t *testing.T) {
	t.Parallel()

	for _, s := range backup.StageOrder() {
		if s != backup.StageDone && managerclient.BackupStageName(string(s)) == "" {
			t.Errorf("%s.Name() is empty", s)
		}
	}
}
