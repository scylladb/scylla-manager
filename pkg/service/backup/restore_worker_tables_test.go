// Copyright (C) 2023 ScyllaDB

package backup_test

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
)

func TestTotalRestoreProgress(t *testing.T) {
	type inputs struct {
		restoredBytes       int64
		totalBytesToRestore int64
	}
	tests := []struct {
		name   string
		fields inputs
		want   float64
	}{
		{
			name: "100% progress",
			fields: inputs{
				restoredBytes:       100,
				totalBytesToRestore: 100,
			},
			want: 100,
		},
		{
			name: "50% progress",
			fields: inputs{
				restoredBytes:       50,
				totalBytesToRestore: 100,
			},
			want: 50,
		},
		{
			name: "no bytes were restored yet",
			fields: inputs{
				restoredBytes:       0,
				totalBytesToRestore: 100,
			},
			want: 0,
		},
		{
			name: "empty restore, no bytes to restore",
			fields: inputs{
				restoredBytes:       0,
				totalBytesToRestore: 0,
			},
			want: 100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			progress := backup.NewTotalRestoreProgress(tt.fields.totalBytesToRestore)
			progress.Update(tt.fields.restoredBytes)
			if got := progress.CurrentProgress(); got != tt.want {
				t.Errorf("CurrentProgress() = %v, want %v", got, tt.want)
			}
		})
	}
}
