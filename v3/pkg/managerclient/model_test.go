package managerclient

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/v3/pkg/util/inexlist"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
)

func TestOne2OneRestoreProgressRender(t *testing.T) {
	testCases := []struct {
		name                   string
		one2oneRestoreProgress string
	}{
		{
			name:                   "Only tables",
			one2oneRestoreProgress: "testdata/1.1_1_restore_run_progress.json",
		},
		{
			name:                   "Only views",
			one2oneRestoreProgress: "testdata/2.1_1_restore_run_progress.json",
		},
		{
			name:                   "Empty",
			one2oneRestoreProgress: "testdata/3.1_1_restore_run_progress.json",
		},
		{
			name:                   "Table and views",
			one2oneRestoreProgress: "testdata/4.1_1_restore_run_progress.json",
		},
		{
			name:                   "Table and views, in progress",
			one2oneRestoreProgress: "testdata/5.1_1_restore_run_progress.json",
		},
		{
			name:                   "Table and views, not detailed",
			one2oneRestoreProgress: "testdata/6.1_1_restore_run_progress.json",
		},
		{
			name:                   "Table and views, not detailed, in progress",
			one2oneRestoreProgress: "testdata/7.1_1_restore_run_progress.json",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			progress := loadOne2OneRestoreProgress(t, tc.one2oneRestoreProgress)
			var b bytes.Buffer
			if err := progress.Render(&b); err != nil {
				t.Fatalf("Unexpected err: %v", err)
			}
			saveGoldenFile(t, tc.one2oneRestoreProgress, b.Bytes(), false)
			expected := loadGoldenFile(t, tc.one2oneRestoreProgress)
			if diff := cmp.Diff(b.String(), expected); diff != "" {
				t.Fatalf("Actual != Expected\n%s", diff)
			}
		})
	}
}

func TestRestoreProgressRender(t *testing.T) {
	const dir = "testdata/restore_progress/"

	// Fixtures are built from SM server restore progress goldens
	// (pkg/service/restore/testdata/AggregateProgress) wrapped with a task run.
	testCases := []struct {
		name     string
		fixture  string
		detailed bool
		keyspace []string
	}{
		// rclone only
		{name: "rclone", fixture: "rclone"},
		{name: "rclone detailed", fixture: "rclone", detailed: true},
		// native only, partially restored
		{name: "native", fixture: "native"},
		{name: "native detailed", fixture: "native", detailed: true},
		// tablet aware only
		{name: "tablet", fixture: "tablet"},
		{name: "tablet detailed", fixture: "tablet", detailed: true},
		// tablet aware with failed table
		{name: "tablet_failed", fixture: "tablet_failed"},
		{name: "tablet_failed detailed", fixture: "tablet_failed", detailed: true},
		// tablet aware and rclone
		{name: "tablet_rclone", fixture: "tablet_rclone"},
		{name: "tablet_rclone detailed", fixture: "tablet_rclone", detailed: true},
		{name: "tablet_rclone detailed filtered", fixture: "tablet_rclone", detailed: true, keyspace: []string{"other"}},
		// native and rclone
		{name: "native_rclone", fixture: "native_rclone"},
		{name: "native_rclone detailed", fixture: "native_rclone", detailed: true},
		// older SM server not reporting methods
		{name: "no_method", fixture: "no_method"},
		{name: "no_method detailed", fixture: "no_method", detailed: true},
		// task run without progress
		{name: "no_progress", fixture: "no_progress"},
		{name: "no_progress detailed", fixture: "no_progress", detailed: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			progress := loadRestoreProgress(t, dir+tc.fixture+".json")
			progress.Detailed = tc.detailed
			if tc.keyspace != nil {
				filter, err := inexlist.ParseInExList(tc.keyspace)
				if err != nil {
					t.Fatalf("Parse keyspace filter: %v", err)
				}
				progress.KeyspaceFilter = filter
			}

			var b bytes.Buffer
			if err := progress.Render(&b); err != nil {
				t.Fatalf("Unexpected err: %v", err)
			}
			golden := dir + strings.ReplaceAll(tc.name, " ", ".")
			saveGoldenFile(t, golden, b.Bytes(), false)
			expected := loadGoldenFile(t, golden)
			if diff := cmp.Diff(b.String(), expected); diff != "" {
				t.Fatalf("Actual != Expected\n%s", diff)
			}
		})
	}
}

func loadRestoreProgress(t *testing.T, filePath string) RestoreProgress {
	t.Helper()
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Read file: %v", err)
	}
	var progress models.TaskRunRestoreProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		t.Fatalf("JSON unmarshal: %v", err)
	}
	return RestoreProgress{
		TaskRunRestoreProgress: &progress,
		Task: &models.Task{
			ClusterID: progress.Run.ClusterID,
			Type:      RestoreTask,
		},
	}
}

func loadOne2OneRestoreProgress(t *testing.T, filePath string) One2OneRestoreProgress {
	t.Helper()
	var progress One2OneRestoreProgress
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Read file: %v", err)
	}
	err = json.Unmarshal(data, &progress)
	if err != nil {
		t.Fatalf("JSON unmarshal: %v", err)
	}
	return progress
}

func saveGoldenFile(t *testing.T, prefix string, content []byte, update bool) {
	t.Helper()
	if !update {
		return
	}
	fileName := prefix + ".golden"
	if err := os.WriteFile(fileName, content, 0o666); err != nil {
		t.Fatalf("can't save golden file: %v", err)
	}
}

func loadGoldenFile(t *testing.T, prefix string) string {
	t.Helper()
	fileName := prefix + ".golden"
	b, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatalf("can't read golden file: %v", err)
	}
	return string(b)
}
