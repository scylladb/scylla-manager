package managerclient

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
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
