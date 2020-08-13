// Copyright (C) 2017 ScyllaDB

package mermaidclient

import (
	"bytes"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/go-cmp/cmp"
)

func TestCmdlineRender(t *testing.T) {
	t.Parallel()

	repairTask := &Task{
		ClusterID: "564a4ef1-0f37-40c5-802c-d08d788b8503",
		Type:      "repair",
		Schedule: &Schedule{
			Interval:   "7d",
			StartDate:  strfmt.DateTime(time.Date(2019, 3, 18, 23, 0, 0, 0, time.UTC)),
			NumRetries: 3,
		},
		Properties: map[string]interface{}{
			"keyspace":              []interface{}{"test_keyspace_dc1_rf3.*", "!test_keyspace_dc2*"},
			"dc":                    []interface{}{"dc2"},
			"fail_fast":             true,
			"intensity":             1,
			"small_table_threshold": 1073741824,
		},
	}

	backupTask := &Task{
		ClusterID: "564a4ef1-0f37-40c5-802c-d08d788b8503",
		Type:      "backup",
		Schedule: &Schedule{
			Interval:   "7d",
			StartDate:  strfmt.DateTime(time.Date(2019, 3, 18, 23, 0, 0, 0, time.UTC)),
			NumRetries: 3,
		},
		Properties: map[string]interface{}{
			"keyspace":          []interface{}{"test_keyspace_dc1_rf3.*", "!test_keyspace_dc2*"},
			"dc":                []interface{}{"dc1", "dc2"},
			"snapshot_parallel": []interface{}{"dc1:2", "dc2:3"},
			"upload_parallel":   []interface{}{"dc1:4", "dc2:1"},
			"rate_limit":        2,
			"retention":         3,
		},
	}

	table := []struct {
		Name     string
		Renderer *CmdRenderer
		Golden   string
	}{
		{
			"render all",
			NewCmdRenderer(repairTask, RenderAll),
			"sctool repair --cluster 564a4ef1-0f37-40c5-802c-d08d788b8503 --start-date 2019-03-18T23:00:00.000Z --num-retries 3 --interval 7d -K 'test_keyspace_dc1_rf3.*,!test_keyspace_dc2*' --dc 'dc2' --fail-fast --intensity 1 --small-table-threshold 1.00GiB",
		},
		{
			"render args",
			NewCmdRenderer(repairTask, RenderArgs),
			"--cluster 564a4ef1-0f37-40c5-802c-d08d788b8503 --start-date 2019-03-18T23:00:00.000Z --num-retries 3 --interval 7d -K 'test_keyspace_dc1_rf3.*,!test_keyspace_dc2*' --dc 'dc2' --fail-fast --intensity 1 --small-table-threshold 1.00GiB",
		},
		{
			"render repair task type args",
			NewCmdRenderer(repairTask, RenderTypeArgs),
			"-K 'test_keyspace_dc1_rf3.*,!test_keyspace_dc2*' --dc 'dc2' --fail-fast --intensity 1 --small-table-threshold 1.00GiB",
		},
		{
			"render backup task type args",
			NewCmdRenderer(backupTask, RenderTypeArgs),
			"-K 'test_keyspace_dc1_rf3.*,!test_keyspace_dc2*' --dc 'dc1,dc2' --retention 3 --rate-limit 2 --snapshot-parallel 'dc1:2,dc2:3' --upload-parallel 'dc1:4,dc2:1'",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			buf := bytes.Buffer{}
			err := test.Renderer.Render(&buf)
			if err != nil {
				t.Fatal(err.Error())
			}
			if diff := cmp.Diff(test.Golden, buf.String()); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
