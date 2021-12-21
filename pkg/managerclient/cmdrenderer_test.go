// Copyright (C) 2017 ScyllaDB

package managerclient

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/pkg/testutils"
)

func TestCmdlineRender(t *testing.T) {
	t.Parallel()

	repairTask := &Task{
		ClusterID: "564a4ef1-0f37-40c5-802c-d08d788b8503",
		Type:      "repair",
		Name:      "repair",
		Schedule: &Schedule{
			Cron:       "1 2 3 4 5",
			NumRetries: 3,
		},
		Properties: map[string]interface{}{
			"keyspace":              []interface{}{"test_keyspace_dc1_rf3.*", "!test_keyspace_dc2*"},
			"dc":                    []interface{}{"dc2"},
			"host":                  "192.168.100.11",
			"fail_fast":             true,
			"intensity":             1,
			"parallel":              2,
			"small_table_threshold": 1073741824,
		},
	}

	backupTask := &Task{
		ClusterID: "564a4ef1-0f37-40c5-802c-d08d788b8503",
		Type:      "backup",
		Name:      "backup",
		Schedule: &Schedule{
			Cron:       "1 2 3 4 5",
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

	backupTaskNilProperties := &Task{
		ClusterID: "564a4ef1-0f37-40c5-802c-d08d788b8503",
		Type:      "backup",
		Name:      "backup nil properties",
		Schedule: &Schedule{
			Cron:       "1 2 3 4 5",
			NumRetries: 3,
		},
		Properties: map[string]interface{}{
			"keyspace":          nil,
			"dc":                nil,
			"snapshot_parallel": nil,
			"upload_parallel":   nil,
			"rate_limit":        nil,
			"retention":         nil,
		},
	}

	for _, task := range []*Task{repairTask, backupTask, backupTaskNilProperties} {
		for _, r := range []CmdRenderType{RenderAll, RenderArgs, RenderTypeArgs} {
			t.Run(task.Name+" "+fmt.Sprint(r), func(t *testing.T) {
				var buf bytes.Buffer
				if err := NewCmdRenderer(task, r).Render(&buf); err != nil {
					t.Fatal(err)
				}
				text := buf.String()
				testutils.SaveGoldenTextFileIfNeeded(t, text)
				golden := testutils.LoadGoldenTextFile(t)

				if diff := cmp.Diff(golden, text); diff != "" {
					t.Fatal(diff)
				}
			})
		}
	}
}
