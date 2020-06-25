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

	task := &Task{
		ClusterID: "564a4ef1-0f37-40c5-802c-d08d788b8503",
		Type:      "repair",
		Schedule: &Schedule{
			Interval:   "7d",
			StartDate:  strfmt.DateTime(time.Date(2019, 3, 18, 23, 0, 0, 0, time.UTC)),
			NumRetries: 3,
		},
		Properties: map[string]interface{}{
			"keyspace":     []interface{}{"test_keyspace_dc1_rf3.*", "!test_keyspace_dc2*"},
			"dc":           []interface{}{"dc2"},
			"host":         "192.168.100.11",
			"with_hosts":   []interface{}{"192.168.100.11", "192.168.100.21"},
			"token_ranges": "all",
			"fail_fast":    true,
			"intensity":    1,
		},
	}

	table := []struct {
		Name     string
		Renderer *CmdRenderer
		Err      string
	}{
		{
			"render all",
			NewCmdRenderer(task, RenderAll),
			"sctool repair --cluster 564a4ef1-0f37-40c5-802c-d08d788b8503 --start-date 2019-03-18T23:00:00.000Z --num-retries 3 --interval 7d -K 'test_keyspace_dc1_rf3.*,!test_keyspace_dc2*' --dc 'dc2' --host 192.168.100.11 --with-hosts 192.168.100.11,192.168.100.21 --fail-fast --token-ranges all --intensity 1",
		},
		{
			"render args",
			NewCmdRenderer(task, RenderArgs),
			"--cluster 564a4ef1-0f37-40c5-802c-d08d788b8503 --start-date 2019-03-18T23:00:00.000Z --num-retries 3 --interval 7d -K 'test_keyspace_dc1_rf3.*,!test_keyspace_dc2*' --dc 'dc2' --host 192.168.100.11 --with-hosts 192.168.100.11,192.168.100.21 --fail-fast --token-ranges all --intensity 1",
		},
		{
			"render task type args",
			NewCmdRenderer(task, RenderTypeArgs),
			"-K 'test_keyspace_dc1_rf3.*,!test_keyspace_dc2*' --dc 'dc2' --host 192.168.100.11 --with-hosts 192.168.100.11,192.168.100.21 --fail-fast --token-ranges all --intensity 1",
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
			if diff := cmp.Diff(buf.String(), test.Err); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
