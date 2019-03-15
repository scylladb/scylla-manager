package mermaidclient

import (
	"bytes"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/go-cmp/cmp"
)

func TestCmdlineRender(t *testing.T) {
	task := &Task{
		ClusterID: "564a4ef1-0f37-40c5-802c-d08d788b8503",
		Type:      "repair",
		Schedule: &Schedule{
			Interval:   "7d",
			StartDate:  strfmt.DateTime(time.Date(2019, 3, 18, 23, 0, 0, 0, time.UTC)),
			NumRetries: 3,
		},
		Properties: map[string]interface{}{
			"keyspace":     []interface{}{"test_keyspace_dc1_rf3.*"},
			"dc":           []interface{}{"dc2"},
			"host":         "192.168.100.11",
			"with_hosts":   []interface{}{"192.168.100.11", "192.168.100.21"},
			"token_ranges": "all",
			"fail_fast":    true,
		},
	}

	table := []struct {
		N string
		R *CmdRenderer
		E string
	}{
		{
			"render all",
			NewCmdRenderer(task, RenderAll),
			"sctool repair --cluster 564a4ef1-0f37-40c5-802c-d08d788b8503 --start-date 2019-03-18T23:00:00.000Z --num-retries 3 --interval 7d -K test_keyspace_dc1_rf3.\\* --dc dc2 --host 192.168.100.11 --with-hosts 192.168.100.11,192.168.100.21 --fail-fast --token-ranges all",
		},
		{
			"render args",
			NewCmdRenderer(task, RenderArgs),
			"--cluster 564a4ef1-0f37-40c5-802c-d08d788b8503 --start-date 2019-03-18T23:00:00.000Z --num-retries 3 --interval 7d -K test_keyspace_dc1_rf3.\\* --dc dc2 --host 192.168.100.11 --with-hosts 192.168.100.11,192.168.100.21 --fail-fast --token-ranges all",
		},
		{
			"render task type args",
			NewCmdRenderer(task, RenderTypeArgs),
			"-K test_keyspace_dc1_rf3.\\* --dc dc2 --host 192.168.100.11 --with-hosts 192.168.100.11,192.168.100.21 --fail-fast --token-ranges all",
		},
	}

	for _, test := range table {
		t.Run(test.N, func(t *testing.T) {
			buf := bytes.Buffer{}
			err := test.R.Render(&buf)
			if err != nil {
				t.Fatal(err.Error())
			}
			if diff := cmp.Diff(buf.String(), test.E); diff != "" {
				t.Fatal(diff)
			}
		})
	}

}
