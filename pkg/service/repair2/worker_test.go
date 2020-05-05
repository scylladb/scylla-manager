// Copyright (C) 2017 ScyllaDB

package repair

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/scylladb/mermaid/pkg/scyllaclient"
)

func TestWorkerCount(t *testing.T) {
	table := []struct {
		Name      string
		InputFile string
		Count     int
	}{
		{
			Name:      "RF=2",
			InputFile: "testdata/worker_count/simple_rf2.json",
			Count:     3,
		},
		{
			Name:      "RF=3",
			InputFile: "testdata/worker_count/simple_rf3.json",
			Count:     2,
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			var content []struct {
				Endpoints []string `json:"endpoints"`
			}
			data, err := ioutil.ReadFile(test.InputFile)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &content); err != nil {
				t.Fatal(err)
			}
			var ranges []scyllaclient.TokenRange
			for i := range content {
				ranges = append(ranges, scyllaclient.TokenRange{Replicas: content[i].Endpoints})
			}

			v := workerCount(ranges)
			if v != test.Count {
				t.Errorf("workerCount()=%d expeced %d", v, test.Count)
			}
		})
	}
}
