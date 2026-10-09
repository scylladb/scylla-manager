// Copyright (C) 2026 ScyllaDB

package repair

import (
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

func TestTaskPropertiesMetric(t *testing.T) {
	t.Run("unset properties report the service defaults", func(t *testing.T) {
		got := taskPropertiesMetric(defaultTaskProperties())
		want := metrics.RepairTaskProperties{
			Keyspace:            "*,!system_traces",
			DC:                  "all",
			KeyspaceReplication: string(scyllaclient.ReplicationAll),
			// Scylla Manager does not send the parameter, so it stays unset
			// and the metric reports it as such.
			IncrementalMode: "",
			Host:            "all",
			FailFast:        "false",
		}
		if got != want {
			t.Errorf("got  %+v\nwant %+v", got, want)
		}
	})

	t.Run("configured properties win", func(t *testing.T) {
		p := defaultTaskProperties()
		p.Keyspace = []string{"ks"}
		p.DC = []string{"dc1", "dc2"}
		p.Host = "192.168.200.11"
		p.FailFast = true
		p.IncrementalMode = scyllaclient.IncrementalModeFull

		got := taskPropertiesMetric(p)
		if got.Keyspace != "ks" || got.DC != "dc1,dc2" || got.Host != "192.168.200.11" ||
			got.FailFast != "true" || got.IncrementalMode != string(scyllaclient.IncrementalModeFull) {
			t.Errorf("got %+v", got)
		}
	})
}
