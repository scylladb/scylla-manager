// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseConfigFiles(t *testing.T) {
	expected := config{
		HTTP: "127.0.0.1:1000",
		Scylla: scyllaConfig{
			APIPort:        "1000",
			PrometheusPort: "1000",
		},
	}

	t.Run("with scylla", func(t *testing.T) {
		c, err := parseConfigFiles("testdata/scylla-manager-agent-with-scylla.yaml", "")
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(expected, c); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("without scylla", func(t *testing.T) {
		c, err := parseConfigFiles("testdata/scylla-manager-agent-without-scylla.yaml", "testdata/scylla.yaml")
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(expected, c); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestDefaultConfig(t *testing.T) {
	c, err := parseConfigFiles("../../dist/etc/scylla-manager-agent.yaml", "")
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(defaultConfig(), c); diff != "" {
		t.Fatal(diff)
	}
}
