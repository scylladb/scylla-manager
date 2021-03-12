// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/scylla-manager/pkg/config"
)

func TestApiURL(t *testing.T) {
	table := []struct {
		Name   string
		Config config.ServerConfig
		Golden string
	}{
		{
			Name:   "empty configuration",
			Config: config.ServerConfig{},
			Golden: "",
		},
		{
			Name:   "HTTP",
			Config: config.ServerConfig{HTTP: "127.0.0.1:12345"},
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "HTTPS",
			Config: config.ServerConfig{HTTPS: "127.0.0.1:54321"},
			Golden: "https://127.0.0.1:54321/api/v1",
		},
		{
			Name:   "HTTP override",
			Config: config.ServerConfig{HTTP: "127.0.0.1:12345", HTTPS: "127.0.0.2:54321"},
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "HTTP override on all interfaces",
			Config: config.ServerConfig{HTTP: "0.0.0.0:12345", HTTPS: "127.0.0.1:54321"},
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "HTTP empty host",
			Config: config.ServerConfig{HTTP: ":12345"},
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "IPV6",
			Config: config.ServerConfig{HTTP: "[::1]:12345"},
			Golden: "http://[::1]:12345/api/v1",
		},
		{
			Name:   "IPV6 on all interfaces",
			Config: config.ServerConfig{HTTPS: "[::0]:54321"},
			Golden: "https://[::1]:54321/api/v1",
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			res := urlFromConfig(test.Config)
			if diff := cmp.Diff(test.Golden, res); diff != "" {
				t.Error(diff)
			}
		})
	}
}
