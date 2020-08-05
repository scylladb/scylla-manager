// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/mermaid/pkg/cmd/scylla-manager/config"
)

func TestApiURL(t *testing.T) {
	table := []struct {
		Name   string
		Config *config.ServerConfig
		Golden string
	}{
		{
			Name:   "empty configuration",
			Config: &config.ServerConfig{},
			Golden: "",
		},
		{
			Name:   "just HTTP",
			Config: &config.ServerConfig{HTTP: "127.0.0.1:12345"},
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "just HTTPS",
			Config: &config.ServerConfig{HTTPS: "127.0.0.1:12345"},
			Golden: "https://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "HTTPS override",
			Config: &config.ServerConfig{HTTP: "127.0.0.1:12345", HTTPS: "127.0.0.2:12345"},
			Golden: "https://127.0.0.2:12345/api/v1",
		},
		{
			Name:   "HTTPS override on all interfaces",
			Config: &config.ServerConfig{HTTP: "127.0.0.1:12345", HTTPS: "0.0.0.0:12346"},
			Golden: "https://127.0.0.1:12346/api/v1",
		},
		{
			Name:   "HTTP on all interfaces",
			Config: &config.ServerConfig{HTTP: ":12345"},
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "Support for IPV6",
			Config: &config.ServerConfig{HTTP: "[::1]:12345"},
			Golden: "http://[::1]:12345/api/v1",
		},
		{
			Name:   "Support for IPV6 on all interfaces",
			Config: &config.ServerConfig{HTTPS: "[::0]:12345"},
			Golden: "https://[::1]:12345/api/v1",
		},
	}

	for _, tt := range table {
		t.Run(tt.Name, func(t *testing.T) {
			res := configURL(tt.Config)
			if diff := cmp.Diff(tt.Golden, res); diff != "" {
				t.Error(diff)
			}
		})
	}
}
