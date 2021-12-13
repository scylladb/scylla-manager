// Copyright (C) 2017 ScyllaDB

package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestBaseURL(t *testing.T) {
	table := []struct {
		Name   string
		HTTP   string
		HTTPS  string
		Golden string
	}{
		{
			Name:   "empty configuration",
			Golden: "",
		},
		{
			Name:   "HTTP",
			HTTP:   "127.0.0.1:12345",
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "HTTPS",
			HTTPS:  "127.0.0.1:54321",
			Golden: "https://127.0.0.1:54321/api/v1",
		},
		{
			Name:   "HTTP override",
			HTTP:   "127.0.0.1:12345",
			HTTPS:  "127.0.0.2:54321",
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "HTTP override on all interfaces",
			HTTP:   "0.0.0.0:12345",
			HTTPS:  "127.0.0.1:54321",
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "HTTP empty host",
			HTTP:   ":12345",
			Golden: "http://127.0.0.1:12345/api/v1",
		},
		{
			Name:   "IPV6",
			HTTP:   "[::1]:12345",
			Golden: "http://[::1]:12345/api/v1",
		},
		{
			Name:   "IPV6 on all interfaces",
			HTTPS:  "[::0]:54321",
			Golden: "https://[::1]:54321/api/v1",
		},
	}

	for i := range table {
		test := table[i]
		t.Run(test.Name, func(t *testing.T) {
			if diff := cmp.Diff(test.Golden, baseURL(test.HTTP, test.HTTPS)); diff != "" {
				t.Error(diff)
			}
		})
	}
}
