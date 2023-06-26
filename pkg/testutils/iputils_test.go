// Copyright (C) 2022 ScyllaDB

package testutils

import (
	"testing"
)

func TestIsPublicIP(t *testing.T) {
	testCases := []struct {
		host     string
		pubHost  string
		expected bool
	}{
		{
			host:     "192.168.1.200",
			pubHost:  "192.168.1.1",
			expected: true,
		},
		{
			host:     "100:200::200",
			pubHost:  "192.168.1.1",
			expected: false,
		},
		{
			host:     "100:200::200",
			pubHost:  "100:200:0:0:0:0:0:199",
			expected: true,
		},
		{
			host:     "100:200:0:0:0:0:0:200",
			pubHost:  "100:200::1",
			expected: true,
		},
	}
	for _, tcase := range testCases {
		t.Run(tcase.host, func(t *testing.T) {
			value := isPublicIP(tcase.host, tcase.pubHost)
			if value != tcase.expected {
				t.Errorf("%t != %t", tcase.expected, value)
			}
		})
	}
}

func TestURLEncodeIP(t *testing.T) {
	testCases := []struct {
		host     string
		expected string
	}{
		{
			host:     "192.168.1.1",
			expected: "192.168.1.1",
		},
		{
			host:     "100:200::1",
			expected: "[100:200::1]",
		},
		{
			host:     "100:200::",
			expected: "[100:200::]",
		},
	}
	for _, tcase := range testCases {
		t.Run(tcase.host, func(t *testing.T) {
			value := URLEncodeIP(tcase.host)
			if value != tcase.expected {
				t.Errorf("%s != %s", tcase.expected, value)
			}
		})
	}
}

func TestToCanonicalIP(t *testing.T) {
	testCases := []struct {
		host     string
		expected string
	}{
		{
			host:     "192.168.1.1",
			expected: "192.168.1.1",
		},
		{
			host:     "192.168.1.",
			expected: "192.168.1.",
		},
		{
			host:     "192.168.0.0",
			expected: "192.168.0.0",
		},
		{
			host:     "100:200:0:0:0:0:0:1",
			expected: "100:200::1",
		},
		{
			host:     "100:200:0:0:0:0:",
			expected: "100:200:0:0:0:0:",
		},
		{
			host:     "100:200:0::1",
			expected: "100:200::1",
		},
		{
			host:     "100:200::",
			expected: "100:200::",
		},
	}
	for _, tcase := range testCases {
		t.Run(tcase.host, func(t *testing.T) {
			value := ToCanonicalIP(tcase.host)
			if value != tcase.expected {
				t.Errorf("%s != %s", tcase.expected, value)
			}
		})
	}
}

func TestExpandIP(t *testing.T) {
	testCases := []struct {
		host     string
		expected string
	}{
		{
			host:     "192.168.1.1",
			expected: "192.168.1.1",
		},
		{
			host:     "192.168.1.",
			expected: "192.168.1.",
		},
		{
			host:     "192.168.0.0",
			expected: "192.168.0.0",
		},
		{
			host:     "100:200::1",
			expected: "100:200:0:0:0:0:0:1",
		},
		{
			host:     "100:200::",
			expected: "100:200:0:0:0:0:0:",
		},
		{
			host:     "100:200:0:0:0:0:0:1",
			expected: "100:200:0:0:0:0:0:1",
		},
		{
			host:     "100:200:0:0:0:0:0:",
			expected: "100:200:0:0:0:0:0:",
		},
	}
	for _, tcase := range testCases {
		t.Run(tcase.host, func(t *testing.T) {
			value := ExpandIP(tcase.host)
			if value != tcase.expected {
				t.Errorf("%s != %s", tcase.expected, value)
			}
		})
	}
}
