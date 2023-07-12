// Copyright (C) 2017 ScyllaDB

package cfgutil

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"gopkg.in/yaml.v2"
)

func TestParseYAML(t *testing.T) {
	t.Parallel()

	type result struct {
		AuthToken  string `yaml:"auth_token"`
		Prometheus string
		CPU        int
		Logger     map[string]string
	}

	table := []struct {
		Name          string
		Input         []string
		Golden        result
		ErrorExpected bool
	}{
		{
			Name:  "basic",
			Input: []string{"./testdata/base.yaml", "./testdata/first.yaml"},
			Golden: result{
				AuthToken:  "token",
				Prometheus: ":5090",
				CPU:        -2,
				Logger: map[string]string{
					"mode": "stdout",
				},
			},
		},
		{
			Name:  "missing override file",
			Input: []string{"./testdata/base.yaml", "./testdata/missing.yaml"},
			Golden: result{
				AuthToken:  "",
				Prometheus: ":5090",
				CPU:        -1,
				Logger: map[string]string{
					"mode": "stderr",
				},
			},
		},
		{
			Name:  "one missing override file",
			Input: []string{"./testdata/base.yaml", "./testdata/missing.yaml", "./testdata/first.yaml"},
			Golden: result{
				AuthToken:  "token",
				Prometheus: ":5090",
				CPU:        -2,
				Logger: map[string]string{
					"mode": "stdout",
				},
			},
		},
		{
			Name:  "invalid type",
			Input: []string{"./testdata/base.yaml", "./testdata/invalid_type.yaml"},
			Golden: result{
				AuthToken:  "token",
				Prometheus: ":5090",
				CPU:        -2,
				Logger: map[string]string{
					"mode": "stdout",
				},
			},
			ErrorExpected: true,
		},
		{
			Name:  "missing nested field indent",
			Input: []string{"./testdata/base.yaml", "./testdata/missing_nested_field_indent.yaml"},
			Golden: result{
				AuthToken:  "token",
				Prometheus: ":5090",
				CPU:        -2,
				Logger: map[string]string{
					"mode": "stdout",
				},
			},
			ErrorExpected: true,
		},
	}
	for id := range table {
		test := table[id]
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			got := &result{}
			if err := ParseYAML(got, result{}, test.Input...); err != nil {
				if test.ErrorExpected {
					if _, ok := err.(*yaml.TypeError); ok {
						return
					}
				}
				t.Error(err)
			}
			if diff := cmp.Diff(*got, test.Golden); diff != "" {
				t.Errorf("%s", diff)
			}
		})
	}
}

func TestPermissiveParseYAML(t *testing.T) {
	got := &struct {
		AuthToken string `yaml:"auth_token"`
	}{}
	if err := PermissiveParseYAML(got, "./testdata/base.yaml", "./testdata/first.yaml"); err != nil {
		t.Fatalf("PermissiveParseYAML() error %s", err)
	}
}
