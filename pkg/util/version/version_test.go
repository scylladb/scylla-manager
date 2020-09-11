// Copyright (C) 2017 ScyllaDB

package version

import "testing"

func TestShortVersion(t *testing.T) {
	ts := []struct {
		Version  string
		Expected string
	}{
		{
			Version:  "1.2.3-20200101.b41b3dbs1b",
			Expected: "1.2.3",
		},
		{
			Version:  "4.0.3-0.20200621.1fcf38abd9b",
			Expected: "4.0.3",
		},
		{
			Version:  "4.0.2",
			Expected: "4.0.2",
		},
		{
			Version:  "3.3.3-202006141737",
			Expected: "3.3.3",
		},
		{
			Version:  "4.1.rc2",
			Expected: "4.1.rc2",
		},
		{
			Version:  "i'm - not a version",
			Expected: "i'm - not a version",
		},
	}

	for i := range ts {
		test := ts[i]
		t.Run(test.Version, func(t *testing.T) {
			t.Parallel()
			short := Short(test.Version)
			if short != test.Expected {
				t.Errorf("Expected %s for %s version, got %s", test.Expected, test.Version, short)
			}
		})
	}
}

func TestTransformReleaseCandidate(t *testing.T) {
	ts := []struct {
		Version  string
		Expected string
	}{
		{
			Version:  "4.1.rc2",
			Expected: "4.1~rc2",
		},
		{
			Version:  "4.1.rc0",
			Expected: "4.1~rc0",
		},
		{
			Version:  "4.0.3-0.20200621.1fcf38abd9b",
			Expected: "4.0.3-0.20200621.1fcf38abd9b",
		},
		{
			Version:  "i'm - not a version",
			Expected: "i'm - not a version",
		},
	}

	for i := range ts {
		test := ts[i]
		t.Run(test.Version, func(t *testing.T) {
			t.Parallel()
			rc := TransformReleaseCandidate(test.Version)
			if rc != test.Expected {
				t.Errorf("Expected %s for %s version, got %s", test.Expected, test.Version, rc)
			}
		})
	}
}

func TestCheckConstraint(t *testing.T) {
	ts := []struct {
		Version    string
		Constraint string
		Match      bool
	}{
		{
			Version:    "4.0.3-0.20200621.1fcf38abd9b",
			Constraint: ">= 4.0",
			Match:      true,
		},
		{
			Version:    "4.1.rc2",
			Constraint: ">= 4.1, < 2000",
			Match:      true,
		},
		{
			Version:    "2020.1",
			Constraint: ">= 4.1, < 2000",
			Match:      false,
		},
		{
			Version:    "4.1",
			Constraint: ">= 4.1, < 2000",
			Match:      true,
		},
		{
			Version:    "3.9",
			Constraint: ">= 4.1, < 2000",
			Match:      false,
		},
	}

	for i := range ts {
		test := ts[i]
		t.Run(test.Version, func(t *testing.T) {
			t.Parallel()
			match, err := CheckConstraint(test.Version, test.Constraint)
			if err != nil {
				t.Error(err)
			}
			if test.Match && !match {
				t.Errorf("Expected match for %q version and %q constraint", test.Version, test.Constraint)
			} else if !test.Match && match {
				t.Errorf("Expected no match for %q version and %q constraint", test.Version, test.Constraint)
			}
		})
	}
}
