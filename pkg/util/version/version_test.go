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
