// Copyright (C) 2017 ScyllaDB

package backup

import "testing"

func TestProviderMarshalUnmarshalText(t *testing.T) {
	for _, k := range []Provider{S3} {
		b, err := k.MarshalText()
		if err != nil {
			t.Error(k, err)
		}
		var p Provider
		if err := p.UnmarshalText(b); err != nil {
			t.Error(err)
		}
		if k != p {
			t.Errorf("got %s, expected %s", p, k)
		}
	}
}

func TestLocationMarshalUnmarshalText(t *testing.T) {
	table := []struct {
		Name     string
		Location Location
	}{
		{
			Name: "with dc",
			Location: Location{
				DC:       "dc",
				Provider: S3,
				Path:     "my-bucket.domain",
			},
		},
		{
			Name: "without dc",
			Location: Location{
				Provider: S3,
				Path:     "my-bucket.domain",
			},
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			golden := test.Location
			b, err := golden.MarshalText()
			if err != nil {
				t.Error(golden, err)
			}
			var l Location
			if err := l.UnmarshalText(b); err != nil {
				t.Error(err)
			}
			if golden != l {
				t.Errorf("got %s, expected %s", l, golden)
			}
		})
	}
}

func TestInvalidLocationUnmarshalText(t *testing.T) {
	table := []struct {
		Name     string
		Location string
	}{
		{
			Name:     "empty",
			Location: "",
		},
		{
			Name:     "empty path",
			Location: "s3:",
		},
		{
			Name:     "empty path with dc",
			Location: "dc:s3:",
		},
		{
			Name:     "invalid dc",
			Location: "dc aaa:foo:bar",
		},
		{
			Name:     "invalid provider",
			Location: "foo:bar",
		},
		{
			Name:     "invalid path",
			Location: "s3:name boo",
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			l := Location{}
			if err := l.UnmarshalText([]byte(test.Location)); err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestLocationRemotePath(t *testing.T) {
	l := Location{
		Provider: S3,
		Path:     "foo",
	}

	table := []struct {
		Path       string
		RemotePath string
	}{
		{
			Path:       "bar",
			RemotePath: "s3:foo/bar",
		},
		{
			Path:       "/bar",
			RemotePath: "s3:foo/bar",
		},
	}

	for _, test := range table {
		if p := l.RemotePath(test.Path); p != test.RemotePath {
			t.Error("expected", test.RemotePath, "got", p)
		}
	}
}

func TestDCLimitMarshalUnmarshalText(t *testing.T) {
	table := []struct {
		Name    string
		DCLimit DCLimit
	}{
		{
			Name: "with dc",
			DCLimit: DCLimit{
				DC:    "dc",
				Limit: 100,
			},
		},
		{
			Name: "without dc",
			DCLimit: DCLimit{
				Limit: 100,
			},
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			golden := test.DCLimit
			b, err := golden.MarshalText()
			if err != nil {
				t.Error(golden, err)
			}
			var r DCLimit
			if err := r.UnmarshalText(b); err != nil {
				t.Error(err)
			}
			if golden != r {
				t.Errorf("got %s, expected %s", r, golden)
			}
		})
	}
}
