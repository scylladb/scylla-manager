// Copyright (C) 2017 ScyllaDB

package backupspec

import (
	"testing"
)

func TestNewLocation(t *testing.T) {
	table := []struct {
		Name     string
		Location string
		DC       string
		Provider Provider
		Bucket   string
		Err      string
	}{
		{
			Name: "Empty",
			Err:  ErrInvalid.Error(),
		},
		{
			Name:     "Invalid",
			Location: "34nml3434kk34$%#5",
			Err:      ErrInvalid.Error(),
		},
		{
			Name:     "Valid with prefix",
			Location: "dc1:s3:bucket",
			DC:       "dc1",
			Provider: S3,
			Bucket:   "bucket",
		},
		{
			Name:     "Valid without prefix",
			Location: "s3:bucket",
			DC:       "",
			Provider: S3,
			Bucket:   "bucket",
		},
	}

	for i := 0; i < len(table); i++ {
		t.Run(table[i].Name, func(t *testing.T) {
			l, err := NewLocation(table[i].Location)
			if err != nil {
				if table[i].Err == "" {
					t.Fatalf("NewLocation unexpected error: %+v", err)
				}
				if err.Error() != table[i].Err {
					t.Fatalf("NewLocation expected %s, got: %+v", table[i].Err, err)
				}
			} else if table[i].Err != "" {
				t.Fatal("NewLocation expected error got nil")
			}
			if l.DC != table[i].DC {
				t.Errorf("DC = %s, expected %s", l.DC, table[i].DC)
			}
			if l.Provider != table[i].Provider {
				t.Errorf("Provider = %s, expected %s", l.Provider, table[i].Provider)
			}
			if l.Path != table[i].Bucket {
				t.Errorf("Bucket = %s, expected %s", l.Path, table[i].Bucket)
			}
		})
	}
}

func TestStripDC(t *testing.T) {
	res, err := StripDC("dc1:s3:bucket")
	if err != nil {
		t.Fatal(err)
	}
	if res != "s3:bucket" {
		t.Errorf("StripDC = %s, expected s3:bucket", res)
	}
}

func TestProviderMarshalUnmarshalText(t *testing.T) {
	t.Parallel()

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
			t.Errorf("Got %s, expected %s", p, k)
		}
	}
}

func TestLocationMarshalUnmarshalText(t *testing.T) {
	t.Parallel()

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

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

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
				t.Errorf("Got %s, expected %s", l, golden)
			}
		})
	}
}

func TestInvalidLocationUnmarshalText(t *testing.T) {
	t.Parallel()

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
			Name:     "invalid path",
			Location: "s3:name boo",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			l := Location{}
			if err := l.UnmarshalText([]byte(test.Location)); err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestLocationRemotePath(t *testing.T) {
	t.Parallel()

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
