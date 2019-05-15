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
	k := Location{
		Provider: S3,
		Path:     "my-bucket.domain",
	}
	b, err := k.MarshalText()
	if err != nil {
		t.Error(k, err)
	}
	var l Location
	if err := l.UnmarshalText(b); err != nil {
		t.Error(err)
	}
	if k != l {
		t.Errorf("got %s, expected %s", l, k)
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
