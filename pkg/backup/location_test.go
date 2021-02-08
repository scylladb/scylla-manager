// Copyright (C) 2017 ScyllaDB

package backup

import (
	"testing"
)

func TestSplit(t *testing.T) {
	table := []struct {
		Name     string
		Location string
		DC       string
		Provider string
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
			Provider: "s3",
			Bucket:   "bucket",
		},
		{
			Name:     "Valid without prefix",
			Location: "s3:bucket",
			DC:       "",
			Provider: "s3",
			Bucket:   "bucket",
		},
	}

	for i := 0; i < len(table); i++ {
		t.Run(table[i].Name, func(t *testing.T) {
			dc, provider, bucket, err := Split(table[i].Location)
			if err != nil {
				if table[i].Err == "" {
					t.Fatalf("Split unexpected error: %+v", err)
				}
				if err.Error() != table[i].Err {
					t.Fatalf("Split expected %s, got: %+v", table[i].Err, err)
				}
			} else if table[i].Err != "" {
				t.Fatal("Split expected error got nil")
			}
			if dc != table[i].DC {
				t.Errorf("DC = %s, expected %s", dc, table[i].DC)
			}
			if provider != table[i].Provider {
				t.Errorf("Provider = %s, expected %s", provider, table[i].Provider)
			}
			if bucket != table[i].Bucket {
				t.Errorf("Bucket = %s, expected %s", bucket, table[i].Bucket)
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
