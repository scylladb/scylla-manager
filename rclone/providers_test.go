// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"testing"

	"github.com/rclone/rclone/fs"
)

func TestRegisterS3ProviderSetsS3Options(t *testing.T) {
	InitFsConfig()

	opts := S3Options{
		AccessKeyID:     "a",
		SecretAccessKey: "b",
		Region:          "c",
		Endpoint:        "d",
	}

	if err := RegisterS3Provider(opts); err != nil {
		t.Fatal("RegisterS3Provider() error", err)
	}

	table := []struct {
		Key   string
		Value string
	}{
		{
			Key:   "access_key_id",
			Value: "a",
		},
		{
			Key:   "secret_access_key",
			Value: "b",
		},
		{
			Key:   "region",
			Value: "c",
		},
		{
			Key:   "endpoint",
			Value: "d",
		},
	}

	for _, test := range table {
		if v, _ := fs.ConfigFileGet("s3", test.Key); v != test.Value {
			t.Errorf("ConfigFileGet(%s) = %s, expected %s", test.Key, v, test.Value)
		}
	}
}
