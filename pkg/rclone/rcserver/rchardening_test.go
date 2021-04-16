// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fs/rc"
)

func TestPathHasPrefix(t *testing.T) {
	const prefix = "backup/meta/"

	table := []struct {
		Fs     string
		Remote string
		Error  error
	}{
		{
			Fs:     "s3:bla",
			Remote: "backup/meta/file",
		},
		{
			Fs:     "s3:bla/backup/meta",
			Remote: "file",
		},
		{
			Fs:     "s3:bla",
			Remote: "backup/sst/file",
			Error:  fs.ErrorPermissionDenied,
		},
		{
			Fs:     "s3:bla/backup/sst",
			Remote: "file",
			Error:  fs.ErrorPermissionDenied,
		},
		{
			Fs:     "s3:bla",
			Remote: "backup/meta/../sst/file",
			Error:  fs.ErrorPermissionDenied,
		},
	}

	ctx := context.Background()

	for _, test := range table {
		in := rc.Params{
			"fs":     test.Fs,
			"remote": test.Remote,
		}
		if err := pathHasPrefix(prefix)(ctx, in); err != test.Error {
			t.Fatalf("pathHasPrefix() = %s, expected %s", err, test.Error)
		}
	}
}
