// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
	"github.com/scylladb/scylla-manager/v3/pkg/rclone"
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

func TestLocalToRemote(t *testing.T) {
	rclone.InitFsConfig()
	rclone.MustRegisterLocalDirProvider("tmp", "", "/tmp")
	if err := rclone.RegisterS3Provider(rclone.DefaultS3Options()); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	t.Run("local to remote", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "tmp:/foo",
			"dstFs": "s3:bar",
		}
		if err := localToRemote()(ctx, in); err != nil {
			t.Fatalf("localToRemote() error %s, expected nil", err)
		}
	})
	t.Run("remote to local", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "s3:bar",
			"dstFs": "tmp:/foo",
		}
		if err := localToRemote()(ctx, in); err != fs.ErrorPermissionDenied {
			t.Fatalf("localToRemote() error %s, expected %s", err, fs.ErrorPermissionDenied)
		}
	})
}

func TestSameDir(t *testing.T) {
	table := []struct {
		SrcFs     string
		SrcRemote string
		DstFs     string
		DstRemote string
		Error     error
	}{
		{
			SrcFs:     "s3:foo",
			SrcRemote: "bar/a",
			DstFs:     "s3:foo",
			DstRemote: "bar/b",
		},
		{
			SrcFs:     "s3:foo/bar",
			SrcRemote: "a",
			DstFs:     "s3:foo",
			DstRemote: "bar/b",
		},
		{
			SrcFs:     "s3:foo",
			SrcRemote: "bar/a",
			DstFs:     "gcs:foo",
			DstRemote: "bar/b",
			Error:     fs.ErrorPermissionDenied,
		},
		{
			SrcFs:     "s3:foo",
			SrcRemote: "bar/a",
			DstFs:     "s3:bar",
			DstRemote: "bar/b",
			Error:     fs.ErrorPermissionDenied,
		},
	}

	ctx := context.Background()

	for _, test := range table {
		in := rc.Params{
			"srcFs":     test.SrcFs,
			"srcRemote": test.SrcRemote,
			"dstFs":     test.DstFs,
			"dstRemote": test.DstRemote,
			"error":     test.Error,
		}
		if err := sameDir()(ctx, in); err != test.Error {
			t.Fatalf("sameDir() = %s, expected %s", err, test.Error)
		}
	}
}
