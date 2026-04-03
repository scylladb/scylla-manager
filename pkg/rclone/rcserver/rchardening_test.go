// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"context"
	"errors"
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

func TestDataToBackup(t *testing.T) {
	rclone.InitFsConfig()
	rclone.MustRegisterLocalDirProvider("data", "", "/tmp")
	rclone.MustRegisterLocalDirProvider("localstorage", "", "/tmp")
	if err := rclone.RegisterS3Provider(rclone.DefaultS3Options()); err != nil {
		t.Fatal(err)
	}

	t.Run("data to s3", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "data:/foo",
			"dstFs": "s3:bar",
		}
		if err := dataToBackup()(t.Context(), in); err != nil {
			t.Fatalf("dataToBackup() error %s, expected nil", err)
		}
	})
	t.Run("data to localstorage", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "data:/foo",
			"dstFs": "localstorage:bar",
		}
		if err := dataToBackup()(t.Context(), in); err != nil {
			t.Fatalf("dataToBackup() error %s, expected nil", err)
		}
	})
	t.Run("s3 to data rejected", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "s3:bar",
			"dstFs": "data:/foo",
		}
		if err := dataToBackup()(t.Context(), in); errors.Is(err, fs.ErrorPermissionDenied) {
			t.Fatalf("dataToBackup() error %s, expected %s", err, fs.ErrorPermissionDenied)
		}
	})
	t.Run("localstorage to data rejected", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "localstorage:bar",
			"dstFs": "data:/foo",
		}
		if err := dataToBackup()(t.Context(), in); errors.Is(err, fs.ErrorPermissionDenied) {
			t.Fatalf("dataToBackup() error %s, expected %s", err, fs.ErrorPermissionDenied)
		}
	})
	t.Run("data to data rejected", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "data:/foo",
			"dstFs": "data:/bar",
		}
		if err := dataToBackup()(t.Context(), in); errors.Is(err, fs.ErrorPermissionDenied) {
			t.Fatalf("dataToBackup() error %s, expected %s", err, fs.ErrorPermissionDenied)
		}
	})
}

func TestBackupToData(t *testing.T) {
	rclone.InitFsConfig()
	rclone.MustRegisterLocalDirProvider("data", "", "/tmp")
	rclone.MustRegisterLocalDirProvider("localstorage", "", "/tmp")
	if err := rclone.RegisterS3Provider(rclone.DefaultS3Options()); err != nil {
		t.Fatal(err)
	}

	t.Run("s3 to data", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "s3:bar",
			"dstFs": "data:/foo",
		}
		if err := backupToData()(t.Context(), in); err != nil {
			t.Fatalf("backupToData() error %s, expected nil", err)
		}
	})
	t.Run("localstorage to data", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "localstorage:bar",
			"dstFs": "data:/foo",
		}
		if err := backupToData()(t.Context(), in); err != nil {
			t.Fatalf("backupToData() error %s, expected nil", err)
		}
	})
	t.Run("data to s3 rejected", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "data:/foo",
			"dstFs": "s3:bar",
		}
		if err := backupToData()(t.Context(), in); errors.Is(err, fs.ErrorPermissionDenied) {
			t.Fatalf("backupToData() error %s, expected %s", err, fs.ErrorPermissionDenied)
		}
	})
	t.Run("data to data rejected", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "data:/foo",
			"dstFs": "data:/bar",
		}
		if err := backupToData()(t.Context(), in); errors.Is(err, fs.ErrorPermissionDenied) {
			t.Fatalf("backupToData() error %s, expected %s", err, fs.ErrorPermissionDenied)
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
