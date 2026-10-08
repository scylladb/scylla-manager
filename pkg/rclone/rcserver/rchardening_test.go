// Copyright (C) 2026 ScyllaDB

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

func TestFromLocal(t *testing.T) {
	rclone.InitFsConfig()
	rclone.MustRegisterLocalDirProvider("tmp", "", "/tmp")
	if err := rclone.RegisterS3Provider(rclone.DefaultS3Options()); err != nil {
		t.Fatal(err)
	}

	t.Run("local to local", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "tmp:/foo",
			"dstFs": "tmp:/bar",
		}
		if err := fromLocal()(t.Context(), in); err != nil {
			t.Fatalf("fromLocal() error %s, expected nil", err)
		}
	})
	t.Run("local to remote", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "tmp:/foo",
			"dstFs": "s3:bar",
		}
		if err := fromLocal()(t.Context(), in); err != nil {
			t.Fatalf("fromLocal() error %s, expected nil", err)
		}
	})
	t.Run("remote to local rejected", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "s3:bar",
			"dstFs": "tmp:/foo",
		}
		if err := fromLocal()(t.Context(), in); !errors.Is(err, fs.ErrorPermissionDenied) {
			t.Fatalf("fromLocal() error %s, expected %s", err, fs.ErrorPermissionDenied)
		}
	})
	t.Run("remote to remote rejected", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "s3:foo",
			"dstFs": "s3:bar",
		}
		if err := fromLocal()(t.Context(), in); !errors.Is(err, fs.ErrorPermissionDenied) {
			t.Fatalf("fromLocal() error %s, expected %s", err, fs.ErrorPermissionDenied)
		}
	})
}

func TestToLocal(t *testing.T) {
	rclone.InitFsConfig()
	rclone.MustRegisterLocalDirProvider("tmp", "", "/tmp")
	if err := rclone.RegisterS3Provider(rclone.DefaultS3Options()); err != nil {
		t.Fatal(err)
	}

	t.Run("local to local", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "tmp:/foo",
			"dstFs": "tmp:/bar",
		}
		if err := toLocal()(t.Context(), in); err != nil {
			t.Fatalf("toLocal() error %s, expected nil", err)
		}
	})
	t.Run("remote to local", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "s3:bar",
			"dstFs": "tmp:/foo",
		}
		if err := toLocal()(t.Context(), in); err != nil {
			t.Fatalf("toLocal() error %s, expected nil", err)
		}
	})
	t.Run("local to remote rejected", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "tmp:/foo",
			"dstFs": "s3:bar",
		}
		if err := toLocal()(t.Context(), in); !errors.Is(err, fs.ErrorPermissionDenied) {
			t.Fatalf("toLocal() error %s, expected %s", err, fs.ErrorPermissionDenied)
		}
	})
	t.Run("remote to remote rejected", func(t *testing.T) {
		in := rc.Params{
			"srcFs": "s3:foo",
			"dstFs": "s3:bar",
		}
		if err := toLocal()(t.Context(), in); !errors.Is(err, fs.ErrorPermissionDenied) {
			t.Fatalf("toLocal() error %s, expected %s", err, fs.ErrorPermissionDenied)
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

func TestValidateRemotePaths(t *testing.T) {
	testCases := []struct {
		Name  string
		In    rc.Params
		Error bool
	}{
		{
			Name: "no remote params",
			In:   rc.Params{"fs": "s3:bla"},
		},
		{
			Name: "remote within root",
			In:   rc.Params{"fs": "s3:bla", "remote": "backup/meta/../sst/file"},
		},
		{
			Name: "empty remote",
			In:   rc.Params{"fs": "data:", "remote": ""},
		},
		{
			Name: "absolute remote is resolved under root",
			In:   rc.Params{"fs": "data:", "remote": "/etc/passwd"},
		},
		{
			Name:  "remote parent",
			In:    rc.Params{"fs": "data:sub", "remote": ".."},
			Error: true,
		},
		{
			Name:  "remote above root",
			In:    rc.Params{"fs": "data:sub", "remote": "../../etc/passwd"},
			Error: true,
		},
		{
			Name:  "remote escaping after descending",
			In:    rc.Params{"fs": "data:", "remote": "sub/../../etc"},
			Error: true,
		},
		{
			Name:  "src remote escaping",
			In:    rc.Params{"srcFs": "data:", "srcRemote": "../x", "dstFs": "s3:bla", "dstRemote": "x"},
			Error: true,
		},
		{
			Name:  "dst remote escaping",
			In:    rc.Params{"srcFs": "s3:bla", "srcRemote": "x", "dstFs": "data:", "dstRemote": "../x"},
			Error: true,
		},
		{
			Name: "paths within remote",
			In:   rc.Params{"fs": "data:", "remote": "dir", "paths": []any{"a", "sub/b", "sub/../c"}},
		},
		{
			Name:  "paths escaping remote",
			In:    rc.Params{"fs": "data:", "remote": "dir", "paths": []any{"a", "../../x"}},
			Error: true,
		},
		{
			Name:  "paths not a list of strings",
			In:    rc.Params{"fs": "data:", "remote": "dir", "paths": []any{1}},
			Error: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			err := validateRemotePaths(test.In)
			if test.Error && err == nil {
				t.Fatal("expected error")
			}
			if !test.Error && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}
