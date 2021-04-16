// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"context"
	"path"
	"path/filepath"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/rclone/rclone/fs/rc"
)

type paramsValidator func(ctx context.Context, in rc.Params) error

func wrap(fn rc.Func, v paramsValidator) rc.Func {
	return func(ctx context.Context, in rc.Params) (rc.Params, error) {
		if err := v(ctx, in); err != nil {
			return nil, err
		}
		return fn(ctx, in)
	}
}

// pathHasPrefix reads "fs" and "remote" params, evaluates absolute path and
// ensures it has the required prefix.
func pathHasPrefix(prefix string) paramsValidator {
	return func(ctx context.Context, in rc.Params) error {
		f, err := in.GetString("fs")
		if err != nil {
			return err
		}
		remote, err := in.GetString("remote")
		if err != nil {
			return err
		}
		p, err := join(f, remote)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(p, prefix) {
			return fs.ErrorPermissionDenied
		}
		return nil
	}
}

func join(f, remote string) (string, error) {
	_, fsPath, err := fspath.Parse(f)
	if err != nil {
		return "", err
	}
	p := filepath.Clean(path.Join(fsPath, remote))
	i := strings.Index(p, "/")
	return p[i+1:], nil
}
