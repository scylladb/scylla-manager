// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"bytes"
	"context"
	"encoding/base64"

	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
	"github.com/scylladb/mermaid/pkg/rclone/operations"
	"github.com/scylladb/mermaid/pkg/rclone/rcserver/internal"
)

// CatLimit is the maximum amount of bytes that Cat operation can output.
const CatLimit = 1024 * 1024

func init() {
	// Disable all default calls.
	calls := rc.Calls.List()
	rc.Calls = rc.NewRegistry()

	// Whitelist only supported calls.
	for _, c := range calls {
		if internal.RcloneSupportedCalls.Has(c.Path) {
			rc.Add(*c)
		}
	}
}

// Cat a remote object.
func rcCat(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	f, remote, err := rc.GetFsAndRemote(in)
	if err != nil {
		return nil, err
	}
	o, err := f.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	w := base64.NewEncoder(base64.URLEncoding, &buf)
	if err := operations.Cat(ctx, o, w, CatLimit); err != nil {
		return nil, err
	}
	w.Close()

	out = make(rc.Params)
	out["Content"] = buf.String()
	return out, nil
}

func init() {
	rc.Add(rc.Call{
		Path:         "operations/cat",
		AuthRequired: true,
		Fn:           rcCat,
		Title:        "Concatenate any files and send them in response",
		Help: `This takes the following parameters

- fs - a remote name string eg "drive:path/to/dir"

Returns

- content - base64 encoded file content
`,
	})
}

// rcCheckPermissions checks if location is available for listing, getting,
// creating, and deleting objects.
func rcCheckPermissions(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	l, err := rc.GetFs(in)
	if err != nil {
		return nil, errors.Wrap(err, "init location")
	}

	if err := operations.CheckPermissions(ctx, l); err != nil {
		fs.Errorf(nil, "Location check: error=%+v", err)
		return nil, err
	}

	fs.Infof(nil, "Location check done")
	return rc.Params{}, nil
}

func init() {
	rc.Add(rc.Call{
		Path:         "operations/check-permissions",
		AuthRequired: true,
		Fn:           rcCheckPermissions,
		Title:        "Checks listing, getting, creating, and deleting objects",
		Help: `This takes the following parameters

- fs - a remote name string eg "s3:repository"

`,
	})
}
