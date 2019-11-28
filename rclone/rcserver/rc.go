// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"bytes"
	"context"
	"encoding/base64"

	"github.com/rclone/rclone/fs/rc"
	"github.com/scylladb/mermaid/rclone/internal"
	"github.com/scylladb/mermaid/rclone/operations"
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

	rc.Add(rc.Call{
		Path:         "operations/cat",
		AuthRequired: true,
		Fn:           rcCat,
		Title:        "Concatenate any files and send them in response",
		Help: `This takes the following parameters

- fs - a remote name string eg "drive:path/to/dir"

Returns

- content - base64 encoded file content

See the [cat command](/commands/rclone_cat/) command for more information on the above.
`,
	})
}

// Cat a remote
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
