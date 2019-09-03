// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"bytes"
	"context"
	"encoding/base64"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/rc"
)

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

See the [cat command](/commands/rclone_cat/) command for more information on the above.
`,
	})
}

// Cat a remote
func rcCat(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	f, err := rc.GetFs(in)
	if err != nil && err != fs.ErrorIsFile {
		return nil, err
	}

	var buf bytes.Buffer
	w := base64.NewEncoder(base64.URLEncoding, &buf)
	if err := operations.Cat(ctx, f, w, 0, -1); err != nil {
		return nil, err
	}
	w.Close()

	out = make(rc.Params)
	out["Content"] = buf.String()
	return out, nil
}
