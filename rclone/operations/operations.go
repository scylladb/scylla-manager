// Copyright (C) 2017 ScyllaDB

package operations

import (
	"context"
	"io"

	"github.com/rclone/rclone/fs"
)

// wrap a Reader and a Closer together into a ReadCloser
type readCloser struct {
	io.Reader
	io.Closer
}

// Cat object to the provided io.Writer with limit number of bytes.
// This is a replacement for rclone operations.Cat because that implementation
// lists and outputs all files in the file system.
//
// if limit < 0 then it will be ignored.
// if limit >= 0 then only that many characters will be output.
func Cat(ctx context.Context, o fs.Object, w io.Writer, limit int64) error {
	var err error

	in, err := o.Open(ctx)
	if err != nil {
		fs.Errorf(o, "Failed to open: %v", err)
		return err
	}
	if limit >= 0 {
		in = &readCloser{Reader: &io.LimitedReader{R: in, N: limit}, Closer: in}
	}
	if _, err = io.Copy(w, in); err != nil {
		fs.Errorf(o, "Failed to send to output: %v", err)
		return err
	}
	return nil
}
