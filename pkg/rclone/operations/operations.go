// Copyright (C) 2017 ScyllaDB

package operations

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/sync"
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

// PermissionError wraps remote fs errors returned by CheckPermissions function
// and allows to set a custom message returned to user.
type PermissionError struct {
	cause error
	op    string
}

func (e PermissionError) Error() string {
	return "no " + e.op + " permission" + ": " + e.cause.Error()
}

func (e PermissionError) String() string {
	return e.Error()
}

// Cause implements errors.Causer.
func (e PermissionError) Cause() error {
	return e.cause
}

// CheckPermissions checks if file system is available for listing, getting,
// creating, and deleting objects.
func CheckPermissions(ctx context.Context, l fs.Fs) error {
	// Create temp dir.
	tmpDir, err := ioutil.TempDir("", "scylla-manager-agent-")
	if err != nil {
		return errors.Wrap(err, "create local tmp directory")
	}
	defer os.RemoveAll(tmpDir) //nolint: errcheck

	// Create tmp file.
	var (
		testDirName  = filepath.Base(tmpDir)
		testFileName = "test"
	)
	if err := os.Mkdir(filepath.Join(tmpDir, testDirName), os.ModePerm); err != nil {
		return errors.Wrap(err, "create local tmp subdirectory")
	}
	tmpFile := filepath.Join(tmpDir, testDirName, testFileName)
	if err := ioutil.WriteFile(tmpFile, []byte{0}, os.ModePerm); err != nil {
		return errors.Wrap(err, "create local tmp file")
	}

	// Copy local tmp dir contents to the destination.
	{
		rd, err := fs.NewFs(tmpDir)
		if err != nil {
			return PermissionError{err, "init temp dir"}
		}
		if err := sync.CopyDir(ctx, l, rd, true); err != nil {
			// Special handling of permissions errors
			if errors.Cause(err) == credentials.ErrNoValidProvidersFoundInChain {
				return errors.New("no providers - attach IAM Role to EC2 instance or put your access keys to s3 section of /etc/scylla-manager-agent/scylla-manager-agent.yaml and restart agent") //nolint:lll
			}
			return PermissionError{err, "put"}
		}
	}

	// List directory.
	{
		opts := operations.ListJSONOpt{
			Recurse:   false,
			NoModTime: true,
		}
		if err := operations.ListJSON(ctx, l, testDirName, &opts, func(item *operations.ListJSONItem) error {
			return nil
		}); err != nil {
			return PermissionError{err, "list"}
		}
	}

	// Cat remote file.
	{
		rf, err := l.NewObject(ctx, filepath.Join(testDirName, testFileName))
		if err != nil {
			return errors.Wrap(err, "init remote temp file object")
		}
		if err := Cat(ctx, rf, ioutil.Discard, 1); err != nil {
			return PermissionError{err, "get"}
		}
	}

	// Remove remote dir.
	{
		rd, err := fs.NewFs(fmt.Sprintf("%s:%s/%s", l.Name(), l.Root(), testDirName))
		if err != nil {
			return errors.Wrap(err, "init remote temp dir")
		}
		if err := operations.Delete(ctx, rd); err != nil {
			return PermissionError{err, "delete"}
		}
	}

	return nil
}
