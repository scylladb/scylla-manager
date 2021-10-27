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
	"github.com/rclone/rclone/lib/pacer"
)

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
	// Disable retries for calls in permissions check.
	ctx = pacer.WithRetries(ctx, 1)

	// Create temp dir.
	tmpDir, err := ioutil.TempDir("", "scylla-manager-agent-")
	if err != nil {
		return errors.Wrap(err, "create local tmp directory")
	}
	defer os.RemoveAll(tmpDir) // nolint: errcheck

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
		f, err := fs.NewFs(context.Background(), tmpDir)
		if err != nil {
			return PermissionError{err, "init temp dir"}
		}
		if err := sync.CopyDir(ctx, l, f, true); err != nil {
			// Special handling of permissions errors
			if errors.Is(err, credentials.ErrNoValidProvidersFoundInChain) {
				return errors.New("no providers - attach IAM Role to EC2 instance or put your access keys to s3 section of /etc/scylla-manager-agent/scylla-manager-agent.yaml and restart agent") // nolint: lll
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
		o, err := l.NewObject(ctx, filepath.Join(testDirName, testFileName))
		if err != nil {
			return errors.Wrap(err, "init remote temp file object")
		}
		r, err := o.Open(ctx)
		if err != nil {
			return PermissionError{err, "get"}
		}
		defer r.Close()
		if _, err := io.Copy(io.Discard, r); err != nil {
			return PermissionError{err, "get"}
		}
	}

	// Remove remote dir.
	{
		f, err := fs.NewFs(ctx, fmt.Sprintf("%s:%s/%s", l.Name(), l.Root(), testDirName))
		if err != nil {
			return errors.Wrap(err, "init remote temp dir")
		}
		if err := operations.Delete(ctx, f); err != nil {
			return PermissionError{err, "delete"}
		}
	}

	return nil
}
