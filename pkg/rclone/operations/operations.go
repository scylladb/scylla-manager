// Copyright (C) 2026 ScyllaDB

package operations

import (
	"context"
	stderr "errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/sync"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

// OperationError wraps remote fs errors returned by CheckPermissions function
// and allows to set a custom message returned to user.
type OperationError struct {
	cause      error
	op         string
	statusCode int
}

func asOperationError(op string, l fs.Fs, err error) OperationError {
	statusCode := 400

	if l.Name() == "s3" {
		e, _ := ParseBackendXMLError(err) // nolint: errcheck
		if e != nil {
			err = e
		} else {
			statusCode = 500
		}
	}

	return OperationError{
		cause:      err,
		op:         op,
		statusCode: statusCode,
	}
}

func (e OperationError) Error() string {
	return "operation " + e.op + ": " + e.cause.Error()
}

func (e OperationError) String() string {
	return e.Error()
}

// StatusCode returns HTTP status code that should be returned for this error.
func (e OperationError) StatusCode() int {
	return e.statusCode
}

// CheckPermissions checks if file system is available for
// listing, getting, creating, and deleting objects.
// The remote parameter specifies a subdirectory within l
// where the test directory is created.
// Locked and overrideLock controls whether retention lock
// permissions should be verified.
func CheckPermissions(ctx context.Context, l fs.Fs, remote string, locked, overrideLock bool) error {
	// Disable retries for calls in permissions check.
	ctx = pacer.WithRetries(ctx, 1)

	// Create temp dir.
	tmpDir, err := os.MkdirTemp("", "scylla-manager-agent-")
	if err != nil {
		return errors.Wrap(err, "create local tmp directory")
	}
	defer os.RemoveAll(tmpDir) // nolint: errcheck

	// Create tmp file.
	var (
		testDirName  = filepath.Join(remote, filepath.Base(tmpDir))
		testFileName = "test"
	)
	if err := os.MkdirAll(filepath.Join(tmpDir, testDirName), os.ModePerm); err != nil {
		return errors.Wrap(err, "create local tmp subdirectory")
	}
	tmpFile := filepath.Join(tmpDir, testDirName, testFileName)
	if err := os.WriteFile(tmpFile, []byte{0}, os.ModePerm); err != nil {
		return errors.Wrap(err, "create local tmp file")
	}

	// Copy local tmp dir contents to the destination.
	f, err := fs.NewFs(ctx, tmpDir)
	if err != nil {
		return errors.Wrap(err, "init temp dir")
	}
	if err := copyTestFile(ctx, l, f); err != nil {
		return err
	}

	// List directory.
	{
		opts := operations.ListJSONOpt{
			Recurse:   false,
			NoModTime: true,
		}
		if err := operations.ListJSON(ctx, l, testDirName, &opts, func(_ *operations.ListJSONItem) error {
			return nil
		}); err != nil {
			return asOperationError("list", l, err)
		}
	}

	// Create, cat and remove remote file.
	{
		o, err := l.NewObject(ctx, filepath.Join(testDirName, testFileName))
		if err != nil {
			return errors.Wrap(err, "init remote temp file object")
		}
		r, err := o.Open(ctx)
		if err != nil {
			return asOperationError("open", l, err)
		}
		_, readErr := io.Copy(io.Discard, r)
		if err := stderr.Join(readErr, r.Close()); err != nil {
			return asOperationError("read", l, err)
		}
		if err := o.Remove(ctx); err != nil {
			return asOperationError("remove", l, err)
		}
	}

	// Retention lock check.
	if locked {
		if err := checkRetentionLock(ctx, l, f, filepath.Join(testDirName, testFileName), overrideLock); err != nil {
			return err
		}
	}

	// Cleanup.
	if err := operations.Purge(ctx, l, testDirName); err != nil {
		// As we already verified all permissions needed by SM to perform
		// a successful backup, we can just log an error here to allow
		// backup to proceed in case of unexpected and not critical error.
		fs.Errorf(l, "failed to remove test directory %q: %v", testDirName, err)
	}

	return nil
}

// checkRetentionLock verifies retention lock and optional override lock
// permissions by re-copying the test file and applying retention policies.
func checkRetentionLock(ctx context.Context, l, localFs fs.Fs, remote string, overrideLock bool) error {
	rl, ok := l.(fs.RetentionLocker)
	if !ok {
		return asOperationError("retention-lock", l, errors.Errorf("backend %q does not support retention lock", l.Name()))
	}

	// Re-copy the test file.
	if err := copyTestFile(ctx, l, localFs); err != nil {
		return err
	}

	// Apply retention lock (unlocked, now+1m).
	// Time is rounded as retention periods are calculated according to
	// snapshot tags, which have a second level precision.
	retainUntil := timeutc.Now().Round(time.Second).Add(time.Minute)
	if err := rl.RetentionLock(ctx, remote, false, retainUntil, false); err != nil {
		return asOperationError("retention-lock", l, err)
	}

	// Override retention lock (locked).
	if overrideLock {
		if err := rl.RetentionLock(ctx, remote, true, retainUntil, true); err != nil {
			return asOperationError("override-lock", l, err)
		}
	}

	return nil
}

func copyTestFile(ctx context.Context, l, tmpFs fs.Fs) error {
	err := sync.CopyDir(ctx, l, tmpFs, true)
	if err != nil {
		// Special handling of permissions errors.
		if errors.Is(err, credentials.ErrNoValidProvidersFoundInChain) {
			return errors.New("no providers - attach IAM Role to EC2 instance or put your access keys to s3 section of /etc/scylla-manager-agent/scylla-manager-agent.yaml and restart agent") // nolint: lll
		}
		return asOperationError("put", l, err)
	}
	return nil
}
